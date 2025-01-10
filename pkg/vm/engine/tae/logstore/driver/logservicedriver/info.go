// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservicedriver

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var ErrDriverLsnNotFound = moerr.NewInternalErrorNoCtx("driver info: driver lsn not found")
var ErrRetryTimeOut = moerr.NewInternalErrorNoCtx("driver info: retry time out")

type driverInfo struct {
	// PSN -> [DSN,..]
	addr map[uint64]*common.ClosedIntervals //logservicelsn-driverlsn TODO drop on truncate
	// PSN: physical sequence number. here is the lsn from logservice
	validPSN roaring64.Bitmap
	addrMu   sync.RWMutex

	// dsn: driver sequence number
	// it is monotonically continuously increasing
	// PSN:[DSN:LSN, DSN:LSN, DSN:LSN, ...]
	// One : Many
	dsn   uint64
	dsnmu sync.RWMutex

	syncing  uint64
	synced   uint64
	syncedMu sync.RWMutex

	truncating   atomic.Uint64 //
	truncatedPSN uint64        //

	// writeController is used to control the write token
	// it controles the max write token issued and all finished write tokens
	// to avoid too much pendding writes
	// Example:
	// maxIssuedToken = 100
	// maxFinishedToken = 50
	// maxPendding = 60
	// then we can only issue another 10 write token to avoid too much pendding writes
	// In the real world, the maxFinishedToken is always being updated and it is very
	// rare to reach the maxPendding
	writeController struct {
		sync.RWMutex
		// max write token issued
		maxIssuedToken uint64
		// all finished write tokens
		finishedTokens *common.ClosedIntervals
	}

	commitCond sync.Cond
	inReplay   bool
}

func newDriverInfo() *driverInfo {
	d := &driverInfo{
		addr:       make(map[uint64]*common.ClosedIntervals),
		commitCond: *sync.NewCond(new(sync.Mutex)),
	}
	d.writeController.finishedTokens = common.NewClosedIntervals()
	return d
}

func (info *driverInfo) GetDSN() uint64 {
	info.dsnmu.RLock()
	lsn := info.dsn
	info.dsnmu.RUnlock()
	return lsn
}

func (info *driverInfo) PreReplay() {
	info.inReplay = true
}
func (info *driverInfo) PostReplay() {
	info.inReplay = false
}
func (info *driverInfo) IsReplaying() bool {
	return info.inReplay
}
func (info *driverInfo) onReplay(r *replayer) {
	info.dsn = r.maxDriverLsn
	info.synced = r.maxDriverLsn
	info.syncing = r.maxDriverLsn
	if r.minDriverLsn != math.MaxUint64 {
		info.truncating.Store(r.minDriverLsn - 1)
	}
	info.truncatedPSN = r.truncatedPSN
	info.writeController.finishedTokens.TryMerge(common.NewClosedIntervalsBySlice(r.writeTokens))
}

func (info *driverInfo) onReplayRecordEntry(
	psn uint64, dsns *common.ClosedIntervals,
) {
	info.addr[psn] = dsns
	info.validPSN.Add(psn)
}

func (info *driverInfo) getNextValidPSN(psn uint64) uint64 {
	info.addrMu.RLock()
	defer info.addrMu.RUnlock()
	if info.validPSN.IsEmpty() {
		return 0
	}
	maxPSN := info.validPSN.Maximum()
	if psn >= maxPSN {
		return maxPSN
	}
	psn++
	for !info.validPSN.Contains(psn) {
		psn++
	}
	return psn
}

func (info *driverInfo) isToTruncate(logserviceLsn, dsn uint64) bool {
	maxlsn := info.getMaxDriverLsn(logserviceLsn)
	if maxlsn == 0 {
		return false
	}
	return maxlsn <= dsn
}

func (info *driverInfo) getMaxDriverLsn(logserviceLsn uint64) uint64 {
	info.addrMu.RLock()
	intervals, ok := info.addr[logserviceLsn]
	if !ok {
		info.addrMu.RUnlock()
		return 0
	}
	lsn := intervals.GetMax()
	info.addrMu.RUnlock()
	return lsn
}

func (info *driverInfo) allocateDSNLocked() uint64 {
	info.dsn++
	return info.dsn
}

func (info *driverInfo) getMaxFinishedToken() uint64 {
	info.writeController.RLock()
	defer info.writeController.RUnlock()
	finishedTokens := info.writeController.finishedTokens
	if finishedTokens == nil ||
		len(finishedTokens.Intervals) == 0 ||
		finishedTokens.Intervals[0].Start != 1 {
		return 0
	}
	return finishedTokens.Intervals[0].End
}

func (info *driverInfo) applyWriteToken(
	maxPendding uint64, timeout time.Duration,
) (token uint64, err error) {
	token, err = info.tryApplyWriteToken(maxPendding)
	if err == ErrTooMuchPenddings {
		err = RetryWithTimeout(
			timeout,
			func() (shouldReturn bool) {
				info.commitCond.L.Lock()
				token, err = info.tryApplyWriteToken(maxPendding)
				if err != ErrTooMuchPenddings {
					info.commitCond.L.Unlock()
					return true
				}
				info.commitCond.Wait()
				info.commitCond.L.Unlock()
				token, err = info.tryApplyWriteToken(maxPendding)
				return err != ErrTooMuchPenddings
			},
		)
	}
	return
}

// NOTE: must be called in serial
func (info *driverInfo) tryApplyWriteToken(
	maxPendding uint64,
) (token uint64, err error) {
	maxFinishedToken := info.getMaxFinishedToken()
	if info.writeController.maxIssuedToken-maxFinishedToken >= maxPendding {
		return 0, ErrTooMuchPenddings
	}
	info.writeController.maxIssuedToken++
	return info.writeController.maxIssuedToken, nil
}

func (info *driverInfo) logAppend(appender *driverAppender) {
	info.addrMu.Lock()
	array := make([]uint64, 0, len(appender.entry.Meta.addr))
	for key := range appender.entry.Meta.addr {
		array = append(array, key)
	}
	info.validPSN.Add(appender.logserviceLsn)
	interval := common.NewClosedIntervalsBySlice(array)
	info.addr[appender.logserviceLsn] = interval
	info.addrMu.Unlock()
	if interval.GetMin() != info.syncing+1 {
		panic(fmt.Sprintf("logic err, expect %d, min is %d", info.syncing+1, interval.GetMin()))
	}
	if len(interval.Intervals) != 1 {
		logutil.Debugf("interval is %v", interval)
		panic("logic err")
	}
	info.syncing = interval.GetMax()
}

func (info *driverInfo) gcAddr(logserviceLsn uint64) {
	info.addrMu.Lock()
	defer info.addrMu.Unlock()
	lsnToDelete := make([]uint64, 0)
	for serviceLsn := range info.addr {
		if serviceLsn < logserviceLsn {
			lsnToDelete = append(lsnToDelete, serviceLsn)
		}
	}
	info.validPSN.RemoveRange(0, logserviceLsn)
	for _, lsn := range lsnToDelete {
		delete(info.addr, lsn)
	}
}

func (info *driverInfo) getSynced() uint64 {
	info.syncedMu.RLock()
	lsn := info.synced
	info.syncedMu.RUnlock()
	return lsn
}
func (info *driverInfo) putbackWriteTokens(tokens []uint64) {
	info.syncedMu.Lock()
	info.synced = info.syncing
	info.syncedMu.Unlock()

	finishedToken := common.NewClosedIntervalsBySlice(tokens)
	info.writeController.Lock()
	info.writeController.finishedTokens.TryMerge(finishedToken)
	info.writeController.Unlock()

	info.commitCond.L.Lock()
	info.commitCond.Broadcast()
	info.commitCond.L.Unlock()
}

func (info *driverInfo) tryGetLogServiceLsnByDriverLsn(dsn uint64) (uint64, error) {
	lsn, err := info.getLogServiceLsnByDriverLsn(dsn)
	if err == ErrDriverLsnNotFound {
		if lsn <= info.GetDSN() {
			for i := 0; i < 10; i++ {
				logutil.Infof("retry get logserviceLsn, driverlsn=%d", dsn)
				info.commitCond.L.Lock()
				lsn, err = info.getLogServiceLsnByDriverLsn(dsn)
				if err == nil {
					info.commitCond.L.Unlock()
					break
				}
				info.commitCond.Wait()
				info.commitCond.L.Unlock()
			}
			if err != nil {
				return 0, ErrRetryTimeOut
			}
		}
	}
	return lsn, err
}

func (info *driverInfo) getLogServiceLsnByDriverLsn(dsn uint64) (uint64, error) {
	info.addrMu.RLock()
	defer info.addrMu.RUnlock()
	for lsn, intervals := range info.addr {
		if intervals.Contains(*common.NewClosedIntervalsByInt(dsn)) {
			return lsn, nil
		}
	}
	return 0, ErrDriverLsnNotFound
}
