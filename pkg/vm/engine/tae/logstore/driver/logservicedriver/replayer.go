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
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"go.uber.org/zap"
)

type replayer2 struct {
	readBatchSize int

	driver *LogServiceDriver
	handle driver.ApplyHandle

	replayedState struct {
		// DSN->PSN mapping
		dsn2PSNMap map[uint64]uint64
		readCache  readCache

		// the DSN is monotonically continuously increasing and the corresponding
		// PSN may not be monotonically increasing due to the concurrent write.
		// So when writing a record, we logs the visible safe DSN in the record,
		// which is the maximum DSN that has been safely written to the backend
		// continuously without any gaps. It means that all records with DSN <=
		// safeDSN have been safely written to the backend. When replaying, we
		// can make sure that all records with DSN <= safeDSN have been replayed
		safeDSN uint64

		writeTokens []uint64

		firstAppliedDSN uint64
		firstAppliedLSN uint64

		lastAppliedDSN uint64
		lastAppliedLSN uint64
	}

	waterMarks struct {
		// psn to read for the next batch
		psnToRead uint64

		// the DSN watermark has been scheduled for apply
		dsnScheduled uint64

		minDSN uint64
		maxDSN uint64
	}

	stats struct {
		applyDuration time.Duration
		readDuration  time.Duration

		appliedLSNCount  atomic.Int64
		readPSNCount     int
		schedulePSNCount int
		scheduleLSNCount int
	}
}

func newReplayer2(
	handle driver.ApplyHandle,
	driver *LogServiceDriver,
	readBatchSize int,
) *replayer2 {
	r := &replayer2{
		handle:        handle,
		driver:        driver,
		readBatchSize: readBatchSize,
	}
	r.replayedState.readCache = newReadCache()
	r.replayedState.dsn2PSNMap = make(map[uint64]uint64)
	r.waterMarks.dsnScheduled = math.MaxUint64
	r.waterMarks.minDSN = math.MaxUint64

	return r
}

func (r *replayer2) exportFields() []zap.Field {
	return []zap.Field{
		zap.Duration("read-duration", r.stats.readDuration),
		zap.Int("read-psn-count", r.stats.readPSNCount),
		zap.Int64("apply-lsn-count", r.stats.appliedLSNCount.Load()),
		zap.Int("schedule-psn-count", r.stats.schedulePSNCount),
		zap.Int("schedule-lsn-count", r.stats.scheduleLSNCount),
		zap.Duration("apply-duration", r.stats.applyDuration),
		zap.Uint64("first-apply-dsn", r.replayedState.firstAppliedDSN),
		zap.Uint64("first-apply-lsn", r.replayedState.firstAppliedLSN),
		zap.Uint64("last-apply-dsn", r.replayedState.lastAppliedDSN),
		zap.Uint64("last-apply-lsn", r.replayedState.lastAppliedLSN),
	}
}

func (r *replayer2) initReadWatermarks(ctx context.Context) (err error) {
	var psn uint64
	if psn, err = r.driver.getTruncatedPSNFromBackend(ctx); err != nil {
		return
	}
	r.waterMarks.psnToRead = psn + 1
	return
}

func (r *replayer2) Replay(ctx context.Context) (err error) {
	var (
		readDone      bool
		resultC       = make(chan error, 1)
		applyC        = make(chan *entry.Entry, 20)
		lastScheduled *entry.Entry
		errMsg        string
	)
	defer func() {
		fields := r.exportFields()
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
			fields = append(fields, zap.String("err-msg", errMsg))
			fields = append(fields, zap.Error(err))
		}
		logger(
			"Wal-Replay-Info",
			fields...,
		)
	}()

	// init the read watermarks to use the next sequnce number of the
	// truncated PSN as the start PSN to read
	if err = r.initReadWatermarks(ctx); err != nil {
		return
	}

	// a dedicated goroutine to replay entries from the applyC
	go r.streamApplying(ctx, applyC, resultC)

	// read log records batch by batch and schedule the records for apply
	for {
		if readDone, err = r.readNextBatch(ctx); err != nil || readDone {
			break
		}

		// when the schedule DSN is less than the safe DSN, we try to
		// schedule some entries for apply
		for r.waterMarks.dsnScheduled < r.replayedState.safeDSN {
			if lastScheduled, err = r.tryScheduleApply(
				ctx, applyC, lastScheduled, false,
			); err != nil {
				break
			}
		}

		if err != nil {
			break
		}
	}

	if err != nil {
		errMsg = fmt.Sprintf("read and schedule error in loop: %v", err)
		close(applyC)
		return
	}

	for err == nil || err != ErrAllRecordsRead {
		lastScheduled, err = r.tryScheduleApply(
			ctx, applyC, lastScheduled, true,
		)
	}

	if err == ErrAllRecordsRead {
		err = nil
	}

	if err != nil {
		errMsg = fmt.Sprintf("schedule apply error: %v", err)
		close(applyC)
		return
	}

	r.replayedState.readCache.clear()

	if lastScheduled != nil {
		lastScheduled.WaitDone()
	}

	close(applyC)

	// wait for the replay to finish
	if applyErr := <-resultC; applyErr != nil {
		err = applyErr
		errMsg = fmt.Sprintf("apply error: %v", applyErr)
	}

	return
}

func (r *replayer2) tryScheduleApply(
	ctx context.Context,
	applyC chan<- *entry.Entry,
	lastScheduled *entry.Entry,
	readDone bool,
) (scheduled *entry.Entry, err error) {
	scheduled = lastScheduled

	// readCache isEmpty means all readed records have been scheduled for apply
	if r.replayedState.readCache.isEmpty() {
		err = ErrAllRecordsRead
		return
	}

	dsn := r.waterMarks.dsnScheduled + 1
	psn, ok := r.replayedState.dsn2PSNMap[dsn]

	// Senario 1 [dsn not found]:
	// dsn is not found in the dsn2PSNMap, which means the record
	// with the dsn has not been read from the backend
	if !ok {
		appliedLSNCount := r.stats.appliedLSNCount.Load()
		if appliedLSNCount == 0 && lastScheduled != nil {
			lastScheduled.WaitDone()
			appliedLSNCount = r.stats.appliedLSNCount.Load()
		}

		if dsn <= r.replayedState.safeDSN {
			// [dsn not found && dsn <= safeDSN]:
			// the record should already be read from the backend

			if appliedLSNCount == 0 {
				//[dsn not found && dsn <= safeDSN && appliedLSNCount == 0]
				// there is no record applied or scheduled for apply
				// maybe there are some old non-contiguous records
				// Example:
				// PSN: 10,     11,     12,     13,     14,     15      16
				// DSN: [26,27],[24,25],[30,31],[28,29],[32,33],[36,37],[34,35]
				// Truncate: want to truncate DSN 32 and it will truncate PSN 13, remmaining: PSN 14, 15, 16
				// PSN: 14,     15,     16
				// DSN: [32,33],[36,37],[34,35]

				logutil.Info(
					"Wal-Replay-Skip-Entry",
					zap.Uint64("dsn", dsn),
					zap.Uint64("safe-dsn", r.replayedState.safeDSN),
				)
				// PXU TODO: ???
				r.waterMarks.minDSN = dsn + 1
				r.waterMarks.dsnScheduled = dsn
				return
			} else {
				// [dsn not found && dsn <= safeDSN && appliedLSNCount > 0]
				err = moerr.NewInternalErrorNoCtxf(
					"dsn %d not found", dsn,
				)
				logutil.Error(
					"Wal-Schedule-Apply-Error",
					zap.Error(err),
					zap.Uint64("dsn", dsn),
					zap.Uint64("safe-dsn", r.replayedState.safeDSN),
					zap.Any("dsn-psn", r.replayedState.dsn2PSNMap),
				)
				return
			}
		} else {
			// PXU TODO: check allReaded ????
			if !readDone {
				panic(fmt.Sprintf("logic error, safe dsn %d, dsn %d", r.replayedState.safeDSN, dsn))
			}

			// [dsn not found && dsn > safeDSN]
			if len(r.replayedState.dsn2PSNMap) != 0 {
				r.AppendSkipCmd(ctx, r.replayedState.dsn2PSNMap)
			}
		}
	}

	if lastScheduled == nil {
		logutil.Info(
			"Wal-Replay-First-Entry",
			zap.Uint64("dsn", dsn),
		)
	}

	var record *recordEntry
	if record, err = r.replayedState.readCache.getRecord(psn); err != nil {
		return
	}

	dsns := make([]uint64, 0, len(record.Meta.addr))
	scheduleApply := func(dsn uint64, e *entry.Entry) {
		dsns = append(dsns, dsn)
		lastScheduled = e
		applyC <- e
	}

	if err = record.forEachLogEntry(scheduleApply); err != nil {
		return
	}

	dsnRange := common.NewClosedIntervalsBySlice(dsns)
	r.updateDSN(dsnRange.GetMax())
	r.updateDSN(dsnRange.GetMin())
	r.waterMarks.dsnScheduled = dsnRange.GetMax()
	r.stats.schedulePSNCount++

	// dsn2PSNMap is produced by the readNextBatch and consumed if it is scheduled apply
	delete(r.replayedState.dsn2PSNMap, dsn)

	// driver is mangaging the psn to dsn mapping
	// here the replayer is responsible to provide the all the existing psn to dsn
	// info to the driver
	// PXU TODO: not all the replayer are responsible for this
	// a driver is a statemachine.
	// INITed -> REPLAYING -> REPLAYED
	// a driver can be readonly or readwrite
	// for readonly driver, it is always in the REPLAYING state
	// for readwrite driver, it can only serve the write request
	// after the REPLAYED state
	r.driver.recordPSNInfo(psn, dsnRange)

	return
}

// updateDSN updates the minDSN and maxDSN
func (r *replayer2) updateDSN(dsn uint64) {
	if dsn == 0 {
		return
	}
	if dsn < r.waterMarks.minDSN {
		r.waterMarks.minDSN = dsn
	}
	if dsn > r.waterMarks.maxDSN {
		r.waterMarks.maxDSN = dsn
	}
}

func (r *replayer2) streamApplying(
	ctx context.Context,
	sourceC <-chan *entry.Entry,
	resultC chan error,
) (err error) {
	var (
		e                *entry.Entry
		lastAppliedEntry *entry.Entry
		ok               bool
	)
	defer func() {
		resultC <- err
	}()
	for {
		select {
		case <-ctx.Done():
			err = context.Cause(ctx)
			return
		case e, ok = <-sourceC:
			if !ok {
				if lastAppliedEntry != nil {
					r.replayedState.lastAppliedDSN = lastAppliedEntry.Lsn
					_, lsn := lastAppliedEntry.Entry.GetLsn()
					r.replayedState.lastAppliedLSN = lsn
				}
				return
			}
			r.stats.scheduleLSNCount++

			t0 := time.Now()

			// state == driver.RE_Nomal means the entry is applied successfully
			// here log the first applied entry
			if state := r.handle(e); state == driver.RE_Nomal {
				if r.stats.appliedLSNCount.Load() == 0 {
					_, lsn := e.Entry.GetLsn()
					r.replayedState.firstAppliedDSN = e.Lsn
					r.replayedState.firstAppliedLSN = lsn
					r.stats.appliedLSNCount.Add(1)
				}
				lastAppliedEntry = e
			}
			e.DoneWithErr(nil)
			e.Entry.Free()

			r.stats.applyDuration += time.Since(t0)
		}
	}
	return
}

// this function reads the next batch of records from the backend
func (r *replayer2) readNextBatch(
	ctx context.Context,
) (done bool, err error) {
	t0 := time.Now()
	nextPSN, records := r.driver.readFromBackend(
		r.waterMarks.psnToRead, r.readBatchSize,
	)
	r.stats.readDuration += time.Since(t0)
	for i, record := range records {
		// skip non-user records
		if record.GetType() != pb.UserRecord {
			continue
		}
		psn := r.waterMarks.psnToRead + uint64(i)
		if updated, entry := r.replayedState.readCache.addRecord(
			psn, record,
		); updated {
			// 1. update the safe DSN
			if r.replayedState.safeDSN < entry.appended {
				r.replayedState.safeDSN = entry.appended
			}

			// 2. update the stats
			r.stats.readPSNCount++

			// 3. remove the skipped records if the entry is a skip entry
			if entry.Meta.metaType == TReplay {
				logutil.Info(
					"Wal-Replay-Skip-Entry",
					zap.Any("skip-map", entry.cmd.skipMap),
					zap.Uint64("psn", psn),
					zap.Uint64("safe-dsn", entry.appended),
				)

				for dsn := range entry.cmd.skipMap {
					if _, ok := r.replayedState.dsn2PSNMap[dsn]; !ok {
						panic(fmt.Sprintf("dsn %d not found in the dsn2PSNMap", dsn))
					}
					delete(r.replayedState.dsn2PSNMap, dsn)
				}

				return
			}

			// 4. update the DSN->PSN mapping
			dsn := entry.GetMinLsn()
			r.replayedState.dsn2PSNMap[dsn] = psn

			// 5. init the scheduled DSN watermark
			// it only happens there is no record scheduled for apply
			if dsn-1 < r.waterMarks.dsnScheduled {
				if r.stats.schedulePSNCount != 0 {
					// it means a bigger DSN has been scheduled for apply and then there is
					// a smaller DSN with bigger PSN. it should not happen
					panic(fmt.Sprintf(
						"dsn: %d, psn: %d, scheduled: %d",
						dsn, psn, r.waterMarks.dsnScheduled,
					))
				}
				r.waterMarks.dsnScheduled = dsn - 1
			}
		}
	}
	if nextPSN <= r.waterMarks.psnToRead {
		done = true
	} else {
		r.waterMarks.psnToRead = nextPSN
	}
	return
}

// PXU TODO: make sure there is no concurrent write
func (r *replayer2) AppendSkipCmd(
	ctx context.Context,
	skipMap map[uint64]uint64,
) (err error) {
	var (
		now        = time.Now()
		maxRetry   = 10
		retryTimes = 0
	)
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"Wal-Replay-Append-Skip-Entries",
			zap.Any("gsn-psn-map", skipMap),
			zap.Duration("duration", time.Since(now)),
			zap.Int("retry-times", retryTimes),
			zap.Error(err),
		)
	}()

	// the skip map should not be larger than the max client count
	// FIXME: the ClientMaxCount is configurable and it may be different
	// from the last time it writes logs
	if len(skipMap) > r.driver.config.ClientMaxCount {
		err = moerr.NewInternalErrorNoCtxf(
			"too many skip entries, count %d", len(skipMap),
		)
		return
	}

	cmd := NewReplayCmd(skipMap)
	recordEntry := newRecordEntry()
	recordEntry.Meta.metaType = TReplay
	recordEntry.cmd = cmd

	client, writeToken := r.driver.getClientForWrite()
	r.replayedState.writeTokens = append(r.replayedState.writeTokens, writeToken)

	size := recordEntry.prepareRecord()
	client.TryResize(size)
	record := client.record
	copy(record.Payload(), recordEntry.payload)
	record.ResizePayload(size)

	for ; retryTimes < maxRetry; retryTimes++ {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(
			ctx, time.Second*10, moerr.CauseAppendSkipCmd,
		)
		_, err = client.c.Append(ctx, client.record)
		err = moerr.AttachCause(ctx, err)
		cancel()
		if err == nil {
			break
		}
	}
	return
}
