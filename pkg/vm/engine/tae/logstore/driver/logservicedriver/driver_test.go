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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lni/vfs"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"

	"github.com/stretchr/testify/assert"
)

func initTest(t *testing.T) (*logservice.Service, *logservice.ClientConfig) {
	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	fs := vfs.NewStrictMem()
	service, ccfg, err := logservice.NewTestService(fs)
	assert.NoError(t, err)
	return service, &ccfg
}

func restartDriver(t *testing.T, d *LogServiceDriver, h func(*entry.Entry)) *LogServiceDriver {
	assert.NoError(t, d.Close())
	t.Log("Addr:")
	// preAddr:=d.addr
	for lsn, intervals := range d.psn.dsnMap {
		t.Logf("%d %v", lsn, intervals)
	}
	// preLsns:=d.validPSN
	t.Logf("Valid lsn: %v", d.psn.records)
	t.Logf("Driver DSN %d, Syncing %d, Synced %d", d.dsn, d.watermark.committingDSN, d.watermark.committedDSN)
	t.Logf("Truncated %d", d.truncateDSNIntent.Load())
	t.Logf("LSTruncated %d", d.truncatedPSN)
	d = NewLogServiceDriver(&d.config)
	tempLsn := uint64(0)
	err := d.Replay(context.Background(), func(e *entry.Entry) driver.ReplayEntryState {
		if e.DSN <= tempLsn {
			panic("logic err")
		}
		tempLsn = e.DSN
		if h != nil {
			h(e)
		}
		return driver.RE_Nomal
	})
	assert.NoError(t, err)
	t.Log("Addr:")
	for lsn, intervals := range d.psn.dsnMap {
		t.Logf("%d %v", lsn, intervals)
	}
	// assert.Equal(t,len(preAddr),len(d.addr))
	// for lsn,intervals := range preAddr{
	// 	replayedInterval,ok:=d.addr[lsn]
	// 	assert.True(t,ok)
	// 	assert.Equal(t,intervals.Intervals[0].Start,replayedInterval.Intervals[0].Start)
	// 	assert.Equal(t,intervals.Intervals[0].End,replayedInterval.Intervals[0].End)
	// }
	t.Logf("Valid lsn: %v", d.psn.records)
	// assert.Equal(t,preLsns.GetCardinality(),d.validPSN.GetCardinality())
	t.Logf("Truncated %d", d.truncateDSNIntent.Load())
	t.Logf("LSTruncated %d", d.truncatedPSN)
	return d
}

func TestReplay1(t *testing.T) {
	// t.Skip("debug")
	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig("", ccfg)
	driver := NewLogServiceDriver(cfg)

	entryCount := 10000
	entries := make([]*entry.Entry, entryCount)

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		driver.Append(e)
		entries[i] = e
	}

	for _, e := range entries {
		e.WaitDone()
	}

	// i := 0
	// h := func(e *entry.Entry) {
	// 	payload := []byte(fmt.Sprintf("payload %d", i))
	// 	assert.Equal(t, payload, e.Entry.GetPayload())
	// 	i++
	// }

	driver = restartDriver(t, driver, nil)

	for _, e := range entries {
		e.Entry.Free()
	}

	driver.Close()
}

func TestReplay2(t *testing.T) {
	t.Skip("debug")

	service, ccfg := initTest(t)
	defer service.Close()

	cfg := NewTestConfig("", ccfg)
	cfg.RecordSize = 100
	driver := NewLogServiceDriver(cfg)

	entryCount := 10000
	entries := make([]*entry.Entry, entryCount)

	for i := 0; i < entryCount; i++ {
		payload := []byte(fmt.Sprintf("payload %d", i))
		e := entry.MockEntryWithPayload(payload)
		driver.Append(e)
		entries[i] = e
	}

	synced := driver.getCommittedDSNWatermark()
	driver.Truncate(synced)

	for i, e := range entries {
		e.WaitDone()
		assert.Equal(t, uint64(i+1), e.DSN)
	}

	truncated, err := driver.GetTruncated()
	i := truncated
	t.Logf("truncate %d", i)
	assert.NoError(t, err)
	h := func(e *entry.Entry) {
		entryPayload := e.Entry.GetPayload()
		strs := strings.Split(string(entryPayload), " ")
		id, err := strconv.Atoi(strs[1])
		assert.NoError(t, err)
		if id <= int(truncated) {
			return
		}

		payload := []byte(fmt.Sprintf("payload %d", i))
		assert.Equal(t, payload, entryPayload)
		i++
	}

	driver = restartDriver(t, driver, h)

	for _, e := range entries {
		e.Entry.Free()
	}

	driver.Close()
}

func Test_RetryWithTimeout(t *testing.T) {
	tryFunc := func() bool {
		return false
	}
	err := RetryWithTimeout(time.Second*0, tryFunc)
	assert.Error(t, err)

	err = RetryWithTimeout(time.Millisecond*3, tryFunc)
	assert.Error(t, err)
}
