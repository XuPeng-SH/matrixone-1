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

package gc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

const (
	JT_GCNoop tasks.JobType = 300 + iota
	JT_GCExecute
	JT_GCReplay
	JT_GCReplayAndExecute
)

func init() {
	tasks.RegisterJobType(JT_GCNoop, "GCNoopJob")
	tasks.RegisterJobType(JT_GCExecute, "GCExecute")
	tasks.RegisterJobType(JT_GCReplay, "GCReplay")
	tasks.RegisterJobType(JT_GCReplayAndExecute, "GCReplayAndExecute")
}

type StateStep = uint32

const (
	StateStep_Write StateStep = iota
	StateStep_Write2Replay
	StateStep_Replay
	StateStep_Replay2Write
)

// DiskCleaner is the main structure of v2 operation,
// and provides "JobFactory" to let tae notify itself
// to perform a v2
type DiskCleaner struct {
	cleaner Cleaner

	step        atomic.Uint32
	replayError atomic.Value

	processQueue sm.Queue

	onceStart sync.Once
	onceStop  sync.Once
}

func NewDiskCleaner(
	diskCleaner Cleaner, isWriteMode bool,
) *DiskCleaner {
	cleaner := &DiskCleaner{
		cleaner: diskCleaner,
	}
	cleaner.step.Store(StateStep_Write)

	cleaner.processQueue = sm.NewSafeQueue(1000, 100, cleaner.process)
	return cleaner
}

func (cleaner *DiskCleaner) GC(ctx context.Context) (err error) {
	return cleaner.scheduleGCJob(ctx)
}

func (cleaner *DiskCleaner) SwitchToReplayMode(ctx context.Context) (err error) {
	oldStep := cleaner.step.Load()
	switch oldStep {
	case StateStep_Replay:
		return
	case StateStep_Replay2Write, StateStep_Write2Replay:
		err = moerr.NewTxnControlErrorNoCtxf("Bad cleaner state: %d", oldStep)
		return
	}

	if !cleaner.step.CompareAndSwap(oldStep, StateStep_Write2Replay) {
		err = moerr.NewTxnControlErrorNoCtxf("Bad cleaner state: %d", oldStep)
		return
	}
	defer func() {
		// any error occurs, switch back to StateStep_Write
		if err != nil {
			cleaner.step.Store(StateStep_Write)
		}
	}()

	// the current state is StateStep_Write2Replay

	noopJob := new(tasks.Job)
	noopJob.Init(
		ctx,
		uuid.Must(uuid.NewV7()).String(),
		JT_GCNoop,
		func(context.Context) *tasks.JobResult {
			logutil.Info(
				"GC-SwitchToReplay",
				zap.String("job", noopJob.String()),
			)
			return nil
		},
	)

	if _, err = cleaner.processQueue.Enqueue(noopJob); err != nil {
		return
	}
	if result := noopJob.WaitDone(); result.Err != nil {
		err = result.Err
		return
	}
	cleaner.step.Store(StateStep_Replay)
	return
}

func (cleaner *DiskCleaner) GetCleaner() Cleaner {
	return cleaner.cleaner
}

// should only be called during the startup
// no check for the current state
func (cleaner *DiskCleaner) forceScheduleJob(jt tasks.JobType) (err error) {
	_, err = cleaner.processQueue.Enqueue(jt)
	return
}

// only can be executed in StateStep_Write
// otherwise, return moerr.NewTxnControlErrorNoCtxf("GC-Not-Write-Mode")
func (cleaner *DiskCleaner) scheduleGCJob(ctx context.Context) (err error) {
	if step := cleaner.step.Load(); step != StateStep_Write {
		err = moerr.NewTxnControlErrorNoCtxf("GC-Not-Write-Mode")
		return
	}
	logutil.Info("GC-Send-Intents")
	_, err = cleaner.processQueue.Enqueue(JT_GCExecute)
	return
}

// execute the GC job
// 1. it should be replayed first with no error
// 2. then execute the GC job
func (cleaner *DiskCleaner) doExecute() (err error) {
	now := time.Now()
	msg := "GC-Execute"
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			msg,
			zap.Duration("duration", time.Since(now)),
			zap.Error(err),
		)
	}()
	if replayErr := cleaner.replayError.Load(); replayErr != nil {
		if err = cleaner.cleaner.Replay(); err != nil {
			msg = "GC-Replay"
			cleaner.replayError.Store(err)
			return
		} else {
			cleaner.replayError.Store(nil)
		}
	}
	err = cleaner.cleaner.Process()
	return
}

// it will update the replayError after replay
func (cleaner *DiskCleaner) doReplay() (err error) {
	if err = cleaner.cleaner.Replay(); err != nil {
		logutil.Error("GC-Replay-Error", zap.Error(err))
		cleaner.replayError.Store(err)
	} else {
		cleaner.replayError.Store(nil)
	}
	return
}

func (cleaner *DiskCleaner) doReplayAndExecute() (err error) {
	// defer func() {
	// 	if err := recover(); err != nil {
	// 		logutil.Error("GC-Replay-Panic", zap.Any("err", err))
	// 	}
	// }()
	msg := "GC-Replay"
	now := time.Now()
	defer func() {
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			msg,
			zap.Duration("duration", time.Since(now)),
			zap.Error(err),
		)
	}()
	if err = cleaner.doReplay(); err != nil {
		return
	}
	msg = "GC-TryGC"
	err = cleaner.cleaner.TryGC()
	return
}

func (cleaner *DiskCleaner) process(items ...any) {
	for _, item := range items {
		switch v := item.(type) {
		case tasks.JobType:
			switch v {
			case JT_GCReplay:
				cleaner.doReplay()
			case JT_GCReplayAndExecute:
				cleaner.doReplayAndExecute()
			case JT_GCExecute:
				cleaner.doExecute()
			default:
				logutil.Error("GC-Unknown-JobType", zap.Any("job-type", v))
			}
		case *tasks.Job:
			v.Run()
		}
	}
}

func (cleaner *DiskCleaner) Start() {
	cleaner.onceStart.Do(func() {
		cleaner.processQueue.Start()
		step := cleaner.step.Load()
		switch step {
		case StateStep_Write:
			if err := cleaner.forceScheduleJob(JT_GCReplayAndExecute); err != nil {
				panic(err)
			}
		case StateStep_Replay:
			if err := cleaner.forceScheduleJob(JT_GCReplay); err != nil {
				panic(err)
			}
		default:
			panic(fmt.Sprintf("Bad cleaner state: %d", step))
		}
	})
}

func (cleaner *DiskCleaner) Stop() {
	cleaner.onceStop.Do(func() {
		cleaner.processQueue.Stop()
		cleaner.cleaner.Stop()
		logutil.Info(
			"GC-DiskCleaner-Started",
			zap.Uint32("step", cleaner.step.Load()),
		)
	})
}
