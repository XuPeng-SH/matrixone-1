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

package checkpoint

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
	"go.uber.org/zap"
)

var ErrCheckpointExecutorBusy = moerr.NewInternalErrorNoCtx("checkpoint executor is busy")

const (
	JT_RunICKP tasks.JobType = 400 + iota
	JT_RunGCKP
)

func init() {
	tasks.RegisterJobType(JT_RunICKP, "RunICKPJob")
	tasks.RegisterJobType(JT_RunGCKP, "RunGCKPJob")
}

type checkpointJob struct {
	tasks.Job
	entry *CheckpointEntry
}

func (job *checkpointJob) String() string {
	return fmt.Sprintf("CheckpointJob{%s}", job.entry.String())
}

func (job *checkpointJob) WaitC() <-chan struct{} {
	ch := make(chan struct{}, 1)
	go func() {
		job.WaitDone()
		close(ch)
	}()
	return ch
}

func newExecutor() *executor {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &executor{
		ctx:    ctx,
		cancel: cancel,
	}
}

type executor struct {
	sid          string
	catalogCache *catalog.Catalog
	wal          wal.Driver
	sourcer      logtail.Collector
	store        *runnerStore
	ctx          context.Context
	cancel       context.CancelCauseFunc
	running      atomic.Pointer[checkpointJob]
	watermark    atomic.Value

	onCheckpointDone func(context.Context, *CheckpointEntry)

	options struct {
		blockRows           int
		size                int
		entryCountToReserve uint64
	}

	fs fileservice.FileService
}

func (e *executor) Close() {
	e.cancel(context.Canceled)
	if running := e.running.Load(); running != nil {
		running.WaitDone()
		e.running.Store(nil)
	}
}

func (e *executor) Schedule(
	ctx context.Context, entry *CheckpointEntry, blocking bool,
) (*checkpointJob, error) {
	var job *checkpointJob
	for {
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case <-e.ctx.Done():
			return nil, context.Cause(e.ctx)
		default:
		}

		running := e.running.Load()
		if running != nil && !blocking {
			return nil, ErrCheckpointExecutorBusy
		} else if running != nil {
			// running != nil && blocking

			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			case <-e.ctx.Done():
				return nil, context.Cause(ctx)
			case <-running.WaitC():
			}
		} else {
			// running == nil

			job = &checkpointJob{entry: entry}
			var (
				jt tasks.JobType
				op tasks.JobExecutor
			)
			if entry.IsIncremental() {
				jt = JT_RunICKP
				op = e.runICKP
			} else {
				jt = JT_RunGCKP
				op = e.runGCKP
			}
			job.Init(
				ctx,
				"",
				jt,
				op,
			)
			if e.running.CompareAndSwap(nil, job) {
				go job.Run()
				break
			}
			job.Close()
			job = nil
		}
	}
	return job, nil
}

func (e *executor) checkEntryValidity(entry *CheckpointEntry) error {
	var watermark types.TS
	if v := e.watermark.Load(); v != nil {
		watermark = v.(types.TS)
	}
	if entry.IsIncremental() {
		start := entry.GetStart()
		if !watermark.EQ(&start) {
			return moerr.NewInternalErrorNoCtxf(
				"cannot schedule incremental checkpoint job: %s, watermark: %s, start: %s",
				entry.String(), watermark.ToString(), start.ToString(),
			)
		}
	} else {
		ts := entry.GetEnd()
		if watermark.GE(&ts) {
			return moerr.NewInternalErrorNoCtxf(
				"cannot schedule global checkpoint job: %s, watermark: %s, end: %s",
				entry.String(), watermark.ToString(), ts.ToString(),
			)
		}
	}
	return nil
}

func (e *executor) runICKP(ctx context.Context) *tasks.JobResult {
	var (
		err           error
		detail        string
		lsn           uint64
		lsnToTruncate uint64

		result    tasks.JobResult
		start     = time.Now()
		entry     = e.running.Load().entry
		zapFields = make([]zap.Field, 0, 10)
	)

	logutil.Info(
		"Checkpoint-Start",
		zap.String("entry", entry.String()),
	)
	defer func() {
		if err != nil {
			result.Err = err
			logutil.Error(
				"Checkpoint-Error",
				zap.String("entry", entry.String()),
				zap.Duration("duration", time.Since(start)),
				zap.String("detail", detail),
				zap.Error(result.Err),
			)
		} else {
			zapFields = append(zapFields, zap.Duration("duration", time.Since(start)))
			zapFields = append(zapFields, zap.Uint64("lsn", lsn))
			zapFields = append(zapFields, zap.Uint64("lsn-to-truncate", lsnToTruncate))
			zapFields = append(zapFields, zap.String("entry", entry.String()))
			zapFields = append(zapFields, zap.Uint64("reserved-count", e.options.entryCountToReserve))
			logutil.Info(
				"Checkpoint-End",
				zapFields...,
			)
		}
	}()

	if err = e.checkEntryValidity(entry); err != nil {
		return &result
	}

	var (
		ickpFiles []string
		metaFile  string
	)
	if zapFields, ickpFiles, err = e.writeICKPFiles(
		ctx, entry,
	); err != nil {
		detail = "writeICKPFiles"
		return &result
	}

	lsn = e.sourcer.GetMaxLSN(entry.start, entry.end)
	if lsn > e.options.entryCountToReserve {
		lsnToTruncate = lsn - e.options.entryCountToReserve
	}

	if metaFile, err = e.saveMetaFile(
		ctx, entry.start, entry.end, 0, 0,
	); err != nil {
		detail = "saveMetaFile"
		return &result
	}

	{
		e.store.AddMetaFile(
			blockio.EncodeCheckpointMetadataFileNameWithoutDir(
				PrefixMetadata, entry.start, entry.end,
			),
		)
		entry.SetLSN(lsn, lsnToTruncate)
		entry.SetState(ST_Finished)
		e.watermark.Store(entry.end)
	}

	ickpFiles = append(ickpFiles, metaFile)

	// PXU TODO: how to handle the wal error
	var logEntry wal.LogEntry
	if logEntry, err = e.wal.RangeCheckpoint(
		1, lsnToTruncate, ickpFiles...,
	); err != nil {
		detail = "wal-ckp"
		return &result
	}
	if err = logEntry.WaitDone(); err != nil {
		detail = "wait-wal-ckp-done"
		return &result
	}

	if e.onCheckpointDone != nil {
		e.onCheckpointDone(ctx, entry)
	}
	return &result
}

// TODO: use ctx
func (e *executor) saveMetaFile(
	ctx context.Context, start, end types.TS, lsn, lsnToTruncate uint64,
) (fileName string, err error) {
	bat := e.store.MakeMetadataBatch(
		ctx, start, end, lsn, lsnToTruncate,
	)
	defer bat.Close()
	fileName = blockio.EncodeCheckpointMetadataFileName(
		CheckpointDir, PrefixMetadata, start, end,
	)
	var writer *objectio.ObjectWriter
	if writer, err = objectio.NewObjectWriterSpecial(
		objectio.WriterCheckpoint, fileName, e.fs,
	); err != nil {
		return
	}

	if _, err = writer.Write(containers.ToCNBatch(bat)); err != nil {
		return
	}

	// TODO: checkpoint entry should maintain the location
	_, err = writer.WriteEnd(ctx)
	return
}

func (e *executor) writeICKPFiles(
	ctx context.Context,
	entry *CheckpointEntry,
) (zapFields []zap.Field, ickpFiles []string, err error) {
	factory := logtail.IncrementalCheckpointDataFactory(
		e.sid, entry.start, entry.end, true,
	)
	var data *logtail.CheckpointData
	if data, err = factory(e.catalogCache); err != nil {
		return
	}
	defer data.Close()

	var (
		cnLocation, tnLocation objectio.Location
	)
	if cnLocation, tnLocation, ickpFiles, err = data.WriteTo(
		e.fs,
		e.options.blockRows,
		e.options.size,
	); err != nil {
		return
	}

	ickpFiles = append(ickpFiles, cnLocation.Name().String())
	entry.SetLocation(cnLocation, tnLocation)

	zapFields = data.ExportStats("")

	perfcounter.Update(e.ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.CheckPoint.DoIncrementalCheckpoint.Add(1)
	})
	return
}

func (e *executor) runGCKP(ctx context.Context) *tasks.JobResult {
	var result tasks.JobResult
	entry := e.running.Load().entry
	if err := e.checkEntryValidity(entry); err != nil {
		result.Err = err
		return &result
	}
	// TODO
	return &result
}
