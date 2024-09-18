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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

type FileSinker interface {
	Sink(context.Context, *batch.Batch) error
	Sync(context.Context) (*objectio.ObjectStats, error)
	Reset()
	Close() error
}

var TODOMergeSortFunc func([]*batch.Batch, int, *batch.Batch, *mpool.MPool) error

func ConsructInsertFlow(
	mp *mpool.MPool,
	fs fileservice.FileService,
	loadNextBatch func(context.Context, *batch.Batch, *mpool.MPool) (bool, error),
) *InsertFlow {
	return &InsertFlow{
		loadNextBatch: loadNextBatch,
		mp:            mp,
		fs:            fs,
	}
}

type InsertFlow struct {
	loadNextBatch func(context.Context, *batch.Batch, *mpool.MPool) (bool, error)
	staged        struct {
		inMemory            []*batch.Batch
		persisted           []*objectio.ObjectStats
		inMemorySize        int
		memorySizeThreshold int
		sinker              FileSinker
	}
	results []*objectio.ObjectStats
	buffers []*batch.Batch
	mp      *mpool.MPool
	fs      fileservice.FileService
}

func (flow *InsertFlow) fetchBuffer() *batch.Batch {
	if len(flow.buffers) > 0 {
		bat := flow.buffers[len(flow.buffers)-1]
		flow.buffers = flow.buffers[:len(flow.buffers)-1]
		return bat
	}
	return batch.New(false, ObjectTableAttrs)
}

func (flow *InsertFlow) putbackBuffer(bat *batch.Batch) {
	bat.CleanOnlyData()
	flow.buffers = append(flow.buffers, bat)
}

// bat should be sorted by the first column
// stageData take the ownership of the bat
func (flow *InsertFlow) stageData(
	ctx context.Context, bat *batch.Batch,
) error {
	flow.staged.inMemory = append(flow.staged.inMemory, bat)
	flow.staged.inMemorySize += bat.Size()
	if flow.staged.inMemorySize >= flow.staged.memorySizeThreshold {
		return flow.trySpill(ctx)
	}
	return nil
}

func (flow *InsertFlow) trySpill(ctx context.Context) error {
	sinker := func(data *batch.Batch) error {
		return nil
	}

	// 1. merge sort
	buffer := flow.fetchBuffer() // note the lifecycle of buffer
	defer flow.putbackBuffer(buffer)
	if err := TODOMergeSortFunc(
		flow.staged.inMemory,
		0,
		buffer,
		flow.mp,
	); err != nil {
		return err
	}

	// 2. dedup
	if err := DedupSortedBatches(
		0,
		flow.staged.inMemory,
	); err != nil {
		return err
	}

	// 3. spill
	fSinker := flow.getStageFileSinker()
	defer flow.putStageFileSinker(fSinker)
	for _, bat := range flow.staged.inMemory {
		if err := fSinker.Sink(ctx, bat); err != nil {
			return err
		}
	}
	stats, err := fSinker.Sync(ctx)
	if err != nil {
		return err
	}

	flow.staged.persisted = append(flow.staged.persisted, &stats)

	// put back the in-memory data to the buffer pool
	// and reset the in-memory data
	for i, bat := range flow.staged.inMemory {
		flow.putbackBuffer(bat)
		flow.staged.inMemory[i] = nil
	}
	flow.staged.inMemory = flow.staged.inMemory[:0]
	flow.staged.inMemorySize = 0
}

func (flow *InsertFlow) putStageFileSinker(sinker FileSinker) {
	flow.staged.sinker.Reset()
}
func (flow *InsertFlow) getStageFileSinker() FileSinker {
	// TODO
	// if flow.staged.sinker == nil {
	// 	flow.staged.sinker = NewFileSinker()
	// }
	return flow.staged.sinker
}

func (flow *InsertFlow) Process(ctx context.Context) error {
	for {
		buffer := flow.fetchBuffer()
		done, err := flow.loadNextBatch(ctx, buffer, flow.mp)
		if err != nil || done {
			flow.putbackBuffer(buffer)
			return err
		}
		if err = flow.processOneBatch(ctx, buffer); err != nil {
			flow.putbackBuffer(buffer)
			return err
		}
	}
	return flow.doneAllBatches(ctx)
}

// processOneBatch take the ownership of the buffer
func (flow *InsertFlow) processOneBatch(
	ctx context.Context,
	data *batch.Batch,
) error {
	if err := mergesort.SortColumnsByIndex(
		data.Vecs,
		0,
		flow.mp,
	); err != nil {
		return err
	}
	return flow.stageData(ctx, data)
}

func (flow *InsertFlow) doneAllBatches(ctx context.Context) error {
	if len(flow.staged.persisted) == 0 && len(flow.staged.inMemory) == 0 {
		return nil
	}
	// spill the remaining data
	if len(flow.staged.inMemory) > 0 {
		if err = flow.trySpill(ctx); err != nil {
			return err
		}
	}
	// if there is only one file, it is sorted an deduped
	if len(flow.staged.persisted) == 1 {
		flow.results = append(flow.results, flow.staged.persisted[0])
	}

	// TODO: merge the files and dedup
	// newPersied, err := MergeSortedFilesAndDedup(flow.staged.persisted)
	// if err != nil {
	// 	return err
	// }
	// flow.results = append(flow.results, newPersied...)
	return nil
}

func (flow *InsertFlow) Close() error {
	// TODO
	return nil
}
