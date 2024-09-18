package gc

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

type InsertFlow struct {
	staged struct {
		inMemory            []*batch.Batch
		persisted           []*objectio.ObjectStats
		wholeSize           int
		whileRows           int
		inMemorySize        int
		memorySizeThreshold int
	}
	reusable []*batch.Batch
	mp       *mpool.MPool
	fs       fileservice.FileService
}

func (flow *InsertFlow) fetchBatch() *batch.Batch {
	if len(flow.reusable) > 0 {
		bat := flow.reusable[len(flow.reusable)-1]
		flow.reusable = flow.reusable[:len(flow.reusable)-1]
		return bat
	}
	return batch.New(false, ObjectTableAttrs)
}

func (flow *InsertFlow) putBackBatch(bat *batch.Batch) {
	bat.CleanOnlyData()
	flow.reusable = append(flow.reusable, bat)
}

// bat should be sorted by the first column
func (flow *InsertFlow) stageData(
	ctx context.Context, bat *batch.Batch,
) error {
	flow.staged.inMemory = append(flow.staged.inMemory, bat)
	flow.staged.inMemorySize += bat.Size()
	if flow.staged.inMemorySize >= flow.staged.memorySizeThreshold {
		return flow.stageBySpill(ctx)
	}
	return nil
}

func (flow *InsertFlow) stageBySpill(ctx context.Context) error {

}

func (flow *InsertFlow) processInputData(
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
