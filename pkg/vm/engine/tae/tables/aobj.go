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

package tables

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type aobject struct {
	*baseObject
	frozen     atomic.Bool
	freezelock sync.Mutex
}

func newAObject(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
	isTombstone bool,
) *aobject {
	obj := &aobject{}
	obj.baseObject = newBaseObject(obj, meta, rt)
	if obj.meta.HasDropCommitted() {
		pnode := newPersistedNode(obj.baseObject)
		node := NewNode(pnode)
		node.Ref()
		obj.node.Store(node)
		obj.FreezeAppend()
	} else {
		mnode := newMemoryNode(obj.baseObject, isTombstone)
		node := NewNode(mnode)
		node.Ref()
		obj.node.Store(node)
	}
	return obj
}

func (obj *aobject) FreezeAppend() {
	obj.frozen.Store(true)
}

func (obj *aobject) IsAppendFrozen() bool {
	return obj.frozen.Load()
}

func (obj *aobject) IsAppendable() bool {
	if obj.IsAppendFrozen() {
		return false
	}
	node := obj.PinNode()
	defer node.Unref()
	if node.IsPersisted() {
		return false
	}
	rows, _ := node.Rows()
	return rows < obj.meta.GetSchema().BlockMaxRows
}

func (obj *aobject) PrepareCompactInfo() (result bool, reason string) {
	if n := obj.RefCount(); n > 0 {
		reason = fmt.Sprintf("entering refcount %d", n)
		return
	}
	obj.FreezeAppend()
	if !obj.meta.PrepareCompact() || !obj.appendMVCC.PrepareCompact() {
		if !obj.meta.PrepareCompact() {
			reason = "meta preparecomp false"
		} else {
			reason = "mvcc preparecomp false"
		}
		return
	}

	if n := obj.RefCount(); n != 0 {
		reason = fmt.Sprintf("ending refcount %d", n)
		return
	}
	return obj.RefCount() == 0, reason
}

func (obj *aobject) PrepareCompact() bool {
	if obj.RefCount() > 0 {
		return false
	}

	// see more notes in flushtabletail.go
	obj.freezelock.Lock()
	obj.FreezeAppend()
	obj.freezelock.Unlock()

	obj.meta.RLock()
	defer obj.meta.RUnlock()
	droppedCommitted := obj.meta.HasDropCommittedLocked()

	if droppedCommitted {
		if !obj.meta.PrepareCompactLocked() {
			if obj.meta.CheckPrintPrepareCompactLocked() {
				obj.meta.PrintPrepareCompactDebugLog()
			}
			return false
		}
	} else {
		if !obj.meta.PrepareCompactLocked() {
			if obj.meta.CheckPrintPrepareCompactLocked() {
				obj.meta.PrintPrepareCompactDebugLog()
			}
			return false
		}
		if !obj.appendMVCC.PrepareCompactLocked() /* all appends are committed */ {
			if obj.meta.CheckPrintPrepareCompactLocked() {
				logutil.Infof("obj %v, data prepare compact failed", obj.meta.ID.String())
				if !obj.meta.HasPrintedPrepareComapct {
					obj.meta.HasPrintedPrepareComapct = true
					logutil.Infof("append MVCC %v", obj.appendMVCC.StringLocked())
				}
			}
			return false
		}
	}
	prepareCompact := obj.RefCount() == 0
	if !prepareCompact && obj.meta.CheckPrintPrepareCompactLocked() {
		logutil.Infof("obj %v, data ref count is %d", obj.meta.ID.String(), obj.RefCount())
	}
	return prepareCompact
}

func (obj *aobject) Pin() *common.PinnedItem[*aobject] {
	obj.Ref()
	return &common.PinnedItem[*aobject]{
		Val: obj,
	}
}

func (obj *aobject) GetColumnDataByIds(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema any,
	_ uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	return obj.resolveColumnDatas(
		ctx,
		txn,
		readSchema.(*catalog.Schema),
		colIdxes,
		false,
		mp,
	)
}

func (obj *aobject) GetColumnDataById(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema any,
	_ uint16,
	col int,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	return obj.resolveColumnData(
		ctx,
		txn,
		readSchema.(*catalog.Schema),
		col,
		false,
		mp,
	)
}
func (obj *aobject) GetCommitTSVector(maxRow uint32, mp *mpool.MPool) (containers.Vector, error) {
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return node.MustMNode().getCommitTSVec(maxRow, mp)
	} else {
		vec, err := obj.LoadPersistedCommitTS(0)
		return vec, err
	}
}
func (obj *aobject) GetCommitTSVectorInRange(start, end types.TS, mp *mpool.MPool) (containers.Vector, error) {
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return node.MustMNode().getCommitTSVecInRange(start, end, mp)
	} else {
		vec, err := obj.LoadPersistedCommitTS(0)
		return vec, err
	}
}
func (obj *aobject) resolveColumnDatas(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxes []int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	node := obj.PinNode()
	defer node.Unref()
	if obj.meta.IsTombstone {
		skipDeletes = true
	}

	if !node.IsPersisted() {
		return node.MustMNode().resolveInMemoryColumnDatas(
			ctx,
			txn, readSchema, colIdxes, skipDeletes, mp,
		)
	} else {
		return obj.ResolvePersistedColumnDatas(
			ctx,
			txn,
			readSchema,
			0,
			colIdxes,
			skipDeletes,
			mp,
		)
	}
}

// check if all rows are committed before the specified ts
// here we assume that the ts is greater equal than the block's
// create ts and less than the block's delete ts
// it is a coarse-grained check
func (obj *aobject) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	// if the block is not frozen, always return false
	if !obj.IsAppendFrozen() {
		return false
	}

	node := obj.PinNode()
	defer node.Unref()

	// if the block is in memory, check with the in-memory node
	// it is a fine-grained check if the block is in memory
	if !node.IsPersisted() {
		return node.MustMNode().allRowsCommittedBefore(ts)
	}

	// always return false for if the block is persisted
	// it is a coarse-grained check
	return false
}
func (obj *aobject) GetCommitVec() (containers.Vector, error) {
	return nil, nil
}
func (obj *aobject) resolveColumnData(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	node := obj.PinNode()
	defer node.Unref()

	if obj.meta.IsTombstone {
		skipDeletes = true
	}
	if !node.IsPersisted() {
		return node.MustMNode().resolveInMemoryColumnData(
			txn, readSchema, col, skipDeletes, mp,
		)
	} else {
		return obj.ResolvePersistedColumnData(
			ctx,
			txn,
			readSchema,
			0,
			col,
			skipDeletes,
			mp,
		)
	}
}

func (obj *aobject) GetValue(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	_ uint16,
	row, col int,
	skipCheckDelete bool,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	node := obj.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	if !node.IsPersisted() {
		return node.MustMNode().getInMemoryValue(txn, schema, row, col, skipCheckDelete, mp)
	} else {
		return obj.getPersistedValue(
			ctx, txn, schema, 0, row, col, true, skipCheckDelete, mp,
		)
	}
}

// GetByFilter will read pk column, which seqnum will not change, no need to pass the read schema.
func (obj *aobject) GetByFilter(
	ctx context.Context,
	txn txnif.AsyncTxn,
	filter *handle.Filter,
	mp *mpool.MPool,
) (blkID uint16, offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if obj.meta.GetSchema().SortKey == nil {
		rid := filter.Val.(types.Rowid)
		offset = rid.GetRowOffset()
		return
	}

	node := obj.PinNode()
	defer node.Unref()
	_, offset, err = node.GetRowByFilter(ctx, txn, filter, mp, obj.rt.VectorPool.Small)
	return
}

func (obj *aobject) GetDuplicatedRows(
	ctx context.Context,
	txn txnif.TxnReader,
	keys containers.Vector,
	keysZM index.ZM,
	precommit bool,
	checkWWConflict bool,
	bf objectio.BloomFilter,
	rowIDs containers.Vector,
	mp *mpool.MPool,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Debugf("BatchDedup obj-%s: %v", obj.meta.ID.String(), err)
		}
	}()
	node := obj.PinNode()
	defer node.Unref()
	maxRow := uint32(math.MaxUint32)
	if !precommit {
		obj.RLock()
		maxRow, err = obj.GetMaxRowByTSLocked(txn.GetStartTS())
		obj.RUnlock()
	}
	if !node.IsPersisted() {
		return node.GetDuplicatedRows(
			ctx,
			txn,
			maxRow,
			keys,
			keysZM,
			rowIDs,
			bf,
			precommit,
			checkWWConflict,
			mp,
		)
	} else {
		return obj.persistedGetDuplicatedRows(
			ctx,
			txn,
			precommit,
			keys,
			keysZM,
			rowIDs,
			true,
			maxRow,
			bf,
			mp,
		)
	}
}

func (obj *aobject) GetMaxRowByTSLocked(ts types.TS) (uint32, error) {
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return obj.appendMVCC.GetMaxRowByTSLocked(ts), nil
	} else {
		vec, err := obj.LoadPersistedCommitTS(0)
		if err != nil {
			return 0, err
		}
		for i := uint32(0); i < uint32(vec.Length()); i++ {
			commitTS := vec.Get(int(i)).(types.TS)
			if commitTS.Greater(&ts) {
				return i, nil
			}
		}
		return uint32(vec.Length()), nil
	}
}
func (obj *aobject) Contains(
	ctx context.Context,
	txn txnif.TxnReader,
	precommit bool,
	keys containers.Vector,
	keysZM index.ZM,
	bf objectio.BloomFilter,
	mp *mpool.MPool,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Debugf("BatchDedup obj-%s: %v", obj.meta.ID.String(), err)
		}
	}()
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return node.Contains(
			ctx,
			keys,
			keysZM,
			bf,
			txn,
			precommit,
			mp,
		)
	} else {
		return obj.persistedContains(
			ctx,
			txn,
			precommit,
			keys,
			keysZM,
			true,
			bf,
			mp,
		)
	}
}

func (obj *aobject) CollectAppendInRange(
	ctx context.Context,
	start, end types.TS,
	withAborted bool,
	withPersistedData bool,
	mp *mpool.MPool,
) (*containers.BatchWithVersion, error) {
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return node.CollectAppendInRange(start, end, withAborted, mp)
	} else {
		if !withPersistedData {
			return nil, nil
		}
		return obj.persistedCollectAppendInRange(ctx, start, end, withAborted, mp)
	}
}

func (obj *aobject) persistedCollectAppendInRange(
	ctx context.Context,
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool,
) (*containers.BatchWithVersion, error) {
	if !obj.meta.IsTombstone {
		panic("not support")
	}
	obj.meta.RLock()
	minTS := obj.meta.GetCreatedAtLocked()
	maxTS := obj.meta.GetDeleteAtLocked()
	obj.meta.RUnlock()
	if minTS.Greater(&end) || maxTS.Less(&start) {
		return nil, nil
	}
	var bat *containers.Batch
	readSchema := obj.meta.GetTable().GetLastestSchema(true)
	id := obj.meta.AsCommonID()
	location, err := obj.buildMetalocation(uint16(0))
	if err != nil {
		return nil, err
	}
	vecs, err := LoadPersistedColumnDatas(ctx, readSchema, obj.rt, id, catalog.TombstoneBatchIdxes, location, mp)
	if err != nil {
		return nil, err
	}
	commitTSVec, err := obj.GetCommitTSVectorInRange(start, end, mp)
	if err != nil {
		return nil, err
	}

	rowIDs := vector.MustFixedCol[types.Rowid](vecs[0].GetDownstreamVector())
	commitTSs := vector.MustFixedCol[types.TS](commitTSVec.GetDownstreamVector())
	for i := 0; i < vecs[0].Length(); i++ {
		if commitTSs[0].Less(&start) || commitTSs[0].Greater(&end) {
			continue
		}
		if bat == nil {
			bat = containers.BuildBatch(
				readSchema.Attrs(),
				readSchema.AllTypes(),
				containers.Options{Allocator: mp})
			bat.AddVector(
				catalog.AttrCommitTs,
				containers.MakeVector(types.T_TS.ToType(),
					mp))
		}
		bat.GetVectorByName(catalog.AttrRowID).Append(rowIDs[i], false)
		bat.GetVectorByName(catalog.AttrPKVal).Append(vecs[1].Get(i), false)
		bat.GetVectorByName(catalog.AttrCommitTs).Append(commitTSs[i], false)
	}
	batchWithVersion := &containers.BatchWithVersion{
		Version:    readSchema.Version,
		NextSeqnum: uint16(readSchema.Extra.NextColSeqnum),
		Seqnums:    readSchema.AllSeqnums(),
		Batch:      bat,
	}
	return batchWithVersion, nil
}

func (obj *aobject) estimateRawScore() (score int, dropped bool, err error) {
	if obj.meta.HasDropCommitted() {
		dropped = true
		return
	}
	obj.meta.RLock()
	atLeastOneCommitted := obj.meta.HasCommittedNodeLocked()
	obj.meta.RUnlock()
	if !atLeastOneCommitted {
		score = 1
		return
	}

	rows, err := obj.Rows()
	if rows == int(obj.meta.GetSchema().BlockMaxRows) {
		score = 100
		return
	}

	if rows == 0 {
		score = 0
	} else {
		score = 1
	}

	if score > 0 {
		if _, terminated := obj.meta.GetTerminationTS(); terminated {
			score = 100
		}
	}
	return
}

func (obj *aobject) RunCalibration() (score int, err error) {
	score, _, err = obj.estimateRawScore()
	return
}

func (obj *aobject) OnReplayAppend(node txnif.AppendNode) (err error) {
	an := node.(*updates.AppendNode)
	obj.appendMVCC.OnReplayAppendNode(an)
	return
}

func (obj *aobject) OnReplayAppendPayload(bat *containers.Batch) (err error) {
	appender, err := obj.MakeAppender()
	if err != nil {
		return
	}
	_, err = appender.ReplayAppend(bat, nil)
	return
}

func (obj *aobject) MakeAppender() (appender data.ObjectAppender, err error) {
	if obj == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	appender = newAppender(obj)
	return
}

func (obj *aobject) Init() (err error) { return }

func (obj *aobject) EstimateMemSize() (int, int) {
	node := obj.PinNode()
	defer node.Unref()
	obj.RLock()
	defer obj.RUnlock()
	asize := obj.appendMVCC.EstimateMemSizeLocked()
	if !node.IsPersisted() {
		asize += node.MustMNode().EstimateMemSizeLocked()
	}
	return asize, 0
}

func (obj *aobject) GetRowsOnReplay() uint64 {
	if obj.meta.HasDropCommitted() {
		return uint64(obj.meta.GetLatestCommittedNodeLocked().
			BaseNode.ObjectStats.Rows())
	}
	return uint64(obj.appendMVCC.GetTotalRow())
}
