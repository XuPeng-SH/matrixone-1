// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TestFlushBailoutPos1 struct{}
type TestFlushBailoutPos2 struct{}

var FlushTableTailTaskFactory = func(
	metas, tombstones []*catalog.ObjectEntry, rt *dbutils.Runtime, endTs types.TS, /* end of dirty range*/
) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewFlushTableTailTask(ctx, txn, metas, tombstones, rt, endTs)
	}
}

type flushTableTailTask struct {
	*tasks.BaseTask
	txn        txnif.AsyncTxn
	rt         *dbutils.Runtime
	dirtyEndTs types.TS

	scopes          []common.ID
	schema          *catalog.Schema
	tombstoneSchema *catalog.Schema

	rel  handle.Relation
	dbid uint64

	// record the row mapping from deleted blocks to created blocks
	transMappings *api.BlkTransferBooking

	aObjMetas         []*catalog.ObjectEntry
	aObjHandles       []handle.Object
	createdObjHandles handle.Object

	aTombstoneMetas         []*catalog.ObjectEntry
	aTombstoneHandles       []handle.Object
	createdTombstoneHandles handle.Object

	createdMergedObjectName    string
	createdMergedTombstoneName string

	mergeRowsCnt, aObjDeletesCnt, tombstoneMergeRowsCnt int
}

// A note about flush start timestamp
//
// As the last **committed** time, not the newest allcated time,
// is used in NewFlushTableTailTask, there will be a situation that
// some commiting appends prepared between committed-time and aobj-freeze-time
// are ignored during the data collection stage of flushing,
// which leads to transfer-row-not-found problem.
//
// The proposed solution is to add a check function in NewFlushTableTailTask
// to figure out if there exist an AppendNode with a bigger prepared time
// than flush-start-ts, and if so, retry the flush task
//
// Two question:
//
// 1. How about deletes prepared in that special time range?
//    Never mind, deletes will be transfered when committing the flush task
// 2. Is it guaranteed that the check function is able to see all possible AppendNodes?
//    Probably no, because getting appender and attaching AppendNode are not atomic group opertions.
//    Imagine:
//
//                freeze  check
// committed  x1     |     |     x2
// prepared          |     |  o2
// preparing    i2   |     |
//
// - x1 is the last committed time.
// - getting appender(i2 in graph) is before the freezing
// - attaching AppendNode successfully (o2 in graph) after the check
// - finishing commit at x2
//
// So in order for the check function to work, a dedicated lock is added
// on ablock to ensure that NO AppendNode will be attatched to ablock
// after the very moment when the ablock is freezed.
//
// In the first version proposal, the check in NewFlushTableTailTask is omitted,
// because the existing PrepareCompact in ablock already handles that thing.
// If the last AppendNode in an ablock is not committed, PrepareCompact will
// return false to reschedule the task. However, commiting AppendNode doesn't
// guarantee that the committs has been updated. It's still possible to get a
// old startts which is not able to collect all appends in the ablock.

func NewFlushTableTailTask(
	ctx *tasks.Context,
	txn txnif.AsyncTxn,
	objs []*catalog.ObjectEntry,
	tombStones []*catalog.ObjectEntry,
	rt *dbutils.Runtime,
	dirtyEndTs types.TS,
) (task *flushTableTailTask, err error) {
	task = &flushTableTailTask{
		txn:        txn,
		rt:         rt,
		dirtyEndTs: dirtyEndTs,
	}

	var meta *catalog.ObjectEntry
	if len(objs) != 0 {
		meta = objs[0]
	} else {
		meta = tombStones[0]
	}
	dbId := meta.GetTable().GetDB().ID
	task.dbid = dbId
	var database handle.Database
	database, err = txn.UnsafeGetDatabase(dbId)
	if err != nil {
		return
	}
	tableId := meta.GetTable().ID
	var rel handle.Relation
	rel, err = database.UnsafeGetRelation(tableId)
	task.rel = rel
	if err != nil {
		return
	}
	task.schema = rel.Schema(false).(*catalog.Schema)

	for _, obj := range objs {
		task.scopes = append(task.scopes, *obj.AsCommonID())
		var hdl handle.Object
		hdl, err = rel.GetObject(&obj.ID, false)
		if err != nil {
			return
		}
		if obj.IsTombstone {
			panic(fmt.Sprintf("logic err, obj %v is tombstone", obj.ID.String()))
		}
		if !hdl.IsAppendable() {
			panic(fmt.Sprintf("logic err %v is nonappendable", hdl.GetID().String()))
		}
		task.aObjMetas = append(task.aObjMetas, obj)
		task.aObjHandles = append(task.aObjHandles, hdl)
		if obj.GetObjectData().CheckFlushTaskRetry(txn.GetStartTS()) {
			return nil, txnif.ErrTxnNeedRetry
		}
	}

	task.tombstoneSchema = rel.Schema(true).(*catalog.Schema)

	for _, obj := range tombStones {
		task.scopes = append(task.scopes, *obj.AsCommonID())
		var hdl handle.Object
		hdl, err = rel.GetObject(&obj.ID, true)
		if err != nil {
			return
		}
		if !obj.IsTombstone {
			panic(fmt.Sprintf("logic err, obj %v is not tombstone", obj.ID.String()))
		}
		if !hdl.IsAppendable() {
			panic(fmt.Sprintf("logic err %v is nonappendable", hdl.GetID().String()))
		}
		task.aTombstoneMetas = append(task.aTombstoneMetas, obj)
		task.aTombstoneHandles = append(task.aTombstoneHandles, hdl)
		if obj.GetObjectData().CheckFlushTaskRetry(txn.GetStartTS()) {
			return nil, txnif.ErrTxnNeedRetry
		}
	}

	task.transMappings = mergesort.NewBlkTransferBooking(len(task.aObjHandles))

	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)

	return
}

// Scopes is used in conflict checking in scheduler. For ScopedTask interface
func (task *flushTableTailTask) Scopes() []common.ID { return task.scopes }

// Name is for ScopedTask interface
func (task *flushTableTailTask) Name() string {
	return fmt.Sprintf("[%d]FT-%d-%s", task.ID(), task.rel.ID(), task.schema.Name)
}

func (task *flushTableTailTask) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	enc.AddString("endTs", task.dirtyEndTs.ToString())
	objs := ""
	for _, obj := range task.aObjMetas {
		objs = fmt.Sprintf("%s%s,", objs, obj.ID.ShortStringEx())
	}
	enc.AddString("a-objs", objs)
	tombstones := ""
	for _, obj := range task.aTombstoneMetas {
		tombstones = fmt.Sprintf("%s%s,", tombstones, obj.ID.ShortStringEx())
	}
	enc.AddString("a-tombstones", tombstones)

	toObjs := ""
	if task.createdObjHandles != nil {
		id := task.createdObjHandles.GetID()
		toObjs = fmt.Sprintf("%s%s,", toObjs, id.ShortStringEx())
	}
	if toObjs != "" {
		enc.AddString("to-objs", toObjs)
	}

	toTombstones := ""
	if task.createdTombstoneHandles != nil {
		id := task.createdTombstoneHandles.GetID()
		toTombstones = fmt.Sprintf("%s%s,", toTombstones, id.ShortStringEx())
	}
	if toTombstones != "" {
		enc.AddString("to-tombstones", toTombstones)
	}
	return
}

func (task *flushTableTailTask) Execute(ctx context.Context) (err error) {
	logutil.Info("[Start]", common.OperationField(task.Name()), common.OperandField(task),
		common.OperandField(len(task.aObjHandles)+len(task.aTombstoneHandles)))

	phaseDesc := ""
	defer func() {
		if err != nil {
			logutil.Error("[DoneWithErr]", common.OperationField(task.Name()),
				common.AnyField("error", err),
				common.AnyField("phase", phaseDesc),
			)
		}
	}()
	now := time.Now()

	/////////////////////
	//// phase seperator
	///////////////////

	phaseDesc = "1-flushing appendable objects for snapshot"
	snapshotSubtasks, err := task.flushAObjsForSnapshot(ctx, false)
	if err != nil {
		return
	}
	defer func() {
		releaseFlushObjTasks(snapshotSubtasks, err)
	}()

	/////////////////////
	//// phase seperator
	///////////////////

	phaseDesc = "1-flushing appendable tombstones for snapshot"
	tombstoneSnapshotSubtasks, err := task.flushAObjsForSnapshot(ctx, true)
	if err != nil {
		return
	}
	defer func() {
		releaseFlushObjTasks(tombstoneSnapshotSubtasks, err)
	}()

	/////////////////////
	//// phase seperator
	///////////////////

	phaseDesc = "1-merge aobjects"
	// merge aobjects, no need to wait, it is a sync procedure, that is why put it
	// after flushAObjsForSnapshot
	if err = task.mergeAObjs(ctx, false); err != nil {
		return
	}

	if v := ctx.Value(TestFlushBailoutPos1{}); v != nil {
		err = moerr.NewInternalErrorNoCtx("test merge bail out")
		return
	}

	/////////////////////
	//// phase seperator
	///////////////////

	phaseDesc = "1-merge atombstones"
	// merge atombstones, no need to wait, it is a sync procedure, that is why put it
	// after flushAObjsForSnapshot
	if err = task.mergeAObjs(ctx, true); err != nil {
		return
	}

	if v := ctx.Value(TestFlushBailoutPos1{}); v != nil {
		err = moerr.NewInternalErrorNoCtx("test merge bail out")
		return
	}

	/////////////////////
	//// phase seperator
	///////////////////
	phaseDesc = "1-waiting flushing appendable blocks for snapshot"
	// wait flush tasks
	if err = task.waitFlushAObjForSnapshot(ctx, snapshotSubtasks, false); err != nil {
		return
	}

	/////////////////////
	//// phase seperator
	///////////////////

	phaseDesc = "1-waiting flushing appendable tombstones for snapshot"
	if err = task.waitFlushAObjForSnapshot(ctx, tombstoneSnapshotSubtasks, true); err != nil {
		return
	}

	phaseDesc = "1-wait LogTxnEntry"
	txnEntry := txnentries.NewFlushTableTailEntry(
		task.txn,
		task.ID(),
		task.transMappings,
		task.rel.GetMeta().(*catalog.TableEntry),
		task.aObjMetas,
		task.aObjHandles,
		task.createdObjHandles,
		task.createdMergedObjectName,
		task.aTombstoneMetas,
		task.aTombstoneHandles,
		task.createdTombstoneHandles,
		task.createdMergedTombstoneName,
		task.rt,
	)
	readset := make([]*common.ID, 0, len(task.aObjMetas))
	for _, obj := range task.aObjMetas {
		readset = append(readset, obj.AsCommonID())
	}
	tombstoneReadset := make([]*common.ID, 0, len(task.aTombstoneMetas))
	for _, obj := range task.aTombstoneMetas {
		tombstoneReadset = append(readset, obj.AsCommonID())
	}
	if err = task.txn.LogTxnEntry(
		task.dbid,
		task.rel.ID(),
		txnEntry,
		readset,
		tombstoneReadset,
	); err != nil {
		return
	}
	/////////////////////

	duration := time.Since(now)
	logutil.Info("[End]", common.OperationField(task.Name()),
		common.AnyField("txn-start-ts", task.txn.GetStartTS().ToString()),
		zap.Int("aobj-deletes", task.aObjDeletesCnt),
		zap.Int("aobj-merge-rows", task.mergeRowsCnt),
		zap.Int("tombstone-rows", task.tombstoneMergeRowsCnt),
		common.DurationField(duration),
		common.OperandField(task))

	v2.TaskFlushTableTailDurationHistogram.Observe(duration.Seconds())

	sleep, name, exist := fault.TriggerFault("slow_flush")
	if exist && name == task.schema.Name {
		time.Sleep(time.Duration(sleep) * time.Second)
	}
	return
}

// prepareAObjSortedData read the data from appendable blocks, sort them if sort key exists
func (task *flushTableTailTask) prepareAObjSortedData(
	ctx context.Context, objIdx int, idxs []int, sortKeyPos int, isTombstone bool,
) (bat *containers.Batch, empty bool, err error) {
	if len(idxs) <= 0 {
		logutil.Infof("[FlushTabletail] no mergeable columns")
		return nil, true, nil
	}

	var obj handle.Object
	if isTombstone {
		obj = task.aTombstoneHandles[objIdx]
	} else {
		obj = task.aObjHandles[objIdx]
	}

	views, err := obj.GetColumnDataByIds(ctx, 0, idxs, common.MergeAllocator)
	if err != nil {
		return
	}
	bat = containers.NewBatch()
	rowCntBeforeApplyDelete := views.Columns[0].Length()
	deletes := views.DeleteMask
	views.ApplyDeletes()
	defer views.Close()
	for i, colidx := range idxs {
		colview := views.Columns[i]
		if colview == nil {
			empty = true
			return
		}
		vec := colview.Orphan()
		if vec.Length() == 0 {
			empty = true
			vec.Close()
			bat.Close()
			return
		}
		bat.AddVector(task.schema.ColDefs[colidx].Name, vec.TryConvertConst())
	}

	if isTombstone && deletes != nil {
		panic(fmt.Sprintf("logic err, tombstone %v has deletes", obj.GetID().String()))
	}

	if deletes != nil {
		task.aObjDeletesCnt += deletes.GetCardinality()
	}

	if isTombstone {
		return
	}

	var sortMapping []int32
	if sortKeyPos >= 0 {
		if objIdx == 0 {
			logutil.Infof("flushtabletail sort obj on %s", bat.Attrs[sortKeyPos])
		}
		sortMapping, err = mergesort.SortBlockColumns(bat.Vecs, sortKeyPos, task.rt.VectorPool.Transient)
		if err != nil {
			return
		}
	}
	mergesort.AddSortPhaseMapping(task.transMappings, objIdx, rowCntBeforeApplyDelete, deletes, sortMapping)
	return
}

// mergeAObjs merge the data from appendable blocks, and write the merged data to new block,
// recording row mapping in blkTransferBooking struct
func (task *flushTableTailTask) mergeAObjs(ctx context.Context, isTombstone bool) (err error) {
	var objMetas []*catalog.ObjectEntry
	var objHandles []handle.Object
	if isTombstone {
		objHandles = task.aTombstoneHandles
		objMetas = task.aTombstoneMetas
	} else {
		objHandles = task.aObjHandles
		objMetas = task.aObjMetas
	}

	if len(objMetas) == 0 {
		return nil
	}
	// prepare columns idx and sortKey to read sorted batch
	var schema *catalog.Schema
	if isTombstone {
		schema = task.tombstoneSchema
	} else {
		schema = task.schema
	}
	seqnums := make([]uint16, 0, len(schema.ColDefs))
	readColIdxs := make([]int, 0, len(schema.ColDefs))
	sortKeyIdx := -1
	sortKeyPos := -1
	if schema.HasSortKey() {
		sortKeyIdx = schema.GetSingleSortKeyIdx()
	}
	for i, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		readColIdxs = append(readColIdxs, def.Idx)
		if def.Idx == sortKeyIdx {
			sortKeyPos = i
		}
		seqnums = append(seqnums, def.SeqNum)
	}

	// read from aobjects
	readedBats := make([]*containers.Batch, 0, len(objHandles))
	for _, block := range objHandles {
		err = block.Prefetch(readColIdxs)
		if err != nil {
			return
		}
	}
	for i := range objHandles {
		bat, empty, err := task.prepareAObjSortedData(ctx, i, readColIdxs, sortKeyPos, isTombstone)
		if err != nil {
			return err
		}
		if empty {
			continue
		}
		readedBats = append(readedBats, bat)
	}

	for _, bat := range readedBats {
		defer bat.Close()
	}

	if len(readedBats) == 0 {
		// just soft delete all Objects
		for _, obj := range objHandles {
			tbl := obj.GetRelation()
			if err = tbl.SoftDeleteObject(obj.GetID(), isTombstone); err != nil {
				return err
			}
		}
		if !isTombstone {
			mergesort.CleanTransMapping(task.transMappings)
		}
		return nil
	}

	// create new object to hold merged blocks
	var toObjectEntry *catalog.ObjectEntry
	var toObjectHandle handle.Object
	if toObjectHandle, err = task.rel.CreateNonAppendableObject(false, isTombstone, nil); err != nil {
		return
	}
	toObjectEntry = toObjectHandle.GetMeta().(*catalog.ObjectEntry)
	toObjectEntry.SetSorted()

	// prepare merge
	// pick the sort key or first column to run first merge, determing the ordering
	sortVecs := make([]containers.Vector, 0, len(readedBats))
	// fromLayout describes the layout of the input batch, which is a list of batch length
	fromLayout := make([]uint32, 0, len(readedBats))
	// toLayout describes the layout of the output batch, i.e. [8192, 8192, 8192, 4242]
	toLayout := make([]uint32, 0, len(readedBats))
	totalRowCnt := 0
	if sortKeyPos < 0 {
		// no pk, just pick the first column to reshape
		sortKeyPos = 0
	}
	for _, bat := range readedBats {
		vec := bat.Vecs[sortKeyPos]
		fromLayout = append(fromLayout, uint32(vec.Length()))
		totalRowCnt += vec.Length()
		sortVecs = append(sortVecs, vec)
	}
	task.mergeRowsCnt = totalRowCnt
	rowsLeft := totalRowCnt
	for rowsLeft > 0 {
		if rowsLeft > int(schema.BlockMaxRows) {
			toLayout = append(toLayout, schema.BlockMaxRows)
			rowsLeft -= int(schema.BlockMaxRows)
		} else {
			toLayout = append(toLayout, uint32(rowsLeft))
			break
		}
	}

	// do first sort
	var orderedVecs []containers.Vector
	var sortedIdx []uint32
	if schema.HasSortKey() {
		// mergesort is needed, allocate sortedidx and mapping
		allocSz := totalRowCnt * 4
		// sortedIdx is used to shuffle other columns according to the order of the sort key
		sortIdxNode, err := common.MergeAllocator.Alloc(allocSz)
		if err != nil {
			panic(err)
		}
		// sortedidx will be used to shuffle other column, defer free
		defer common.MergeAllocator.Free(sortIdxNode)
		sortedIdx = unsafe.Slice((*uint32)(unsafe.Pointer(&sortIdxNode[0])), totalRowCnt)

		mappingNode, err := common.MergeAllocator.Alloc(allocSz)
		if err != nil {
			panic(err)
		}
		mapping := unsafe.Slice((*uint32)(unsafe.Pointer(&mappingNode[0])), totalRowCnt)

		// modify sortidx and mapping
		orderedVecs = mergesort.MergeColumn(sortVecs, sortedIdx, mapping, fromLayout, toLayout, task.rt.VectorPool.Transient)
		if !isTombstone {
			mergesort.UpdateMappingAfterMerge(task.transMappings, mapping, fromLayout, toLayout)
		}
		// free mapping, which is never used again
		common.MergeAllocator.Free(mappingNode)
	} else {
		// just do reshape
		orderedVecs = mergesort.ReshapeColumn(sortVecs, fromLayout, toLayout, task.rt.VectorPool.Transient)
		// UpdateMappingAfterMerge will handle the nil mapping
		if !isTombstone {
			mergesort.UpdateMappingAfterMerge(task.transMappings, nil, fromLayout, toLayout)
		}
	}
	for _, vec := range orderedVecs {
		defer vec.Close()
	}

	// make all columns ordered and prepared writtenBatches
	writtenBatches := make([]*containers.Batch, 0, len(orderedVecs))
	if isTombstone {
		task.createdTombstoneHandles = toObjectHandle
	} else {
		task.createdObjHandles = toObjectHandle
	}
	for i := 0; i < len(orderedVecs); i++ {
		writtenBatches = append(writtenBatches, containers.NewBatch())
	}
	vecs := make([]containers.Vector, 0, len(readedBats))
	for i, idx := range readColIdxs {
		// skip rowid and sort(reshape) column in the first run
		if schema.ColDefs[idx].IsPhyAddr() {
			continue
		}
		if i == sortKeyPos {
			for i, vec := range orderedVecs {
				writtenBatches[i].AddVector(schema.ColDefs[idx].Name, vec)
			}
			continue
		}
		vecs = vecs[:0]
		for _, bat := range readedBats {
			vecs = append(vecs, bat.Vecs[i])
		}
		var outvecs []containers.Vector
		if schema.HasSortKey() {
			outvecs = mergesort.ShuffleColumn(vecs, sortedIdx, fromLayout, toLayout, task.rt.VectorPool.Transient)
		} else {
			outvecs = mergesort.ReshapeColumn(vecs, fromLayout, toLayout, task.rt.VectorPool.Transient)
		}

		for i, vec := range outvecs {
			writtenBatches[i].AddVector(schema.ColDefs[idx].Name, vec)
			defer vec.Close()
		}
	}

	// write!
	name := objectio.BuildObjectNameWithObjectID(&toObjectEntry.ID)
	writer, err := blockio.NewBlockWriterNew(task.rt.Fs.Service, name, schema.Version, seqnums)
	if err != nil {
		return err
	}
	if schema.HasPK() {
		pkIdx := schema.GetSingleSortKeyIdx()
		writer.SetPrimaryKey(uint16(pkIdx))
	} else if schema.HasSortKey() {
		writer.SetSortKey(uint16(schema.GetSingleSortKeyIdx()))
	}
	if isTombstone {
		for _, bat := range writtenBatches {
			_, err = writer.WriteTombstoneBatch(containers.ToCNBatch(bat))
			if err != nil {
				return err
			}
		}
	} else {
		for _, bat := range writtenBatches {
			_, err = writer.WriteBatch(containers.ToCNBatch(bat))
			if err != nil {
				return err
			}
		}
	}
	_, _, err = writer.Sync(ctx)
	if err != nil {
		return err
	}
	if isTombstone {
		task.createdMergedTombstoneName = name.String()
	} else {
		task.createdMergedObjectName = name.String()
	}

	// update new status for created blocks
	err = toObjectHandle.UpdateStats(writer.Stats(isTombstone))
	if err != nil {
		return
	}
	err = toObjectHandle.GetMeta().(*catalog.ObjectEntry).GetObjectData().Init()
	if err != nil {
		return
	}

	// soft delete all aobjs
	for _, obj := range objHandles {
		tbl := obj.GetRelation()
		if err = tbl.SoftDeleteObject(obj.GetID(), isTombstone); err != nil {
			return err
		}
	}

	return nil
}

// flushAObjsForSnapshot schedule io task to flush aobjects for snapshot read. this function will not release any data in io task
func (task *flushTableTailTask) flushAObjsForSnapshot(ctx context.Context, isTombstone bool) (subtasks []*flushObjTask, err error) {
	defer func() {
		if err != nil {
			releaseFlushObjTasks(subtasks, err)
		}
	}()

	var metas []*catalog.ObjectEntry
	if isTombstone {
		metas = task.aTombstoneMetas
	} else {
		metas = task.aObjMetas
	}

	subtasks = make([]*flushObjTask, len(metas))
	// fire flush task
	for i, obj := range metas {
		var data *containers.Batch
		var dataVer *containers.BatchWithVersion
		objData := obj.GetObjectData()
		if dataVer, err = objData.CollectAppendInRange(
			types.TS{}, task.txn.GetStartTS(), true, common.MergeAllocator,
		); err != nil {
			return
		}
		data = dataVer.Batch
		if data == nil || data.Length() == 0 {
			// the new appendable block might has no data when we flush the table, just skip it
			// In previous impl, runner will only pass non-empty obj to NewCompactBlackTask
			continue
		}
		// do not close data, leave that to wait phase

		aobjectTask := NewFlushObjTask(
			tasks.WaitableCtx,
			dataVer.Version,
			dataVer.Seqnums,
			objData.GetFs(),
			obj,
			data,
			nil,
			true,
		)
		if err = task.rt.Scheduler.Schedule(aobjectTask); err != nil {
			return
		}
		subtasks[i] = aobjectTask
	}
	return
}

// waitFlushAObjForSnapshot waits all io tasks about flushing aobject for snapshot read, update locations
func (task *flushTableTailTask) waitFlushAObjForSnapshot(ctx context.Context, subtasks []*flushObjTask, isTombstone bool) (err error) {
	ictx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	var handles []handle.Object
	if isTombstone {
		handles = task.aTombstoneHandles
	} else {
		handles = task.aObjHandles
	}
	for i, subtask := range subtasks {
		if subtask == nil {
			continue
		}
		if err = subtask.WaitDone(ictx); err != nil {
			return
		}
		if err = handles[i].UpdateStats(subtask.stat); err != nil {
			return
		}
	}
	return nil
}

func releaseFlushObjTasks(subtasks []*flushObjTask, err error) {
	if err != nil {
		logutil.Infof("[FlushTabletail] release flush aobj bat because of err %v", err)
		// add a timeout to avoid WaitDone block the whole process
		ictx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second, /*6*time.Minute,*/
		)
		defer cancel()
		for _, subtask := range subtasks {
			if subtask != nil {
				// wait done, otherwise the data might be released before flush, and cause data race
				subtask.WaitDone(ictx)
			}
		}
	}
	for _, subtask := range subtasks {
		if subtask != nil && subtask.data != nil {
			subtask.data.Close()
		}
		if subtask != nil && subtask.delta != nil {
			subtask.delta.Close()
		}
	}
}

// For unit test
func (task *flushTableTailTask) GetCreatedObjects() handle.Object {
	return task.createdObjHandles
}
