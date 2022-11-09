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

package txnentries

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type compactBlockEntry struct {
	sync.RWMutex
	txn       txnif.AsyncTxn
	from      handle.Block
	to        handle.Block
	scheduler tasks.TaskScheduler
	mapping   containers.Vector
	deletes   *roaring.Bitmap
}

func NewCompactBlockEntry(
	txn txnif.AsyncTxn,
	from, to handle.Block,
	scheduler tasks.TaskScheduler,
	sortIdx containers.Vector,
	length int,
	deletes *roaring.Bitmap) *compactBlockEntry {
	mapping := containers.MakeVector(types.T_Rowid.ToType(), false)
	helper := make([]int32, length)
	for i := 0; i < length; i++ {
		helper[i] = -1
	}
	for i := 0; i < sortIdx.Length(); i++ {
		idx := sortIdx.Get(i).(uint32)
		helper[idx] = int32(i)
	}
	for _, idx := range helper {
		var rowID types.Rowid
		if idx != -1 {
			rowID = model.EncodePhyAddrKey(from.GetSegment().GetID(), from.ID(), uint32(idx))
		}
		mapping.Append(rowID)
	}
	return &compactBlockEntry{
		txn:       txn,
		from:      from,
		to:        to,
		scheduler: scheduler,
		mapping:   mapping,
		deletes:   deletes,
	}
}

func (entry *compactBlockEntry) PrepareRollback() (err error) {
	// TODO: remove block file? (should be scheduled and executed async)
	return
}
func (entry *compactBlockEntry) ApplyRollback(index *wal.Index) (err error) {
	//TODO:?
	return
}
func (entry *compactBlockEntry) ApplyCommit(index *wal.Index) (err error) {
	entry.from.GetMeta().(*catalog.BlockEntry).GetBlockData().FreeData()
	if err = entry.from.GetMeta().(*catalog.BlockEntry).GetBlockData().ReplayImmutIndex(); err != nil {
		return
	}
	return
}

func (entry *compactBlockEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	cmd = newCompactBlockCmd((*common.ID)(entry.from.Fingerprint()), (*common.ID)(entry.to.Fingerprint()), entry.txn, csn)
	return
}

func (entry *compactBlockEntry) Set1PC()     {}
func (entry *compactBlockEntry) Is1PC() bool { return false }
func (entry *compactBlockEntry) PrepareCommit() (err error) {
	dataBlock := entry.from.GetMeta().(*catalog.BlockEntry).GetBlockData()
	if dataBlock.HasDeleteIntentsPreparedIn(entry.txn.GetStartTS(), types.MaxTs()) {
		err = moerr.NewTxnWWConflict()
	}
	return
}
