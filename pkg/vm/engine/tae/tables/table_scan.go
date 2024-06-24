// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func HybridScanByBlock(
	ctx context.Context,
	tableEntry *catalog.TableEntry,
	txn txnif.TxnReader,
	bat **containers.Batch,
	readSchema *catalog.Schema,
	colIdxs []int,
	blkID *objectio.Blockid,
	mp *mpool.MPool,
) error {
	dataObject, err := tableEntry.GetObjectByID(blkID.Object(), false)
	if err != nil {
		return err
	}
	_, offset := blkID.Offsets()
	err = dataObject.GetObjectData().Scan(ctx, bat, txn, readSchema, offset, colIdxs, mp)
	if err != nil {
		return err
	}
	if *bat == nil {
		return nil
	}
	it := tableEntry.MakeObjectIt(false, true)
	for ; it.Valid(); it.Next() {
		tombstone := it.Get().GetPayload()
		err := tombstone.GetObjectData().FillBlockTombstones(ctx, txn, blkID, &(*bat).Deletes, mp)
		if err != nil {
			return err
		}
	}
	id := dataObject.AsCommonID()
	id.BlockID = *blkID
	err = txn.GetStore().FillInWorkspaceDeletes(id, &(*bat).Deletes)
	return err
}

func TombstoneRangeScanByObject(
	ctx context.Context,
	tableEntry *catalog.TableEntry,
	objectID objectio.ObjectId,
	start, end types.TS,
	mp *mpool.MPool,
	vpool *containers.VectorPool,
) (bat *containers.Batch, err error) {
	it := tableEntry.MakeTombstoneObjectIt(false)
	for ; it.Valid(); it.Next() {
		node := it.Get()
		tombstone := node.GetPayload()
		tombstone.RLock()
		skip := tombstone.IsCreatingOrAbortedLocked() || tombstone.HasDropCommittedLocked()
		tombstone.RUnlock()
		if skip {
			continue
		}
		visible := tombstone.IsVisibleInRange(start, end)
		if !visible {
			continue
		}
		if tombstone.HasCommittedPersistedData() {
			zm := tombstone.GetSortKeyZonemap()
			if !zm.PrefixEq(objectID[:]) {
				continue
			}
			// TODO: Bloomfilter
		}
		err = tombstone.GetObjectData().CollectObjectTombstoneInRange(ctx, start, end, &objectID, &bat, mp, vpool)
		if err != nil {
			return nil, err
		}
	}
	return
}
