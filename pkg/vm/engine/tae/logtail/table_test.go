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

package logtail

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestOptimizeTable(t *testing.T) {
	txnCnt := 10
	blockSize := 4
	idAlloc := common.NewTxnIDAllocator()
	tsAlloc := types.GlobalTsAlloctor
	table := NewTxnTable(blockSize, tsAlloc.Alloc)

	// block-0: [tid-0] [tid-0: block-0] [tid-0: block-1] [tid-1]
	// block-1: [tid-0] [tid-0: block-2] [tid-1: block-3] [tid-2]
	// block-2: [tid-2: block-4] [tid-0: block-2] [tid-1] [tid-1: block-3]
	dbId := common.NextGlobalSeqNum()
	tblId0 := common.NextGlobalSeqNum()
	tblId1 := common.NextGlobalSeqNum()
	tblId2 := common.NextGlobalSeqNum()
	blk0 := objectio.RandomBlockid(0, 0)
	blk1 := objectio.RandomBlockid(0, 0)
	blk2 := objectio.RandomBlockid(0, 0)
	blk3 := objectio.RandomBlockid(0, 0)
	blk4 := objectio.RandomBlockid(0, 0)

	memos := make([]*txnif.TxnMemo, 0, txnCnt)
	for i := 0; i < txnCnt; i++ {
		memo := txnif.NewTxnMemo()
		memos = append(memos, memo)
		txn := new(txnbase.Txn)
		txn.TxnCtx = txnbase.NewTxnCtx(idAlloc.Alloc(), tsAlloc.Alloc(), types.TS{})
		txn.PrepareTS = tsAlloc.Alloc()
		txn.Memo = memo
		assert.NoError(t, table.AddTxn(txn))
	}

	for i := 0; i < txnCnt; i++ {
		memo := memos[i]
		switch i {
		case 0:
			memo.AddCatalogChange()
			memo.AddTable(dbId, tblId0)
			memos = append(memos, memo)
		case 1:
			memo.AddBlock(dbId, tblId0, blk0)
		case 2:
			memo.AddBlock(dbId, tblId0, blk1)
		case 3:
			memo.AddCatalogChange()
			memo.AddTable(dbId, tblId1)
		case 4:
			memo.AddCatalogChange()
			memo.AddTable(dbId, tblId0)
		case 5:
			memo.AddBlock(dbId, tblId0, blk2)
		case 6:
			memo.AddBlock(dbId, tblId1, blk3)
		case 7:
			memo.AddCatalogChange()
			memo.AddTable(dbId, tblId2)
		case 8:
			memo.AddBlock(dbId, tblId2, blk4)
		case 9:
			memo.AddBlock(dbId, tblId1, blk3)
		}
	}
}

func TestTxnTable1(t *testing.T) {
	txnCnt := 22
	blockSize := 10
	idAlloc := common.NewTxnIDAllocator()
	tsAlloc := types.GlobalTsAlloctor
	table := NewTxnTable(blockSize, tsAlloc.Alloc)
	for i := 0; i < txnCnt; i++ {
		txn := new(txnbase.Txn)
		txn.TxnCtx = txnbase.NewTxnCtx(idAlloc.Alloc(), tsAlloc.Alloc(), types.TS{})
		txn.PrepareTS = tsAlloc.Alloc()
		assert.NoError(t, table.AddTxn(txn))
	}
	t.Log(table.BlockCount())
	t.Log(table.String())
	timestamps := make([]types.TS, 0)
	fn1 := func(block *txnBlock) bool {
		timestamps = append(timestamps, block.bornTS)
		return true
	}
	table.Scan(fn1)
	assert.Equal(t, 3, len(timestamps))

	cnt := 0

	op := func(row RowT) (goNext bool) {
		cnt++
		return true
	}

	table.ForeachRowInBetween(
		timestamps[0].Prev(),
		types.MaxTs(),
		nil,
		op,
		nil,
	)
	assert.Equal(t, txnCnt, cnt)

	cnt = 0
	table.ForeachRowInBetween(
		timestamps[1],
		types.MaxTs(),
		nil,
		op,
		nil,
	)
	assert.Equal(t, txnCnt-blockSize, cnt)

	cnt = 0
	table.ForeachRowInBetween(
		timestamps[2],
		types.MaxTs(),
		nil,
		op,
		nil,
	)
	assert.Equal(t, txnCnt-2*blockSize, cnt)

	ckp := timestamps[0].Prev()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	// these two are in first block, do not delete
	ckp = timestamps[0].Next()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	ckp = timestamps[1].Prev()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 0, cnt)

	// do not delete if truncate all
	assert.Equal(t, 0, table.TruncateByTimeStamp(types.MaxTs()))
	assert.Equal(t, 0, table.TruncateByTimeStamp(types.MaxTs()))

	ckp = timestamps[1].Next()
	cnt = table.TruncateByTimeStamp(ckp)
	assert.Equal(t, 1, cnt)

	ckp = timestamps[2]
	cnt = table.TruncateByTimeStamp(ckp)
	// 2 blocks left and skip deleting only one block
	assert.Equal(t, 0, cnt)
}
