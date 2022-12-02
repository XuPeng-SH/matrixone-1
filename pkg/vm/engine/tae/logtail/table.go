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
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type RowT = *txnRow
type BlockT = *txnBlock

type txnRow struct {
	txnif.AsyncTxn
}

func (row *txnRow) Length() int             { return 1 }
func (row *txnRow) Window(_, _ int) *txnRow { return nil }

type txnBlock struct {
	sync.RWMutex
	bornTS types.TS
	rows   []*txnRow
}

func (blk *txnBlock) Length() int {
	blk.RLock()
	defer blk.RUnlock()
	return len(blk.rows)
}

func (blk *txnBlock) IsAppendable() bool {
	return blk != nil
}

func (blk *txnBlock) Append(row *txnRow) (err error) {
	blk.Lock()
	defer blk.Unlock()
	blk.rows = append(blk.rows, row)
	return
}

func (blk *txnBlock) Close() {
	blk.Lock()
	defer blk.Unlock()
	blk.bornTS = types.TS{}
	blk.rows = make([]*txnRow, 0)
}

func (blk *txnBlock) ForeachRowInBetween(
	from, to types.TS,
	op func(row RowT) (goNext bool),
) (outOfRange bool) {
	blk.RLock()
	rows := blk.rows[:len(blk.rows)]
	blk.RUnlock()
	for _, row := range rows {
		ts := row.GetPrepareTS()
		if ts.Greater(to) {
			outOfRange = true
			return
		}
		if ts.Less(from) {
			continue
		}

		if !op(row) {
			outOfRange = true
			return
		}
	}
	return
}

func (blk *txnBlock) String() string {
	length := blk.Length()
	var buf bytes.Buffer
	_, _ = buf.WriteString(
		fmt.Sprintf("TXNBLK-[%s][Len=%d]", blk.bornTS.ToString(), length))
	return buf.String()
}

type TxnTable struct {
	*model.AOT[BlockT, RowT]
}

func blockCompareFn(a, b BlockT) bool {
	return a.bornTS.Less(b.bornTS)
}

func timeBasedTruncateFactory(ts types.TS) func(b BlockT) bool {
	return func(b BlockT) bool {
		return b.bornTS.GreaterEq(ts)
	}
}

func NewTxnTable(blockSize int, clock *types.TsAlloctor) *TxnTable {
	factory := func(row RowT) BlockT {
		ts := row.GetPrepareTS()
		if ts == txnif.UncommitTS {
			ts = clock.Alloc()
		}
		return &txnBlock{
			bornTS: ts,
			rows:   make([]*txnRow, 0, blockSize),
		}
	}
	return &TxnTable{
		AOT: model.NewAOT(
			blockSize,
			factory,
			blockCompareFn,
		),
	}
}

func (table *TxnTable) AddTxn(txn txnif.AsyncTxn) (err error) {
	row := &txnRow{
		AsyncTxn: txn,
	}
	err = table.Append(row)
	return
}

func (table *TxnTable) TruncateByTimeStamp(ts types.TS) (cnt int) {
	filter := timeBasedTruncateFactory(ts)
	return table.Truncate(filter)
}

// Invariants:
// 1. txn is sorted by prepareTS if it has one
// 2. bornTS of every block is sorted
// 3. for every block, bornTS <= txn.prepareTS for txn in block
// 4. empty prepareTS = maxTS

// legend: (bornTS) [prepareTS, prepareTS, prepareTS] and
// normal case:
// 	(0)[2,3,4]  (7)[8,10,11] (12)[14,16,19]
//  from = 13,
// rare case:
// 	(0)[2,3,4]  (7)[8,10,15] (12)[17,18,19]
// super super rare case:
// 	(0)[2,3,4]  (7)[8,10,None] (12)[None,None,None]
// 	(0)[20,21,22]  (1)[28,30,31] (2)[34,36,39]

func (table *TxnTable) ForeachRowInBetween(
	from, to types.TS,
	op func(row RowT) (goNext bool),
) {
	snapshot := table.Snapshot()
	pivot := &txnBlock{bornTS: from}
	first_run := true
	snapshot.Descend(pivot, func(blk BlockT) bool {
		if first_run {
			// found first bornTS where bornTS <= from
			pivot.bornTS = blk.bornTS
			first_run = false
			// previous block has to be checked
			return true
		} else {
			blk.RLock()
			// there is at least one row in a block, so no boundry check here
			lastTs := blk.rows[len(blk.rows)-1].GetPrepareTS()
			blk.RUnlock()
			// rare cases
			if lastTs.GreaterEq(from) {
				pivot.bornTS = blk.bornTS
				return true
			}
			// the last txn is absolutely not in range [from..], stop backtracing
			return false
		}
	})
	snapshot.Ascend(pivot, func(blk BlockT) bool {
		if blk.bornTS.Greater(to) {
			return false
		}
		outOfRange := blk.ForeachRowInBetween(
			from,
			to,
			op,
		)

		return !outOfRange
	})
}
