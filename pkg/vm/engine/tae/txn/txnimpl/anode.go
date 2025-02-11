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

package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

// anode corresponds to an appendable standalone-uncommitted block
// which belongs to txn's workspace and can be appended data into.
type anode struct {
	*baseNode
	data    *containers.Batch
	rows    uint32
	appends []*appendInfo
}

// NewANode creates a InsertNode with data in memory.
func NewANode(
	tbl *txnTable,
	meta *catalog.BlockEntry,
) *anode {
	impl := new(anode)
	impl.baseNode = newBaseNode(tbl, meta)
	impl.appends = make([]*appendInfo, 0)
	return impl
}

func (n *anode) Rows() uint32 {
	return n.rows
}

func (n *anode) GetAppends() []*appendInfo {
	return n.appends
}
func (n *anode) AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dest *common.ID) *appendInfo {
	seq := len(n.appends)
	info := &appendInfo{
		dest:    *dest,
		destOff: destOff,
		destLen: destLen,
		srcOff:  srcOff,
		srcLen:  srcLen,
		seq:     uint32(seq),
	}
	n.appends = append(n.appends, info)
	return info
}

func (n *anode) IsPersisted() bool {
	return false
}

func (n *anode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	if n.data == nil {
		return
	}
	composedCmd := NewAppendCmd(id, n, n.data)
	return composedCmd, nil
}

func (n *anode) Close() (err error) {
	if n.data != nil {
		n.data.Close()
		n.data = nil
	}
	return
}

func (n *anode) PrepareAppend(data *containers.Batch, offset uint32) uint32 {
	left := uint32(data.Length()) - offset
	nodeLeft := MaxNodeRows - n.rows
	if left <= nodeLeft {
		return left
	}
	return nodeLeft
}

func (n *anode) Append(data *containers.Batch, offset uint32) (an uint32, err error) {
	schema := n.table.GetLocalSchema()
	if n.data == nil {
		opts := containers.Options{}
		opts.Capacity = data.Length() - int(offset)
		if opts.Capacity > int(MaxNodeRows) {
			opts.Capacity = int(MaxNodeRows)
		}
		n.data = containers.BuildBatch(schema.AllNames(), schema.AllTypes(), opts)
	}

	from := uint32(n.data.Length())
	an = n.PrepareAppend(data, offset)
	for _, attr := range data.Attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := n.data.Vecs[def.Idx]
		// logutil.Infof("destVec: %s, %d, %d", destVec.String(), cnt, data.Length())
		destVec.ExtendWithOffset(data.Vecs[def.Idx], int(offset), int(an))
	}
	n.rows = uint32(n.data.Length())
	err = n.FillPhyAddrColumn(from, an)
	return
}

func (n *anode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	var col *vector.Vector
	if col, err = objectio.ConstructRowidColumn(
		&n.meta.ID, startRow, length, common.DefaultAllocator,
	); err != nil {
		return
	}
	err = n.data.Vecs[n.table.GetLocalSchema().PhyAddrKey.Idx].ExtendVec(col)
	col.Free(common.DefaultAllocator)
	return
}

func (n *anode) FillBlockView(view *model.BlockView, colIdxes []int) (err error) {
	for _, colIdx := range colIdxes {
		orig := n.data.Vecs[colIdx]
		view.SetData(colIdx, orig.CloneWindow(0, orig.Length()))
	}
	view.DeleteMask = n.data.Deletes
	return
}
func (n *anode) FillColumnView(view *model.ColumnView) (err error) {
	orig := n.data.Vecs[view.ColIdx]
	view.SetData(orig.CloneWindow(0, orig.Length()))
	view.DeleteMask = n.data.Deletes
	return
}

func (n *anode) GetSpace() uint32 {
	return MaxNodeRows - n.rows
}

func (n *anode) RowsWithoutDeletes() uint32 {
	deletes := uint32(0)
	if n.data != nil && n.data.Deletes != nil {
		deletes = uint32(n.data.DeleteCnt())
	}
	return uint32(n.data.Length()) - deletes
}

func (n *anode) LengthWithDeletes(appended, toAppend uint32) uint32 {
	if !n.data.HasDelete() {
		return toAppend
	}
	appendedOffset := n.OffsetWithDeletes(appended)
	toAppendOffset := n.OffsetWithDeletes(toAppend + appended)
	// logutil.Infof("appened:%d, toAppend:%d, off1=%d, off2=%d", appended, toAppend, appendedOffset, toAppendOffset)
	return toAppendOffset - appendedOffset
}

func (n *anode) OffsetWithDeletes(count uint32) uint32 {
	if !n.data.HasDelete() {
		return count
	}
	offset := count
	for offset < n.rows {
		deletes := n.data.Deletes.Rank(offset)
		if offset == count+uint32(deletes) {
			break
		}
		offset = count + uint32(deletes)
	}
	return offset
}

func (n *anode) GetValue(col int, row uint32) (any, bool, error) {
	vec := n.data.Vecs[col]
	return vec.Get(int(row)), vec.IsNull(int(row)), nil
}

func (n *anode) RangeDelete(start, end uint32) error {
	n.data.RangeDelete(int(start), int(end+1))
	return nil
}

func (n *anode) IsRowDeleted(row uint32) bool {
	return n.data.IsDeleted(int(row))
}

func (n *anode) PrintDeletes() string {
	if !n.data.HasDelete() {
		return "NoDeletes"
	}
	return n.data.Deletes.String()
}

func (n *anode) WindowColumn(start, end uint32, pos int) (vec containers.Vector, err error) {
	data := n.data
	deletes := data.WindowDeletes(int(start), int(end-start))
	if deletes != nil {
		vec = data.Vecs[pos].CloneWindow(int(start), int(end-start))
		vec.Compact(deletes)
	} else {
		vec = data.Vecs[pos].Window(int(start), int(end-start))
	}
	return
}

func (n *anode) Window(start, end uint32) (bat *containers.Batch, err error) {
	data := n.data
	if data.HasDelete() {
		bat = data.CloneWindow(int(start), int(end-start))
		bat.Compact()
	} else {
		bat = data.Window(int(start), int(end-start))
	}
	return
}

func (n *anode) GetColumnDataByIds(
	colIdxes []int,
) (view *model.BlockView, err error) {
	view = model.NewBlockView()
	err = n.FillBlockView(view, colIdxes)
	return
}

func (n *anode) GetColumnDataById(colIdx int) (view *model.ColumnView, err error) {
	view = model.NewColumnView(colIdx)
	err = n.FillColumnView(view)
	return
}

func (n *anode) Prefetch(idxes []uint16) error {
	return nil
}
