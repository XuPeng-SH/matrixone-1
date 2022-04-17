package tables

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/accessif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/impl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/updates2"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type dataBlock struct {
	*sync.RWMutex
	meta                 *catalog.BlockEntry
	node                 *appendableNode
	file                 dataio.BlockFile
	bufMgr               base.INodeManager
	colUpdates           map[uint16]*updates2.ColumnChain
	updatableIndexHolder accessif.IAppendableBlockIndexHolder
}

func newBlock(meta *catalog.BlockEntry, segFile dataio.SegmentFile, bufMgr base.INodeManager) *dataBlock {
	file := segFile.GetBlockFile(meta.GetID())
	var node *appendableNode
	var holder accessif.IAppendableBlockIndexHolder
	if meta.IsAppendable() {
		node = newNode(bufMgr, meta, file)
		schema := meta.GetSegment().GetTable().GetSchema()
		pkIdx := schema.PrimaryKey
		pkType := schema.Types()[pkIdx]
		holder = impl.NewAppendableBlockIndexHolder(pkType)
	}
	return &dataBlock{
		RWMutex:              new(sync.RWMutex),
		meta:                 meta,
		file:                 file,
		node:                 node,
		updatableIndexHolder: holder,
	}
}

func (blk *dataBlock) IsAppendable() bool {
	if !blk.meta.IsAppendable() {
		return false
	}
	if blk.node.Rows(nil, true) == blk.meta.GetSegment().GetTable().GetSchema().BlockMaxRows {
		return false
	}
	return true
}

func (blk *dataBlock) Rows(txn txnif.AsyncTxn, coarse bool) int {
	if blk.meta.IsAppendable() {
		rows := int(blk.node.Rows(txn, coarse))
		return rows
	}
	return int(blk.file.Rows())
}

func (blk *dataBlock) PPString(level common.PPLevel, depth int, prefix string) string {
	blk.RLock()
	defer blk.RUnlock()
	s := fmt.Sprintf("%s | [Rows=%d]", blk.meta.PPString(level, depth, prefix), blk.Rows(nil, true))
	if blk.colUpdates != nil {
		for _, col := range blk.colUpdates {
			s = fmt.Sprintf("%s\n%s", s, col.StringLocked())
		}
	}
	return s
}

func (blk *dataBlock) MakeAppender() (appender data.BlockAppender, err error) {
	if !blk.IsAppendable() {
		err = data.ErrNotAppendable
		return
	}
	appender = newAppender(blk.node, blk.updatableIndexHolder)
	return
}

func (blk *dataBlock) GetVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, err error) {
	return blk.getVectorCopy(txn, attr, compressed, decompressed, false)
}

func (blk *dataBlock) getVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer, raw bool) (vec *gvec.Vector, err error) {
	h := blk.node.mgr.Pin(blk.node)
	if h == nil {
		panic("not expected")
	}
	defer h.Close()
	blk.RLock()
	vec, err = blk.node.GetVectorCopy(txn, attr, compressed, decompressed)
	if err != nil || raw {
		blk.RUnlock()
		return
	}
	if blk.colUpdates != nil {
		colIdx := blk.meta.GetSegment().GetTable().GetSchema().GetColIdx(attr)
		col := blk.colUpdates[uint16(colIdx)]
		blk.RUnlock()
		if col == nil {
			return
		}
		col.RLock()
		mask, vals := col.CollectUpdatesLocked(txn.GetStartTS())
		col.RUnlock()
		vec = compute.ApplyUpdateToVector(vec, mask, vals)
		return
	}
	blk.RUnlock()
	return
}

func (blk *dataBlock) Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (node txnif.UpdateNode, err error) {
	blk.Lock()
	if blk.colUpdates == nil {
		blk.colUpdates = make(map[uint16]*updates2.ColumnChain)
	}
	col := blk.colUpdates[colIdx]
	if col == nil {
		col = updates2.NewColumnChain(nil, colIdx, blk.meta)
		blk.colUpdates[colIdx] = col
	}
	blk.Unlock()
	col.Lock()
	node = col.AddNodeLocked(txn)
	err = node.UpdateLocked(row, v)
	if err != nil {
		col.DeleteNodeLocked(node.GetDLNode())
	}
	col.Unlock()
	return
}

func (blk *dataBlock) RangeDelete(txn txnif.AsyncTxn, start, end uint32) (node txnif.UpdateNode, err error) {
	return
	// blk.Lock()
	// defer blk.Unlock()
	// // First update
	// if blk.chain == nil {
	// 	blk.chain = updates.NewUpdateChain(blk.RWMutex, blk.meta)
	// 	node = blk.chain.AddNodeLocked(txn)
	// 	node.ApplyDeleteRowsLocked(start, end)
	// 	return
	// }
	// err = blk.chain.CheckDeletedLocked(start, end, txn)
	// if err != nil {
	// 	return
	// }

	// for col := range blk.meta.GetSegment().GetTable().GetSchema().ColDefs {
	// 	for row := start; row <= end; row++ {
	// 		if err = blk.chain.CheckColumnUpdatedLocked(row, uint16(col), txn); err != nil {
	// 			return
	// 		}
	// 	}
	// }
	// node = blk.chain.AddNodeLocked(txn)
	// node.ApplyDeleteRowsLocked(start, end)
	// return
}

// func (blk *dataBlock) GetUpdateChain() txnif.UpdateChain {
// 	blk.RLock()
// 	defer blk.RUnlock()
// 	return blk.GetUpdateChain()
// }

func (blk *dataBlock) GetValue(txn txnif.AsyncTxn, row uint32, col uint16) (v interface{}, err error) {
	blk.RLock()
	defer blk.RUnlock()
	if blk.colUpdates != nil {
		chain := blk.colUpdates[col]
		if chain != nil {
			chain.RLock()
			v, err = chain.GetValueLocked(row, txn.GetStartTS())
			chain.RUnlock()
		}
	}
	if v != nil && err == nil {
		return
	}
	var comp bytes.Buffer
	var decomp bytes.Buffer
	attr := blk.meta.GetSegment().GetTable().GetSchema().ColDefs[col].Name
	raw, _ := blk.getVectorCopy(txn, attr, &comp, &decomp, true)
	v = compute.GetValue(raw, row)
	return
}

func (blk *dataBlock) GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	blk.RLock()
	defer blk.RUnlock()
	return blk.updatableIndexHolder.Search(filter.Val)
}

func (blk *dataBlock) BatchDedup(txn txnif.AsyncTxn, pks *gvec.Vector) (err error) {
	if blk.updatableIndexHolder == nil {
		panic("unexpected error")
	}
	// logutil.Infof("BatchDedup %s: PK=%s", txn.String(), pks.String())
	return blk.updatableIndexHolder.BatchDedup(pks)
}
