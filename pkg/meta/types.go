package meta

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/tidwall/btree"
)

// For each table, it maitains table table as object list
// 1. There are two types of objects: In-memory object and on-disk object
//    On-Disk:     [Object1, Object2, Object3, Object4, Object5]
//    In-memory:   [Object6, Object7, Object8, Object9, Object10]
// 2. In-memory object will finally be flushed to be on-disk object
//    On-Disk:     [Object1, Object2, Object3, Object4, Object5, Object6]
//    In-memory:   [Object7, Object8, Object9, Object10]
// 3. There is only one active in-memory object for each table. All in-active
//    in-memory objects should be flushed to be on-disk objects
//    In-memory:   [Object7, Object8, Object9, Object10]
//                                                |
//												  +-----> Active In-memory Object

type AppendNode struct {
	txnbase.TxnMVCCNode
	chain    *AppendChain
	startRow uint32
	endRow   uint32
}

type DeleteNode struct {
	txnbase.TxnMVCCNode
	mask  *roaring.Bitmap
	chain *DeleteChain
}

type DeleteLocation struct {
	txnbase.TxnMVCCNode
	id       uint64
	location objectio.Location
}

type DeleteChain struct {
	sync.RWMutex
	id    uint64
	nodes *btree.BTreeG[*DeleteNode]
	index map[uint32]*DeleteNode
}

type AppendChain struct {
	sync.RWMutex
	id    uint64
	nodes *txnbase.MVCCSlice[*AppendNode]
	data  *container.Batch
}

type Mutation struct {
	sync.RWMutex
	from      types.TS
	chains    *btree.BTreeG[*DeleteChain]
	locations *btree.BTreeG[*DeleteLocation]
}

type MutationSnapshot struct {
	from      types.TS
	chains    *btree.BTreeG[*DeleteChain]
	locations *btree.BTreeG[*DeleteLocation]
}
