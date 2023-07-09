package meta

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/tidwall/btree"
)

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

type Mutation struct {
	mu        *sync.RWMutex
	from      types.TS
	chains    *btree.BTreeG[*DeleteChain]
	locations *btree.BTreeG[*DeleteLocation]
}

type MutationSnapshot struct {
	from      types.TS
	chains    *btree.BTreeG[*DeleteChain]
	locations *btree.BTreeG[*DeleteLocation]
}
