package state

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/tidwall/btree"
	"go.uber.org/atomic"
)

type Checkpoint struct {
	isIncremental bool
	start, end    types.TS
}

type Checkpoints struct {
	points     []Checkpoint
	start, end types.TS
}

type VisibilityNode struct {
	CreateTS, DeleteTS types.TS
}

type TxnNode struct {
	start, end types.TS
	txn        txnif.TxnReader
	aborted    bool
}

type BlockEntryNode struct {
	*BlockEntry
	TxnNode
	VisibilityNode

	// may be mutated
	location  objectio.Location
	deltaLocs []objectio.Location
}

type BlockEntry struct {
	sync.RWMutex

	// immutable
	id     objectio.Blockid
	state  uint8
	frozen atomic.Bool

	// cache index
	pkZM atomic.Pointer[index.ZM]
}

type ObjectEntry struct {
	VisibilityNode
	// persisted object meta location
	// for non-persisted object, location only contains the object name
	location objectio.Location

	// mutable will not be changed after creation
	// true:  object may be mutated, need to check if it is frozen
	// false: object will not be mutated, no need to check if it is frozen
	mutable bool
	// only valid when mutable is true
	// true: the object is frozen and will not be mutated
	// false: the object is not frozen and may be mutated
	frozen bool

	blocks struct {
		mu      sync.RWMutex
		entries []*BlockEntry
	}
}

type Memtable struct {
	objects     *btree.BTreeG[ObjectEntry]
	dirtyBlocks *btree.BTreeG[BlockEntry]
}

type TableState struct {
	id          uint64
	checkpoints Checkpoints
	memtable    Memtable
}
