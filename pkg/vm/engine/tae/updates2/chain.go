package updates2

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type ColumnChain struct {
	*common.Link
	*sync.RWMutex
	meta *catalog.BlockEntry
	id   *common.ID
	view *ColumnView
}

func NewColumnChain(rwlocker *sync.RWMutex, colIdx uint16, meta *catalog.BlockEntry) *ColumnChain {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	id := meta.AsCommonID()
	id.Idx = colIdx
	chain := &ColumnChain{
		Link:    new(common.Link),
		RWMutex: rwlocker,
		meta:    meta,
		id:      id,
	}
	chain.view = NewColumnView()
	return chain
}

func (chain *ColumnChain) GetMeta() *catalog.BlockEntry { return chain.meta }
func (chain *ColumnChain) GetBlockID() *common.ID       { id := chain.id.AsBlockID(); return &id }
func (chain *ColumnChain) GetID() *common.ID            { return chain.id }
func (chain *ColumnChain) GetColumnIdx() uint16         { return chain.id.Idx }

func (chain *ColumnChain) GetColumnName() string {
	return chain.meta.GetSchema().ColDefs[chain.id.Idx].Name
}

func (chain *ColumnChain) AddNodeLocked(txn txnif.AsyncTxn) txnif.UpdateNode {
	node := NewColumnNode(txn, chain.id, nil)
	node.AttachTo(chain)
	return node
}

func (chain *ColumnChain) DeleteNode(node *common.DLNode) {
	chain.Lock()
	defer chain.Unlock()
	chain.Delete(node)
}

func (chain *ColumnChain) DeleteNodeLocked(node *common.DLNode) {
	chain.Delete(node)
}

func (chain *ColumnChain) AddNode(txn txnif.AsyncTxn) txnif.UpdateNode {
	col := NewColumnNode(txn, chain.id, nil)
	chain.Lock()
	defer chain.Unlock()
	col.AttachTo(chain)
	return col
}

func (chain *ColumnChain) LoopChainLocked(fn func(col *ColumnNode) bool, reverse bool) {
	wrapped := func(node *common.DLNode) bool {
		col := node.GetPayload().(*ColumnNode)
		return fn(col)
	}
	chain.Loop(wrapped, reverse)
}

func (chain *ColumnChain) DepthLocked() int {
	depth := 0
	chain.LoopChainLocked(func(n *ColumnNode) bool {
		depth++
		return true
	}, false)
	return depth
}

func (chain *ColumnChain) UpdateLocked(node *ColumnNode) {
	chain.Update(node.DLNode)
}

func (chain *ColumnChain) StringLocked() string {
	return chain.view.StringLocked()
	// msg := fmt.Sprintf("Block-%s-Col[%d]-Chain:", chain.id.ToBlockFileName(), chain.id.Idx)
	// line := 1
	// chain.LoopChainLocked(func(n *ColumnNode) bool {
	// 	n.RLock()
	// 	msg = fmt.Sprintf("%s\n%d. %s", msg, line, n.StringLocked())
	// 	n.RUnlock()
	// 	line++
	// 	return true
	// }, false)
	// return msg
}

func (chain *ColumnChain) GetValueLocked(row uint32, ts uint64) (v interface{}, err error) {
	return chain.view.GetValue(row, ts)
}

func (chain *ColumnChain) CollectUpdatesLocked(ts uint64) (*roaring.Bitmap, map[uint32]interface{}) {
	return chain.view.CollectUpdates(ts)
}
