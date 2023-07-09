package meta

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/pkg/errors"
	"github.com/tidwall/btree"
)

func NewDeleteChain(id uint64) *DeleteChain {
	return &DeleteChain{
		id:    id,
		nodes: btree.NewBTreeG((*DeleteNode).Less),
		index: make(map[uint32]*DeleteNode),
	}
}

func (chain *DeleteChain) AddDeletesLocked(
	txn txnif.TxnReader,
	rows ...uint32,
) (n *DeleteNode) {
	n = NewDeleteNode(txn, chain)
	n.Add(rows...)
	chain.AddNodeLocked(n)
	chain.AddIntoIndexLocked(n, rows...)
	return
}

func (chain *DeleteChain) AddNodeLocked(
	n *DeleteNode,
) {
	_, replaced := chain.nodes.Set(n)
	if replaced {
		panic("unreachable")
	}
}

func (chain *DeleteChain) AddIntoIndexLocked(
	n *DeleteNode,
	rows ...uint32,
) {
	for _, row := range rows {
		chain.index[row] = n
	}
}

func (chain *DeleteChain) DeleteNodeLocked(
	n *DeleteNode,
) {
	_, ok := chain.nodes.Delete(n)
	if !ok {
		panic("unreachable")
	}
}

func (chain *DeleteChain) DeleteFromIndexLocked(
	n *DeleteNode,
) {
	// delete from index
	it := n.mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		delete(chain.index, row)
	}
}

func (chain *DeleteChain) PrepareDelete(
	ts types.TS,
	rows ...uint32,
) error {
	chain.RLock()
	defer chain.RUnlock()
	return chain.PrepareDeleteLocked(ts, rows...)
}

func (chain *DeleteChain) PrepareDeleteLocked(
	ts types.TS,
	rows ...uint32,
) error {
	if len(rows) == 0 {
		return nil
	}
	for _, row := range rows {
		if _, ok := chain.index[row]; ok {
			return errors.Errorf("row %d is already deleted", row)
		}
	}
	return nil
}

func (chain *DeleteChain) Truncate(
	ts types.TS,
) {
	chain.Lock()
	defer chain.Unlock()
	chain.TruncateLocked(ts)
}

func (chain *DeleteChain) TruncateLocked(
	ts types.TS,
) {
	snap := chain.nodes.Copy()
	it := snap.Iter()
	defer it.Release()
	if it.First() {
		for {
			n := it.Item()
			if n.End.LessEq(ts) {
				chain.nodes.Delete(n)
			} else {
				break
			}
			if !it.Next() {
				break
			}
		}
	}
}

func (chain *DeleteChain) String() string {
	chain.RLock()
	defer chain.RUnlock()
	return chain.StringLocked()
}

func (chain *DeleteChain) StringLocked() string {
	var w bytes.Buffer
	nodes := chain.nodes.Copy()
	line := 1
	w.WriteString(fmt.Sprintf("chain %d\n", chain.id))
	nodes.Scan(func(n *DeleteNode) bool {
		w.WriteString(fmt.Sprintf("%d: %s\n", line, n.String()))
		line++
		return true
	})
	return w.String()
}

// DeleteLocation

// 1. Load object meta
// 2. Check with the rowid column zonemap and bloomfilter
// 3. Load deletes if the above checks are positive
// 4. Check with the deletes
func (dloc *DeleteLocation) PrepareDelete(_ types.TS, rows ...uint32) error {
	// objmeta, err := objectio.FastLoadObjectMeta(
	// 	context.Background(),
	// 	&dloc.location,
	// )
	return nil
}
