package meta

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (chain *AppendChain) AddNodeLocked(
	txn txnif.AsyncTxn,
	startRow, endRow uint32,
) (node *AppendNode, created bool) {
	return
}

func (chain *AppendChain) DeleteNodeLocked(
	node *AppendNode,
) (err error) {
	chain.nodes.DeleteNode(node)
}

// [from, to]
func (chain *AppendChain) CollectAppendLocked(
	from, to types.TS,
) (
	minRow, maxRow uint32,
	commitTSVec, abortVec container.Vector,
	aborts *nulls.Bitmap,
) {
	return
}

func (chain *AppendChain) GetNodeByRowLocked(row uint32) (n *AppendNode) {
	_, n = chain.nodes.SearchNodeByCompareFn(
		func(n *AppendNode) int {
			if node.endRow <= row {
				return -1
			}
			if node.startRow > row {
				return 1
			}
			return 0
		},
	)
	return
}
