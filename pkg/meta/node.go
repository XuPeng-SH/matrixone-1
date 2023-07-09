package meta

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func NewDeleteNode(txn txnif.TxnReader, chain *DeleteChain) *DeleteNode {
	n := &DeleteNode{
		mask:  roaring.New(),
		chain: chain,
	}
	n.Txn = txn
	n.Start = txn.GetStartTS()
	n.Prepare = txnif.UncommitTS
	n.End = txnif.UncommitTS
	return n
}

func (n *DeleteNode) PrepareRollback() (err error) {
	n.chain.Lock()
	defer n.chain.Unlock()
	n.chain.DeleteNodeLocked(n)
	n.chain.DeleteFromIndexLocked(n)
	n.TxnMVCCNode.PrepareRollback()
	return
}

func (n *DeleteNode) PrepareCommit() (err error) {
	n.chain.Lock()
	defer n.chain.Unlock()
	n.chain.DeleteNodeLocked(n)
	if _, err = n.TxnMVCCNode.PrepareCommit(); err != nil {
		return
	}
	n.chain.AddNodeLocked(n)
	return
}

func (n *DeleteNode) ApplyCommit() (err error) {
	n.chain.Lock()
	defer n.chain.Unlock()
	_, err = n.TxnMVCCNode.ApplyCommit()
	return
}

func (n *DeleteNode) Add(rows ...uint32) {
	for _, row := range rows {
		n.mask.Add(row)
	}
}

func (n *DeleteNode) String() string {
	commitState := "C"
	if n.GetEnd() == txnif.UncommitTS {
		commitState = "UC"
	}
	s := fmt.Sprintf(
		"[%s][%d:%s]%s",
		commitState,
		n.mask.GetCardinality(),
		n.mask.String(),
		n.TxnMVCCNode.String(),
	)
	return s
}

func (n *DeleteNode) Less(o *DeleteNode) bool {
	// logutil.Infof("%s vs %v", n.Prepare.ToString(), o.Prepare.ToString())
	v := n.Prepare.Compare(o.Prepare)
	if v < 0 {
		return true
	} else if v > 0 {
		return false
	}
	v = n.Start.Compare(o.Start)
	if v < 0 {
		return true
	}
	return false
}
