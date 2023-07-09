package meta

import "fmt"

func (n *AppendNode) String() string {
	return fmt.Sprintf(
		"%s;[%d,%d]",
		&n.TxnMVCCNode.String(),
		n.startRow,
		n.endRow,
	)
}

func (n *AppendNode) PrepareRollback() (err error) {
	n.chain.Lock()
	defer n.chain.Unlock()
	n.chain.DeleteNodeLocked(n)
	return
}

func (n *AppendNode) ApplyRollback() (err error) {
	n.chain.Lock()
	defer n.chain.Unlock()
	_, err = n.TxnMVCCNode.ApplyRollback()
	return
}

func (n *AppendNode) PrepareCommit() (err error) {
	n.chain.Lock()
	defer n.chain.Unlock()
	_, err = n.TxnMVCCNode.PrepareCommit()
	return
}

func (n *AppendNode) ApplyCommit() (err error) {
	n.chain.Lock()
	defer n.chain.Unlock()
	_, err = n.TxnMVCCNode.ApplyCommit()
	return
}
