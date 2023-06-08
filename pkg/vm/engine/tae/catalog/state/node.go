package state

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (n *TxnNode) GetStart() types.TS {
	return n.start
}

func (n *TxnNode) GetEnd() types.TS {
	return n.end
}

func (n *TxnNode) GetTxn() txnif.TxnReader {
	return n.txn
}

func (n *TxnNode) IsAborted() bool {
	return n.aborted
}

func (n *TxnNode) IsCommitted() bool {
	return n.txn == nil
}

func (n *TxnNode) IsActive() bool {
	return n.txn != nil && !n.txn.GetTxnState(false) == txnif.TxnStateActive
}

func (n *TxnNode) IsSameTxn(txn txnif.TxnReader) bool {
	if n.txn == nil || txn == nil {
		return false
	}
	return n.txn.GetID() == txn.GetID()
}

func (n *TxnNode) CheckConflict(txn txnif.TxnReader) (err error) {
	if n.IsCommitted() {
		if n.IsSameTxn(txn) {
			return
		}
		return txnif.ErrTxnWWConflict
	}

	if n.end.Greater(txn.GetStart()) {
		return txnif.ErrTxnWWConflict
	}

	return
}

func (n *TxnNode) IsVisible(txn txnif.TxnReader) (visible bool) {
	if n.IsSameTxn(txn) {
		return true
	}
	if n.IsActive() || n.IsAborted() {
		return false
	}
	return n.end.LessEq(txn.GetStartTS())
}
