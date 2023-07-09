package meta

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

var idAlloc = common.NewTxnIDAllocator()

func getNextTxn() *txnbase.Txn {
	txn := new(txnbase.Txn)
	txn.TxnCtx = txnbase.NewTxnCtx(
		idAlloc.Alloc(),
		types.NextGlobalTsForTest(),
		types.TS{},
	)
	return txn
}

func commitNode(t *testing.T, txn *txnbase.Txn, node *DeleteNode) {
	txn.CommitTS = types.NextGlobalTsForTest()
	txn.PrepareTS = txn.CommitTS
	err := node.PrepareCommit()
	require.NoError(t, err)
	err = node.ApplyCommit()
	require.NoError(t, err)
}

func TestChain(t *testing.T) {
	id := common.NextGlobalSeqNum()
	chain := NewDeleteChain(id)

	txn1 := getNextTxn()
	err := chain.PrepareDelete(txn1.GetStartTS(), 0, 2, 3)
	require.NoError(t, err)
	node1 := chain.AddDeletesLocked(txn1, 0, 2, 3)
	require.Equal(t, uint64(3), node1.mask.GetCardinality())
	_ = node1

	txn2 := getNextTxn()
	err = chain.PrepareDelete(txn2.GetStartTS(), 0, 4)
	require.Error(t, err)

	txn3 := getNextTxn()
	err = chain.PrepareDelete(txn3.GetStartTS(), 1)
	require.NoError(t, err)
	node3 := chain.AddDeletesLocked(txn3, 1)

	t.Log(chain.String())
	commitNode(t, txn3, node3)

	t.Log(chain.String())
}
