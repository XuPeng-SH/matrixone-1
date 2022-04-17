package updates2

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func initTestPath(t *testing.T) string {
	dir := filepath.Join("/tmp", t.Name())
	os.RemoveAll(dir)
	return dir
}

func mockTxn() *txnbase.Txn {
	txn := new(txnbase.Txn)
	txn.TxnCtx = txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(), common.NextGlobalSeqNum(), nil)
	return txn
}

func commitTxn(txn *txnbase.Txn) {
	txn.CommitTS = common.NextGlobalSeqNum()
}

func TestColumnChain1(t *testing.T) {
	schema := catalog.MockSchema(1)
	c := catalog.MockCatalog(initTestPath(t), "mock", nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	// uncommitted := new(common.Link)
	chain := NewColumnChain(nil, 0, blk)
	cnt1 := 2
	cnt2 := 3
	cnt3 := 4
	cnt4 := 5
	for i := 0; i < cnt1+cnt2+cnt3+cnt4; i++ {
		txn := new(txnbase.Txn)
		txn.TxnCtx = txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(), common.NextGlobalSeqNum(), nil)
		n := chain.AddNode(txn)
		if (i >= cnt1 && i < cnt1+cnt2) || (i >= cnt1+cnt2+cnt3) {
			txn.CommitTS = common.NextGlobalSeqNum()
			n.PrepareCommit()
			n.ApplyCommit()
		}
	}
	t.Log(chain.StringLocked())
	assert.Equal(t, cnt1+cnt2+cnt3+cnt4, chain.DepthLocked())
	t.Log(chain.GetHead().GetPayload().(*ColumnNode).StringLocked())
}

func TestColumnChain2(t *testing.T) {
	schema := catalog.MockSchema(1)
	c := catalog.MockCatalog(initTestPath(t), "mock", nil)
	defer c.Close()

	db, _ := c.CreateDBEntry("db", nil)
	table, _ := db.CreateTableEntry(schema, nil, nil)
	seg, _ := table.CreateSegment(nil, catalog.ES_Appendable, nil)
	blk, _ := seg.CreateBlock(nil, catalog.ES_Appendable, nil)

	chain := NewColumnChain(nil, 0, blk)
	txn1 := new(txnbase.Txn)
	txn1.TxnCtx = txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(), common.NextGlobalSeqNum(), nil)
	n1 := chain.AddNode(txn1)

	err := n1.UpdateLocked(1, int32(11))
	assert.Nil(t, err)
	err = n1.UpdateLocked(2, int32(22))
	assert.Nil(t, err)
	err = n1.UpdateLocked(3, int32(33))
	assert.Nil(t, err)
	err = n1.UpdateLocked(1, int32(111))
	assert.Nil(t, err)
	assert.Equal(t, 3, chain.view.RowCnt())

	txn2 := new(txnbase.Txn)
	txn2.TxnCtx = txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(), common.NextGlobalSeqNum(), nil)
	n2 := chain.AddNode(txn2)
	err = n2.UpdateLocked(2, int32(222))
	assert.Equal(t, txnbase.ErrDuplicated, err)
	err = n2.UpdateLocked(4, int32(44))
	assert.Nil(t, err)
	assert.Equal(t, 4, chain.view.RowCnt())

	txn1.CommitTS = common.NextGlobalSeqNum()
	n1.PrepareCommit()
	n1.ApplyCommit()

	err = n2.UpdateLocked(2, int32(222))
	assert.Equal(t, txnbase.ErrDuplicated, err)

	assert.Equal(t, 1, chain.view.links[1].Depth())
	assert.Equal(t, 1, chain.view.links[2].Depth())
	assert.Equal(t, 1, chain.view.links[3].Depth())
	assert.Equal(t, 1, chain.view.links[4].Depth())

	txn3 := new(txnbase.Txn)
	txn3.TxnCtx = txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(), common.NextGlobalSeqNum(), nil)
	n3 := chain.AddNode(txn3)
	err = n3.UpdateLocked(2, int32(2222))
	assert.Nil(t, err)
	assert.Equal(t, 2, chain.view.links[2].Depth())

	var wg sync.WaitGroup
	doUpdate := func(i int) func() {
		return func() {
			defer wg.Done()
			txn := new(txnbase.Txn)
			txn.TxnCtx = txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(), common.NextGlobalSeqNum(), nil)
			n := chain.AddNode(txn)
			for j := 0; j < 4; j++ {
				n.GetChain().Lock()
				err := n.UpdateLocked(uint32(i*2000+j), int32(i*20000+j))
				assert.Nil(t, err)
				n.GetChain().Unlock()
			}
		}
	}

	p, _ := ants.NewPool(10)
	now := time.Now()
	for i := 1; i < 30; i++ {
		wg.Add(1)
		p.Submit(doUpdate(i))
	}
	wg.Wait()
	t.Log(time.Since(now))

	t.Log(chain.Depth())

	v, err := chain.view.GetValue(1, txn1.GetStartTS())
	assert.Equal(t, int32(111), v)
	assert.Nil(t, err)
	v, err = chain.view.GetValue(2, txn1.GetStartTS())
	assert.Equal(t, int32(22), v)
	assert.Nil(t, err)
	v, err = chain.view.GetValue(2, txn2.GetStartTS())
	assert.NotNil(t, err)
	v, err = chain.view.GetValue(2, txn3.GetStartTS())
	assert.Equal(t, int32(2222), v)
	assert.Nil(t, err)
	v, err = chain.view.GetValue(2, common.NextGlobalSeqNum())
	assert.Equal(t, int32(22), v)
	assert.Nil(t, err)
	v, err = chain.view.GetValue(2000, common.NextGlobalSeqNum())
	assert.NotNil(t, err)

	mask, vals := chain.view.CollectUpdates(txn1.GetStartTS())
	assert.True(t, mask.Contains(1))
	assert.True(t, mask.Contains(2))
	assert.True(t, mask.Contains(3))
	assert.Equal(t, int32(111), vals[uint32(1)])
	assert.Equal(t, int32(22), vals[uint32(2)])
	assert.Equal(t, int32(33), vals[uint32(3)])
	t.Log(mask.String())
	t.Log(vals)

	// t.Log(chain.view.StringLocked())
	// t.Log(chain.StringLocked())
	// t.Log(chain.GetHead().GetPayload().(*ColumnNode).StringLocked())
}

func TestDeleteChain1(t *testing.T) {
	chain := NewDeleteChain(nil, nil)
	txn1 := new(txnbase.Txn)
	txn1.TxnCtx = txnbase.NewTxnCtx(nil, common.NextGlobalSeqNum(), common.NextGlobalSeqNum(), nil)
	n1 := chain.AddNodeLocked(txn1)
	assert.Equal(t, 1, chain.Depth())

	// 1. Txn1 delete from 1 to 10 -- PASS
	err := chain.PrepareRangeDelete(1, 10, txn1.GetStartTS())
	assert.Nil(t, err)
	n1.RangeDeleteLocked(1, 10)
	assert.Equal(t, uint32(10), n1.GetCardinalityLocked())
	t.Log(n1.mask.String())

	// 2. Txn1 delete 10 -- FAIL
	err = chain.PrepareRangeDelete(10, 10, txn1.GetStartTS())
	assert.NotNil(t, err)

	txn2 := mockTxn()
	// 3. Txn2 delete 2 -- FAIL
	err = chain.PrepareRangeDelete(2, 2, txn2.GetStartTS())
	assert.NotNil(t, err)

	// 4. Txn2 delete from 21 to 30 -- PASS
	err = chain.PrepareRangeDelete(20, 30, txn2.GetStartTS())
	assert.Nil(t, err)
	n2 := chain.AddNodeLocked(txn2)
	n2.RangeDeleteLocked(20, 30)
	assert.Equal(t, uint32(11), n2.GetCardinalityLocked())
	t.Log(n2.mask.String())

	merged := chain.AddMergeNode()
	assert.Nil(t, merged)
	assert.Equal(t, 2, chain.Depth())

	collected := chain.CollectDeletesLocked(txn1.GetStartTS())
	assert.Equal(t, uint32(10), collected.GetCardinalityLocked())
	collected = chain.CollectDeletesLocked(txn2.GetStartTS())
	assert.Equal(t, uint32(11), collected.GetCardinalityLocked())

	collected = chain.CollectDeletesLocked(0)
	assert.Nil(t, collected)
	collected = chain.CollectDeletesLocked(common.NextGlobalSeqNum())
	assert.Nil(t, collected)

	commitTxn(txn1)
	assert.Nil(t, n1.PrepareCommit())
	assert.Nil(t, n1.ApplyCommit())
	t.Log(chain.StringLocked())

	collected = chain.CollectDeletesLocked(0)
	assert.Nil(t, collected)
	collected = chain.CollectDeletesLocked(common.NextGlobalSeqNum())
	assert.Equal(t, uint32(10), collected.GetCardinalityLocked())
	collected = chain.CollectDeletesLocked(txn2.GetStartTS())
	assert.Equal(t, uint32(11), collected.GetCardinalityLocked())

	txn3 := mockTxn()
	err = chain.PrepareRangeDelete(5, 5, txn3.GetStartTS())
	assert.NotNil(t, err)
	err = chain.PrepareRangeDelete(31, 33, txn3.GetStartTS())
	assert.Nil(t, err)
	n3 := chain.AddNodeLocked(txn3)
	n3.RangeDeleteLocked(31, 33)

	collected = chain.CollectDeletesLocked(txn3.GetStartTS())
	assert.Equal(t, uint32(13), collected.GetCardinalityLocked())
	t.Log(chain.StringLocked())

	merged = chain.AddMergeNode()
	assert.NotNil(t, merged)
	t.Log(chain.StringLocked())
	assert.Equal(t, 4, chain.DepthLocked())
}
