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
				n.chain.Lock()
				n.Lock()
				err := n.UpdateLocked(uint32(i*2000+j), int32(i*20000+j))
				n.Unlock()
				assert.Nil(t, err)
				n.chain.Unlock()
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

	// t.Log(chain.view.StringLocked())
	// t.Log(chain.StringLocked())
	// t.Log(chain.GetHead().GetPayload().(*ColumnNode).StringLocked())
}
