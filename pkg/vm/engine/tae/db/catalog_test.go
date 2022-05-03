package db

import (
	"bytes"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestCatalog1(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	txn := db.StartTxn(nil)
	schema := catalog.MockSchema(1)
	database, _ := txn.CreateDatabase("db")
	rel, _ := database.CreateRelation(schema)
	// relMeta := rel.GetMeta().(*catalog.TableEntry)
	seg, _ := rel.CreateSegment()
	blk, err := seg.CreateBlock()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))

	txn = db.StartTxn(nil)
	database, _ = txn.GetDatabase("db")
	rel, _ = database.GetRelationByName(schema.Name)
	sseg, err := rel.GetSegment(seg.GetID())
	assert.Nil(t, err)
	t.Log(sseg.String())
	err = sseg.SoftDeleteBlock(blk.Fingerprint().BlockID)
	assert.Nil(t, err)

	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	blk2, err := sseg.CreateBlock()
	assert.Nil(t, err)
	assert.NotNil(t, blk2)
	assert.Nil(t, txn.Commit())
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))

	{
		txn = db.StartTxn(nil)
		database, _ = txn.GetDatabase("db")
		rel, _ = database.GetRelationByName(schema.Name)
		t.Log(txn.String())
		it := rel.MakeBlockIt()
		cnt := 0
		for it.Valid() {
			block := it.GetBlock()
			cnt++
			t.Log(block.String())
			it.Next()
		}
		assert.Equal(t, 1, cnt)
	}
}

func TestLogBlock(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(2)
	txn := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	seg, _ := rel.CreateSegment()
	blk, _ := seg.CreateBlock()
	meta := blk.GetMeta().(*catalog.BlockEntry)
	err := txn.Commit()
	assert.Nil(t, err)
	cmd, err := meta.MakeLogEntry()
	assert.Nil(t, err)
	assert.NotNil(t, cmd)

	var w bytes.Buffer
	err = cmd.WriteTo(&w)
	assert.Nil(t, nil)

	buf := w.Bytes()
	r := bytes.NewBuffer(buf)
	cmd2, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	entryCmd := cmd2.(*catalog.EntryCommand)
	t.Log(meta.StringLocked())
	t.Log(entryCmd.Block.StringLocked())
	assert.Equal(t, meta.ID, entryCmd.Block.ID)
	assert.Equal(t, meta.CurrOp, entryCmd.Block.CurrOp)
	assert.Equal(t, meta.CreateAt, entryCmd.Block.CreateAt)
	assert.Equal(t, meta.DeleteAt, entryCmd.Block.DeleteAt)
}

func TestCheckpointCatalog(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	txn := tae.StartTxn(nil)
	schema := catalog.MockSchemaAll(2)
	db, _ := txn.CreateDatabase("db")
	db.CreateRelation(schema)
	txn.Commit()

	pool, _ := ants.NewPool(10)
	var wg sync.WaitGroup
	mockRes := func() {
		defer wg.Done()
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		seg, err := rel.CreateSegment()
		assert.Nil(t, err)
		var id *common.ID
		for i := 0; i < 4; i++ {
			blk, err := seg.CreateBlock()
			if i == 2 {
				id = blk.Fingerprint()
			}
			assert.Nil(t, err)
		}
		err = txn.Commit()
		assert.Nil(t, err)

		txn = tae.StartTxn(nil)
		db, _ = txn.GetDatabase("db")
		rel, _ = db.GetRelationByName(schema.Name)
		seg, _ = rel.GetSegment(id.SegmentID)
		err = seg.SoftDeleteBlock(id.BlockID)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		pool.Submit(mockRes)
	}
	wg.Wait()
	t.Log(tae.Catalog.SimplePPString(common.PPL1))

	startTs := uint64(0)
	endTs := tae.TxnMgr.StatSafeTS() - 2
	t.Logf("endTs=%d", endTs)

	indexes := tae.Catalog.PrepareCheckpoint(startTs, endTs)

	for i, index := range indexes {
		t.Logf("%d: %s", i, index.String())
	}
}
