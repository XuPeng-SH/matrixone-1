package db

import (
	"bytes"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
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
	cmd := meta.MakeLogEntry()
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

	pool, _ := ants.NewPool(1)
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

	entry := tae.Catalog.PrepareCheckpoint(startTs, endTs)

	// for _, entry := range entry.BlockEntries {
	// 	logutil.Info(entry.StringLocked())
	// }
	// for i, index := range entry.LogIndexes {
	// 	t.Logf("%d: %s", i, index.String())
	// }
	assert.Equal(t, len(entry.LogIndexes)-1, len(entry.Entries))
	blockEntry := entry.Entries[6].Block
	seg := blockEntry.GetSegment()
	blk, err := seg.GetBlockEntryByID(blockEntry.ID)
	t.Log(blk.String())
	assert.Nil(t, err)
	assert.True(t, blk.HasDropped())
	assert.True(t, blk.DeleteAt > endTs)
	assert.True(t, blk.CreateAt > startTs)
	assert.Equal(t, blk.CreateAt, blockEntry.CreateAt)
	assert.Equal(t, uint64(0), blockEntry.DeleteAt)

	buf, err := entry.Marshal()
	assert.Nil(t, err)
	t.Log(len(buf))

	entry2 := catalog.NewEmptyCheckpointEntry()
	err = entry2.Unmarshal(buf)
	assert.Nil(t, err)
	assert.Equal(t, entry.MinTS, entry2.MinTS)
	assert.Equal(t, entry.MaxTS, entry2.MaxTS)
	assert.Equal(t, len(entry.Entries), len(entry2.Entries))
	for i := 0; i < len(entry.Entries); i++ {
		blk1 := entry.Entries[i].Block
		blk2 := entry2.Entries[i].Block
		assert.Equal(t, blk1.ID, blk2.ID)
		assert.Equal(t, blk1.CreateAt, blk2.CreateAt)
		assert.Equal(t, blk1.DeleteAt, blk2.DeleteAt)
		assert.Equal(t, blk1.CurrOp, blk2.CurrOp)
	}

	logEntry, err := entry.MakeLogEntry()
	assert.Nil(t, err)
	lsn, err := tae.Wal.AppendEntry(wal.GroupCatalog, logEntry)
	logEntry.WaitDone()
	logEntry.Free()
	t.Log(lsn)
}
