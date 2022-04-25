package db

import (
	"sync/atomic"
	"time"

	w "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

const (
	WALDir     = "wal"
	CATALOGDir = "catalog"
)

func Open(dirname string, opts *options.Options) (db *DB, err error) {
	dbLocker, err := createDBLock(dirname)
	if err != nil {
		return nil, err
	}
	defer func() {
		if dbLocker != nil {
			dbLocker.Close()
		}
	}()

	opts = opts.FillDefaults(dirname)

	indexBufMgr := buffer.NewNodeManager(opts.CacheCfg.IndexCapacity, nil)
	mutBufMgr := buffer.NewNodeManager(opts.CacheCfg.InsertCapacity, nil)
	txnBufMgr := buffer.NewNodeManager(opts.CacheCfg.TxnCapacity, nil)

	db = &DB{
		Dir:         dirname,
		Opts:        opts,
		IndexBufMgr: indexBufMgr,
		MTBufMgr:    mutBufMgr,
		TxnBufMgr:   txnBufMgr,
		ClosedC:     make(chan struct{}),
		Closed:      new(atomic.Value),
	}

	db.Opts.Catalog = catalog.MockCatalog(dirname, CATALOGDir, nil)

	dataFactory := tables.NewDataFactory(mockio.SegmentFileMockFactory, mutBufMgr)
	db.TxnLogDriver = txnbase.NewNodeDriver(dirname, WALDir, nil)
	txnStoreFactory := txnimpl.TxnStoreFactory(db.Opts.Catalog, db.TxnLogDriver, txnBufMgr, dataFactory)
	txnFactory := txnimpl.TxnFactory(db.Opts.Catalog)
	db.TxnMgr = txnbase.NewTxnManager(txnStoreFactory, txnFactory)

	db.DBLocker, dbLocker = dbLocker, nil
	db.TxnMgr.Start()
	db.IOScheduler = newIOScheduler(db)
	db.TaskScheduler = newTaskScheduler(db)
	db.CKPDriver = checkpoint.NewDriver(db.TaskScheduler)
	handle := newTimedLooper(db, newCalibrationProcessor())
	db.CalibrationTimer = w.NewHeartBeater(time.Duration(opts.CheckpointCfg.CalibrationInterval)*time.Millisecond, handle)
	db.startWorkers()

	return
}
