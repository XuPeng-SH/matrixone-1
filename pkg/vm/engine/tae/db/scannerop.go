package db

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type ScannerOp interface {
	catalog.Processor
	PreExecute() error
	PostExecute() error
}

type calibrationOp struct {
	*catalog.LoopProcessor
	db *DB
}

func newCalibrationOp(db *DB) *calibrationOp {
	processor := &calibrationOp{
		db:            db,
		LoopProcessor: new(catalog.LoopProcessor),
	}
	processor.BlockFn = processor.onBlock
	return processor
}

func (processor *calibrationOp) PreExecute() error  { return nil }
func (processor *calibrationOp) PostExecute() error { return nil }

func (processor *calibrationOp) onSegment(segmentEntry *catalog.SegmentEntry) (err error) {
	segmentEntry.RLock()
	maxTs := segmentEntry.MaxCommittedTS()
	segmentEntry.RUnlock()
	if maxTs == txnif.UncommitTS {
		panic(maxTs)
	}
	return
}

func (processor *calibrationOp) onBlock(blockEntry *catalog.BlockEntry) (err error) {
	blockEntry.RLock()
	// 1. Skip uncommitted entries
	if !blockEntry.IsCommitted() {
		blockEntry.RUnlock()
		return nil
	}
	// 2. Skip committed dropped entries
	if blockEntry.IsDroppedCommitted() {
		blockEntry.RUnlock()
		return nil
	}
	blockEntry.RUnlock()

	data := blockEntry.GetBlockData()

	// 3. Run calibration and estimate score for checkpoint
	data.RunCalibration()
	score := data.EstimateScore()
	if score > 0 {
		processor.db.CKPDriver.EnqueueCheckpointUnit(data)
	}
	return
}

type catalogStatsMonitor struct {
	*catalog.LoopProcessor
	db                         *DB
	unCheckpointedCnt          int
	checkpointedTS             uint64
	lastCheckpointScheduleTime uint64
}

func newCatalogStatsMonitor(db *DB) *catalogStatsMonitor {
	monitor := &catalogStatsMonitor{
		db: db,
	}
	monitor.BlockFn = monitor.onBlock
	monitor.SegmentFn = monitor.onSegment
	monitor.TableFn = monitor.onTable
	monitor.DatabaseFn = monitor.onDatabase
	return monitor
}

func (monitor *catalogStatsMonitor) PreExecute() error {
	monitor.unCheckpointedCnt = 0
	monitor.checkpointedTS = monitor.db.Catalog.GetCheckpointed()
	return nil
}

func (monitor *catalogStatsMonitor) PostExecute() error {
	return nil
}

func (monitor *catalogStatsMonitor) onBlock(entry *catalog.BlockEntry) (err error) {
	return
}

func (monitor *catalogStatsMonitor) onSegment(entry *catalog.SegmentEntry) (err error) {
	return
}

func (monitor *catalogStatsMonitor) onTable(entry *catalog.TableEntry) (err error) {
	return
}

func (monitor *catalogStatsMonitor) onDatabase(entry *catalog.DBEntry) (err error) {
	return
}
