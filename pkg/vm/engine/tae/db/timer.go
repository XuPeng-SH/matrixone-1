package db

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

type calibrationProcessor struct {
	*catalog.LoopProcessor
}

func newCalibrationProcessor() *calibrationProcessor {
	processor := &calibrationProcessor{
		LoopProcessor: new(catalog.LoopProcessor),
	}
	processor.BlockFn = processor.onBlock
	return processor
}

func (processor *calibrationProcessor) onBlock(blockEntry *catalog.BlockEntry) (err error) {
	now := time.Now()
	data := blockEntry.GetBlockData()
	data.RunCalibration()
	logutil.Infof("%s Score: %d, Time: %s", data.MutationInfo(), data.EstimateScore(), time.Since(now))
	return
}

type timedLooper struct {
	db        *DB
	processor catalog.Processor
}

func newTimedLooper(db *DB, processor catalog.Processor) *timedLooper {
	c := &timedLooper{
		processor: processor,
		db:        db,
	}
	return c
}

func (collector *timedLooper) OnStopped() {
	logutil.Infof("TimedLooper Stopped")
}

func (collector *timedLooper) OnExec() {
	collector.db.Opts.Catalog.RecurLoop(collector.processor)
}
