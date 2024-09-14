package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func BuildTombstoneFileTransferFlow(
	ctx context.Context,
	table *txnTable,
	nextTombstoneFile func() *objectio.ObjectStats,
	isObjectDeletedFn func(*objectio.Objectid) bool,
	targetObjects []objectio.ObjectStats,
	mp *mpool.MPool,
) *TrasnferFlow {
	data := batch.New(false, []string{catalog.Row_ID, table.tableDef.Cols[table.primaryIdx].Name})
	data.SetVector(0, vector.NewVec(objectio.RowidType))
	data.SetVector(1, vector.NewVec(types.T(table.tableDef.Cols[table.primaryIdx].Typ.Id).ToType()))

	sourcer := NewObjectSourcer(
		ctx,
		table,
		nextTombstoneFile,
		*data.Vecs[1].GetType(),
		data.Attrs[1],
		mp,
	)

	processor := NewTransferProcessor(
		*data.Vecs[1].GetType(),
		data.Attrs[1],
		isObjectDeletedFn,
		targetObjects,
		mp,
	)

	return &TrasnferFlow{
		processor: processor,
		sourcer:   sourcer,
		sinker:    new(TransferSinker),
	}
}

type TrasnferFlow struct {
	processor *TransferProcessor
	data      *batch.Batch
	sourcer   *ObjectSourcer
	sinker    *TransferSinker
	outStats  []objectio.ObjectStats
}

func (t *TrasnferFlow) Run(ctx context.Context, mp *mpool.MPool) error {
	var (
		isEnd    bool
		overflow bool
		err      error
		buffer   *batch.Batch
	)
	for !isEnd {
		if isEnd, err = t.sourcer.Next(ctx, t.data); err != nil {
			return err
		}
		if buffer, err = t.processor.Process(ctx, t.data); err != nil {
			return err
		}
		if overflow, err = t.sinker.Write(ctx, buffer); err != nil {
			return err
		}
		if overflow {
			var stats objectio.ObjectStats
			if stats, err = t.sinker.Sync(); err != nil {
				return err
			}
			t.outStats = append(t.outStats, stats)
		}
	}
	stats, err = t.sinker.Sync()
	if err != nil {
		return err
	}
	t.outStats = append(t.outStats, stats)

	return nil
}

func (t *TrasnferFlow) GetStats() []objectio.ObjectStats {
	return t.outStats
}

func (t *TrasnferFlow) Close() {
	if t.sourcer != nil {
		t.sourcer.Close()
		t.sourcer = nil
	}
	if t.sinker != nil {
		t.sinker.Close()
		t.sinker = nil
	}
	if t.processor != nil {
		t.processor.Close()
		t.processor = nil
	}
	if t.data != nil {
		t.data.Close()
		t.data = nil
	}
	t.outStats = nil
}

func NewObjectSourcer(
	ctx context.Context,
	table *txnTable,
	nextTombstoneFile func() *objectio.ObjectStats,
	pkType types.Type,
	pkColumName string,
	mp *mpool.MPool,
) (*ObjectSourcer, error) {
	buffer := batch.New(false, []string{catalog.Row_ID, pkColumName})
	buffer.SetVector(0, vector.NewVec(objectio.RowidType))
	buffer.SetVector(1, vector.NewVec(pkType))
	sourcer := &ObjectSourcer{
		nextTombstoneFile: nextTombstoneFile,
		mp:                mp,
		buffer:            buffer,
	}

	return sourcer
}

type ObjectSourcer struct {
	table  *txnTable
	reader engine.Reader
	buffer *batch.Batch
	mp     *mpool.MPool

	nextTombstoneFile func() *objectio.ObjectStats
}

func (s *ObjectSourcer) getReader(ctx context.Context) (r engine.Reader, err error) {
	if s.reader != nil {
		return s.reader, nil
	}
	obj := nextTombstoneFile()
	relData := NewObjectBlockListRelationData(obj, true)
	readers, err := s.table.BuildReaders(
		ctx,
		table.proc.Load(),
		nil,
		relData,
		1,
		0,
		false,
		engine.Policy_CheckCommittedOnly,
	)
	if err != nil {
		return nil, err
	}
	s.reader = readers[0]
	return s.reader, nil
}

func (s *ObjectSourcer) Next(ctx context.Context, data *batch.Batch) (bool, error) {
	reader := s.getReader(ctx)
	done, err := reader.Read(ctx, s.buffer.Attrs, nil, s.mp, s.buffer)
	if err != nil {
		return done, err
	}
	if done {
		s.closeReader()
	}
	return done, nil
}

func (s *ObjectSourcer) closeReader() {
	if s.reader != nil {
		s.reader.Close()
		s.reader = reader
	}
}

func (s *ObjectSourcer) Close() {
	if s.buffer != nil {
		s.buffer.Clean(s.mp)
		s.buffer = nil
		s.mp = nil
	}
	if s.reader != nil {
		s.reader.Close()
		s.reader = nil
	}
}

func NewTransferProcessor(
	pkType types.Type,
	pkColumName string,
	isObjectDeletedFn func(*objectio.Objectid) bool,
	targetObjects []objectio.ObjectStats,
	mp *mpool.MPool,
) *TransferProcessor {
	buffer := batch.New(false, []string{catalog.Row_ID, pkColumName})
	buffer.SetVector(0, vector.NewVec(objectio.RowidType))
	buffer.SetVector(1, vector.NewVec(pkType))
	buffer2 := batch.New(false, []string{catalog.Row_ID, pkColumName})
	buffer2.SetVector(0, vector.NewVec(objectio.RowidType))
	buffer2.SetVector(1, vector.NewVec(pkType))

	return &TransferProcessor{
		currBuf:           buffer1,
		outBuf:            buffer2,
		mp:                mp,
		isObjectDeletedFn: isObjectDeletedFn,
		targetObjects:     targetObjects,
	}
}

type TransferProcessor struct {
	currBuf *batch.Batch
	outBuf  *batch.Batch
	mp      *mpool.MPool

	isObjectDeletedFn func(*objectio.Objectid) bool
	targetObjects     []objectio.ObjectStats
}

func (p *TransferProcessor) Process(
	ctx context.Context, input *batch.Batch, isEnd bool,
) (output *batch.Batch, err error) {
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](input.Vecs[0])
	if len(rowids) == 0 {
		return nil
	}
	var (
		last    types.ObjectId
		deleted bool
	)
	for i, rowid := range rowids {
		objectid := rowid.BorrowObjectID()
		if !objectid.Eq(last) {
			deleted = p.isObjectDeletedFn(objectid)
			last = *objectid
		}
		if deleted {
			continue
		}
		if err = p.currBuf.UnionOne(input, int64(i), p.mp); err != nil {
			return
		}
		if p.currBuf.Vecs[0].Length() >= objectio.BlockMaxRows {
			buffer := p.currBuf
			p.outBuf.CleanOnlyData()
			p.currBuf = p.outBuf
			p.outBuf = buffer
			if err = p.doProcess(p.outBuf); err != nil {
				return
			}
		}
	}
	p.outBuf.SetRowCount(p.outBuf.Vecs[0].Length())
	output = p.outBuf
	return
}

func (p *TransferProcessor) doProcess(input *batch.Batch) error {
}

func (p *TransferProcessor) Close() {
	if p.buffer1 != nil {
		p.buffer1.Clean(p.mp)
		p.buffer1 = nil
	}
	if p.buffer2 != nil {
		p.buffer2.Clean(p.mp)
		p.buffer2 = nil
	}
	p.mp = nil
}

type TransferSinker struct {
	writer *colexec.S3Writer
}

func (s *TransferSinker) Write(
	proc process.Process, input *batch.Batch,
) (bool, error) {
	data, err := input.Dup(proc.Mp())
	if s.writer == nil {
		s.writer = colexec.NewS3TombstoneWriter()
	}
	return s.writer.StashBatch(proc, data), nil
}

func (s *TransferSinker) Sync(proc *process.Process) (objectio.ObjectStats, error) {
	_, stats, err := s.writer.Sync()
	s.writer.Free(proc)
	s.writer = nil
	return stats, err
}

func (s *TransferSinker) Close(proc *process.Process) {
	if s.writer != nil {
		s.writer.Free(proc)
		s.writer = nil
	}
}
