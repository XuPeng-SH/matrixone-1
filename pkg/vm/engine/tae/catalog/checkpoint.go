package catalog

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type LogEntry = entry.Entry

const (
	ETCatalogCheckpoint = entry.ETCustomizedStart + 100 + iota
)

type Checkpoint struct {
	MaxTS uint64
	LSN   uint64
}

func (ckp *Checkpoint) String() string {
	if ckp == nil {
		return "LSN=0,MaxTS=0"
	}
	return fmt.Sprintf("LSN=%d,MaxTS=%d", ckp.LSN, ckp.MaxTS)
}

type CheckpointEntry struct {
	MinTS, MaxTS uint64
	LogIndexes   []*wal.Index
	Entries      []*EntryCommand
}

func NewEmptyCheckpointEntry() *CheckpointEntry {
	return &CheckpointEntry{
		Entries: make([]*EntryCommand, 0, 16),
	}
}

func NewCheckpointEntry(minTs, maxTs uint64) *CheckpointEntry {
	return &CheckpointEntry{
		MinTS:      minTs,
		MaxTS:      maxTs,
		LogIndexes: make([]*wal.Index, 0, 16),
		Entries:    make([]*EntryCommand, 0, 16),
	}
}

func (e *CheckpointEntry) AddCommand(cmd *EntryCommand) {
	e.Entries = append(e.Entries, cmd)
}

func (e *CheckpointEntry) AddIndex(index *wal.Index) {
	e.LogIndexes = append(e.LogIndexes, index)
}

func (e *CheckpointEntry) Unmarshal(buf []byte) (err error) {
	r := bytes.NewBuffer(buf)
	if err = binary.Read(r, binary.BigEndian, &e.MinTS); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &e.MaxTS); err != nil {
		return
	}
	var cmdCnt uint32
	if err = binary.Read(r, binary.BigEndian, &cmdCnt); err != nil {
		return
	}
	for i := 0; i < int(cmdCnt); i++ {
		cmd, err := txnbase.BuildCommandFrom(r)
		if err != nil {
			return err
		}
		e.Entries = append(e.Entries, cmd.(*EntryCommand))
	}

	return
}

func (e *CheckpointEntry) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	if err = binary.Write(&w, binary.BigEndian, e.MinTS); err != nil {
		return
	}
	if err = binary.Write(&w, binary.BigEndian, e.MaxTS); err != nil {
		return
	}

	if err = binary.Write(&w, binary.BigEndian, uint32(len(e.Entries))); err != nil {
		return
	}
	for _, cmd := range e.Entries {
		if err = cmd.WriteTo(&w); err != nil {
			return
		}
	}

	buf = w.Bytes()
	return
}

func (e *CheckpointEntry) MakeLogEntry() (logEntry LogEntry, err error) {
	var buf []byte
	if buf, err = e.Marshal(); err != nil {
		return
	}
	logEntry = entry.GetBase()
	logEntry.SetType(ETCatalogCheckpoint)
	logEntry.Unmarshal(buf)
	return
}
