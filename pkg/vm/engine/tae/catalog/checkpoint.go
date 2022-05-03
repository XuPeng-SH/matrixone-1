package catalog

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"

type CheckpointEntry struct {
	MinTS, MaxTS   uint64
	LogIndexes     []*wal.Index
	BlockEntries   []*BlockEntry
	SegmentEntries []*SegmentEntry
	TableEntries   []*TableEntry
	DBEntries      []*DBEntry
}

func NewCheckpointEntry(minTs, maxTs uint64) *CheckpointEntry {
	return &CheckpointEntry{
		MinTS:          minTs,
		MaxTS:          maxTs,
		LogIndexes:     make([]*wal.Index, 0, 16),
		BlockEntries:   make([]*BlockEntry, 0, 16),
		SegmentEntries: make([]*SegmentEntry, 0, 4),
		TableEntries:   make([]*TableEntry, 0, 4),
		DBEntries:      make([]*DBEntry, 0, 4),
	}
}

func (entry *CheckpointEntry) AddBlock(block *BlockEntry) {
	entry.BlockEntries = append(entry.BlockEntries, block)
}

func (entry *CheckpointEntry) AddSegment(segment *SegmentEntry) {
	entry.SegmentEntries = append(entry.SegmentEntries, segment)
}

func (entry *CheckpointEntry) AddTable(table *TableEntry) {
	entry.TableEntries = append(entry.TableEntries, table)
}

func (entry *CheckpointEntry) AddDB(db *DBEntry) {
	entry.DBEntries = append(entry.DBEntries, db)
}

func (entry *CheckpointEntry) AddIndex(index *wal.Index) {
	entry.LogIndexes = append(entry.LogIndexes, index)
}
