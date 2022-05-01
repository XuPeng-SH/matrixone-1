package wal

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"

const (
	GroupC uint32 = iota + 10
	GroupUC
)

type NodeEntry entry.Entry

type NodeDriver interface {
	AppendEntry(uint32, NodeEntry) (uint64, error)
	LoadEntry(groupId uint32, lsn uint64) (NodeEntry, error)
	Close() error
}
