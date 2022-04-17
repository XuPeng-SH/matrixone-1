package updates2

import "sync"

type BlockNode struct {
	*sync.RWMutex
	updates map[uint16]*ColumnChain
}
