package updates2

import "sync"

type AppendNode struct {
	*sync.RWMutex
}
