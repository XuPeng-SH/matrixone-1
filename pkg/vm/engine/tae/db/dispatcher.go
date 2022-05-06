package db

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type asyncJobDispatcher struct {
	sync.RWMutex
	*tasks.BaseDispatcher
	actives    map[common.ID]bool
	taskscopes map[uint64][]common.ID
}

func newAsyncJobDispatcher() *asyncJobDispatcher {
	return &asyncJobDispatcher{
		BaseDispatcher: tasks.NewBaseDispatcher(),
	}
}
