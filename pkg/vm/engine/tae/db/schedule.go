package db

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type taskScheduler struct {
	*tasks.BaseScheduler
	db *DB
}

func NewScheduler(db *DB) *taskScheduler {
	s := &taskScheduler{
		BaseScheduler: tasks.NewBaseScheduler("tae"),
		db:            db,
	}
	dispatcher := tasks.NewBaseDispatcher()
	ioHandlers := tasks.NewPoolHandler(1)
	ioHandlers.Start()
	txnHandler := tasks.NewPoolHandler(1)
	txnHandler.Start()

	dispatcher.RegisterHandler(tasks.TxnTask, txnHandler)
	dispatcher.RegisterHandler(tasks.CompactBlockTask, txnHandler)

	s.RegisterDispatcher(tasks.TxnTask, dispatcher)
	s.RegisterDispatcher(tasks.CompactBlockTask, dispatcher)
	s.Start()
	return s
}

func (s *taskScheduler) ScheduleTxnTask(ctx *tasks.Context, factory tasks.TxnTaskFactory) (task tasks.Task, err error) {
	task = NewScheduledTxnTask(ctx, s.db, factory)
	err = s.Schedule(task)
	return
}
