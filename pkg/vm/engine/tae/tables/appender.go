package tables

import (
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/accessif"
)

type blockAppender struct {
	node          *appendableNode
	handle        base.INodeHandle
	indexAppender accessif.IAppendableBlockIndexHolder
}

func newAppender(node *appendableNode, idxApd accessif.IAppendableBlockIndexHolder) *blockAppender {
	appender := new(blockAppender)
	appender.node = node
	appender.handle = node.mgr.Pin(node)
	appender.indexAppender = idxApd
	return appender
}

func (appender *blockAppender) Close() error {
	if appender.handle != nil {
		appender.handle.Close()
		appender.handle = nil
	}
	return nil
}

func (appender *blockAppender) GetID() *common.ID {
	return appender.node.meta.AsCommonID()
}

func (appender *blockAppender) PrepareAppend(rows uint32) (n uint32, err error) {
	return appender.node.PrepareAppend(rows)
}

func (appender *blockAppender) ApplyAppend(bat *gbat.Batch, offset, length uint32, ctx interface{}) (from uint32, err error) {

	err = appender.node.Expand(0, func() error {
		var err error
		from, err = appender.node.ApplyAppend(bat, offset, length, ctx)
		return err
	})

	schema := appender.node.meta.GetSegment().GetTable().GetSchema()
	pkIdx := schema.PrimaryKey
	pks := bat.Vecs[pkIdx]
	// logutil.Infof("Append into %d: %s", appender.node.meta.GetID(), pks.String())
	err = appender.indexAppender.BatchInsert(pks, offset, int(length), from, false)

	return
}
