package updates2

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type ColumnView struct {
	links map[uint32]*common.Link
	// chain *ColumnChain
}

// func NewColumnView(chain *ColumnChain) *ColumnView {
func NewColumnView() *ColumnView {
	return &ColumnView{
		// chain: chain,
		links: make(map[uint32]*common.Link),
	}
}

func (view *ColumnView) GetValue(key uint32, startTs uint64) (v interface{}, err error) {
	link := view.links[key]
	if link == nil {
		err = txnbase.ErrNotFound
		return
	}
	head := link.GetHead()
	for head != nil {
		node := head.GetPayload().(*ColumnNode)
		if node.GetStartTS() < startTs {
			node.RLock()
			//        |
			// start \|/ commit
			// --+----+----+-------------->
			//   |_________|  next
			// --|   NODE  |------->
			//   +---------+
			// 1. Read ts is between node start and commit. Go to prev node
			if node.GetCommitTSLocked() > startTs {
				node.RUnlock()
				head = head.GetNext()
				continue
			} else {
				v, err = node.GetValueLocked(key)
				nTxn := node.txn
				node.RUnlock()
				// 2. Node was committed and can be used as the data node
				if nTxn == nil {
					break
				}
				// 3. Node is committing and wait committed or rollbacked
				state := nTxn.GetTxnState(true)
				if state == txnif.TxnStateCommitted {
					// 3.1 If committed. use this node
					break
				} else {
					// 3.2 If rollbacked. go to prev node
					err = nil
					v = nil
					head = head.GetNext()
					continue
				}
			}
		}
		if node.GetStartTS() > startTs {
			head = head.GetNext()
			continue
		}

		node.RLock()
		v, err = node.GetValueLocked(key)
		node.RUnlock()
		break
	}
	if v == nil {
		err = txnbase.ErrNotFound
	}
	return
}

func (view *ColumnView) Insert(key uint32, n *ColumnNode) (err error) {
	// First update to key
	var link *common.Link
	if link = view.links[key]; link == nil {
		link = new(common.Link)
		link.Insert(n)
		view.links[key] = link
		return
	}

	node := link.GetHead().GetPayload().(*ColumnNode)
	node.RLock()
	// 1. The specified row has committed update
	if node.txn == nil {
		// 1.1 The update was committed after txn start. w-w conflict
		if node.GetCommitTSLocked() > n.GetStartTS() {
			err = txnbase.ErrDuplicated
			node.RUnlock()
			return
		}
		node.RUnlock()
		// 1.2 The update was committed before txn start. use it
		link.Insert(n)
		return
	}
	// 2. The specified row was updated by the same txn
	if node.txn.GetStartTS() == n.GetStartTS() {
		node.RUnlock()
		return
	}
	// 3. The specified row has other uncommitted change
	// Note: Here we have some overkill to proactivelly w-w with committing txn
	node.RUnlock()
	err = txnbase.ErrDuplicated
	return
}

func (view *ColumnView) Delete(key uint32, n *ColumnNode) (err error) {
	link := view.links[key]
	var target *common.DLNode
	link.Loop(func(dlnode *common.DLNode) bool {
		node := dlnode.GetPayload().(*ColumnNode)
		if node.GetStartTS() == n.GetStartTS() {
			target = dlnode
			return false
		}
		return true
	}, false)
	if target == nil {
		panic("logic error")
	}
	link.Delete(target)
	return
}

func (view *ColumnView) RowStringLocked(row uint32, link *common.Link) string {
	s := fmt.Sprintf("[ROW=%d]:", row)
	link.Loop(func(dlnode *common.DLNode) bool {
		n := dlnode.GetPayload().(*ColumnNode)
		n.RLock()
		s = fmt.Sprintf("%s\n%s", s, n.StringLocked())
		n.RUnlock()
		return true
	}, false)
	return s
}

func (view *ColumnView) StringLocked() string {
	mask := roaring.New()
	for k, _ := range view.links {
		mask.Add(k)
	}
	s := "[VIEW]"
	it := mask.Iterator()
	for it.HasNext() {
		row := it.Next()
		s = fmt.Sprintf("%s\n%s", s, view.RowStringLocked(row, view.links[row]))
	}
	return s
}

func (view *ColumnView) RowCnt() int {
	return len(view.links)
}
