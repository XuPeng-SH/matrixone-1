package state

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func (e *BlockEntry) GetID() *objectio.Blockid {
	return &e.id
}

func (e *BlockEntry) GetState() uint8 {
	return e.state
}

func (e *BlockEntry) GetPKZM() *index.ZM {
	return e.pkZM.Load()
}

func (n *BlockEntryNode) GetMetaLoc() objectio.Location {
	if n.state == 1 || n.frozen.Load() {
		return n.location
	}
	n.RLock()
	defer n.RUnlock()
	return n.location
}

func (n *BlockEntryNode) GetDeltaLocs() []objectio.Location {
	if n.frozen.Load() {
		return n.deltaLocs
	}
	n.RLock()
	defer n.RUnlock()
	return n.deltaLocs
}
