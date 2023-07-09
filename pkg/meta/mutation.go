package meta

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (ss *MutationSnapshot) GetChainAndLocations(
	id uint64,
) (chain *DeleteChain, locs []*DeleteLocation) {
	if ss.chains.Len() > 0 {
		chainPivot := &DeleteChain{id: id}
		chain, _ = ss.chains.Get(chainPivot)
	}
	if ss.locations.Len() > 0 {
		locationPivot := &DeleteLocation{id: id}
		ss.locations.Ascend(
			locationPivot,
			func(item *DeleteLocation) bool {
				locs = append(locs, item)
				return true
			},
		)
	}
	return
}

func (mut *Mutation) GetSnapshot() *MutationSnapshot {
	mut.mu.RLock()
	defer mut.mu.RUnlock()
	return mut.GetSnapshotLocked()
}

func (mut *Mutation) GetSnapshotLocked() *MutationSnapshot {
	return &MutationSnapshot{
		from:      mut.from,
		chains:    mut.chains.Copy(),
		locations: mut.locations.Copy(),
	}
}

func (mut *Mutation) GetChainAndLocations(
	id uint64,
) (chain *DeleteChain, locs []*DeleteLocation) {
	snap := mut.GetSnapshot()
	return snap.GetChainAndLocations(id)
}

func (mut *Mutation) PrepareDelete(
	ts types.TS, id uint64, rows ...uint32,
) (err error) {
	chain, locs := mut.GetChainAndLocations(id)
	if chain != nil {
		if err = chain.PrepareDelete(ts, rows...); err != nil {
			return
		}
	}
	for _, loc := range locs {
		if err = loc.PrepareDelete(ts, rows...); err != nil {
			return
		}
	}
	return
}

func (mut *Mutation) Delete(
	txn txnif.TxnReader,
	id uint64,
	rows ...uint32,
) (node *DeleteNode, err error) {
	mut.mu.Lock()
	snap := mut.GetSnapshotLocked()
	chain, _ := snap.GetChainAndLocations(id)
	if chain == nil {
		chain = mut.AddChainLocked(id)
	}
	chain.Lock()
	mut.mu.Unlock()
	defer chain.Unlock()
	if err = chain.PrepareDeleteLocked(txn.GetStartTS(), rows...); err != nil {
		return
	}
	node = chain.AddDeletesLocked(txn, rows...)
	return
}

func (mut *Mutation) AddChainLocked(id uint64) (chain *DeleteChain) {
	chain = &DeleteChain{id: id}
	// TODO: check first
	mut.chains.Set(chain)
	return
}

func (mut *Mutation) Truncate(
	ts types.TS,
) {
	mut.mu.Lock()
	chainSnap := mut.chains.Copy()
	locSnap := mut.locations.Copy()
	mut.mu.Unlock()
	chainSnap.Scan(func(item *DeleteChain) bool {
		item.Truncate(ts)
		return true
	})
	locSnap.Scan(func(item *DeleteLocation) bool {
		// item.Truncate(ts)
		return true
	})
}
