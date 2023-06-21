// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbutils

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type TraceTicket interface {
	ID() uint64
	Release() error
	String() string
}

// ============================================================================
// traceTicket
// ============================================================================

type traceTicket struct {
	createTime time.Time
	id         uint64
	ids        []objectio.Blockid
	mon        *BlockTracer
}

func (t *traceTicket) ID() uint64 {
	return t.id
}

func (t *traceTicket) Release() error {
	return t.mon.Return(t.id)
}

func (t *traceTicket) String() string {
	return t.stringFormat("")
}

func (t *traceTicket) stringFormat(ident string) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf(
		"%s<Ticket>[ID=%d][TS=%s,Dur=%s][CNT=%d]",
		ident,
		t.id,
		t.createTime.Format("2006-01-02 15:04:05.000"),
		time.Since(t.createTime),
		len(t.ids),
	))
	if len(t.ids) == 0 {
		return w.String()
	}
	_ = w.WriteByte('\n')
	for i, bid := range t.ids {
		_, _ = w.WriteString(fmt.Sprintf(
			"%s%s%d. %s\n",
			ident, ident, i, bid.String(),
		))
	}

	return w.String()
}

type HealthCheckOption struct {
	TTL    time.Duration
	Memory int64
	Disk   int64
}

// ============================================================================
// BlockTracer
// ============================================================================

type BlockTracer struct {
	mu      sync.RWMutex
	index   map[objectio.Blockid]*traceTicket
	tickets map[uint64]*traceTicket
}

func NewBlockTracer() *BlockTracer {
	return &BlockTracer{
		index:   make(map[objectio.Blockid]*traceTicket),
		tickets: make(map[uint64]*traceTicket),
	}
}

func (m *BlockTracer) HealthCheck(opt HealthCheckOption) error {
	if opt.TTL == 0 {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, tic := range m.tickets {
		dur := time.Since(tic.createTime)
		if dur > opt.TTL {
			return moerr.NewInternalErrorNoCtx("ticket %d is active for a long time %s", tic.id, dur)
		}
	}
	return nil
}

func (m *BlockTracer) Query(id *objectio.Blockid) *traceTicket {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.index[*id]
}

func (m *BlockTracer) ReturnByAny(id *objectio.Blockid) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tic, ok := m.index[*id]
	if !ok {
		return moerr.NewInternalErrorNoCtx("blk %s not found", id.String())
	}
	return m.returnLocked(tic.id)
}

func (m *BlockTracer) Return(id uint64) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.returnLocked(id)
}

func (m *BlockTracer) returnLocked(id uint64) (err error) {
	tic, ok := m.tickets[id]
	if !ok {
		return moerr.NewInternalErrorNoCtx("ticket %d not found", id)
	}
	for _, bid := range tic.ids {
		if _, ok := m.index[bid]; !ok {
			return moerr.NewInternalErrorNoCtx("ticket %d blk %s not found", id, bid.String())
		}
	}
	for _, bid := range tic.ids {
		delete(m.index, bid)
	}
	delete(m.tickets, id)
	return
}

func (m *BlockTracer) Apply2(ids ...common.ID) *traceTicket {
	m.mu.Lock()
	defer m.mu.Unlock()
	nids := make([]objectio.Blockid, 0, len(ids))
	for _, id := range ids {
		if _, ok := m.index[id.BlockID]; ok {
			return nil
		}
		nids = append(nids, id.BlockID)
	}
	tic := &traceTicket{
		id:         common.NextGlobalSeqNum(),
		createTime: time.Now(),
		ids:        nids,
		mon:        m,
	}
	for _, id := range ids {
		m.index[id.BlockID] = tic
	}
	m.tickets[tic.id] = tic
	return tic
}

func (m *BlockTracer) Apply(ids ...objectio.Blockid) *traceTicket {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		if _, ok := m.index[id]; ok {
			return nil
		}
	}
	tic := &traceTicket{
		id:         common.NextGlobalSeqNum(),
		createTime: time.Now(),
		ids:        ids,
		mon:        m,
	}
	for _, id := range ids {
		m.index[id] = tic
	}
	m.tickets[tic.id] = tic
	return tic
}

func (m *BlockTracer) String() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return fmt.Sprintf(
		"<BlockTracer>[TIC-CNT=%d][BLK-CNT=%d]",
		len(m.tickets),
		len(m.index),
	)
}

func (m *BlockTracer) PPString() string {
	var w bytes.Buffer
	m.mu.RLock()
	arr := make([]*traceTicket, 0, len(m.tickets))
	for _, tic := range m.tickets {
		arr = append(arr, tic)
	}
	cnt1 := len(m.tickets)
	cnt2 := len(m.index)
	m.mu.RUnlock()
	sort.Slice(arr, func(i, j int) bool {
		return arr[i].createTime.Before(arr[j].createTime)
	})
	_, _ = w.WriteString(fmt.Sprintf(
		"<BlockTracer>[TIC-CNT=%d][BLK-CNT=%d]",
		cnt1, cnt2,
	))
	if cnt1 == 0 {
		return w.String()
	}
	_ = w.WriteByte('\n')
	first := true
	for _, tic := range arr {
		if !first {
			_ = w.WriteByte('\n')
			first = false
		}
		_, _ = w.WriteString(tic.stringFormat("\t"))
	}
	return w.String()
}
