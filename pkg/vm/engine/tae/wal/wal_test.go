package wal

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAEWAL"
)

func TestCheckpoint(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	cfg := &store.StoreCfg{
		RotateChecker: store.NewMaxSizeRotateChecker(int(common.K) * 2),
	}
	driver := NewDriver(dir, "store", cfg)

	var bs bytes.Buffer
	for i := 0; i < 300; i++ {
		bs.WriteString("helloyou")
	}
	buf := bs.Bytes()

	e := entry.GetBase()
	e.SetType(entry.ETCustomizedStart)
	buf2 := make([]byte, common.K)
	copy(buf2, buf)
	e.Unmarshal(buf2)
	lsn, err := driver.AppendEntry(GroupC, e)
	assert.Nil(t, err)
	err = e.WaitDone()
	assert.Nil(t, err)
	_, err = driver.LoadEntry(GroupC, lsn)
	assert.Nil(t, err)

	flush := entry.GetBase()
	flush.SetType(entry.ETCustomizedStart)
	buf3 := make([]byte, common.K*3/2)
	copy(buf3, buf)
	flush.Unmarshal(buf3)
	l, err := driver.AppendEntry(GroupC+1, flush)
	assert.Nil(t, err)
	assert.Equal(t,uint64(1),l)
	err = flush.WaitDone()
	assert.Nil(t, err)

	index := []*Index{{
		LSN:  lsn,
		CSN:  0,
		Size: 1,
	}}
	ckp, err := driver.Checkpoint(index)
	assert.Nil(t, err)
	ckp.WaitDone()

	flush2 := entry.GetBase()
	flush2.SetType(entry.ETCustomizedStart)
	buf4 := make([]byte, common.K*3/2)
	copy(buf4, buf)
	err=flush2.Unmarshal(buf4)
	assert.Nil(t, err)
	l, err = driver.AppendEntry(GroupC+1, flush2)
	assert.Nil(t, err)
	assert.Equal(t,uint64(2),l)
	err = flush2.WaitDone()
	assert.Nil(t, err)

	err = driver.Compact()
	assert.Nil(t, err)
	_, err = driver.LoadEntry(GroupC, lsn)
	assert.NotNil(t, err)
}
