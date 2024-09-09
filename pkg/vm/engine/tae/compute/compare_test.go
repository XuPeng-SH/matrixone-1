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

package compute

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/assert"
)

func TestCompareGeneric(t *testing.T) {
	defer testutils.AfterTest(t)()
	x := types.Decimal256{
		B0_63: 0, B64_127: 0,
		B128_191: 0, B192_255: 0,
	}
	y := types.Decimal256{
		B0_63: ^x.B0_63, B64_127: ^x.B64_127,
		B128_191: ^x.B128_191, B192_255: ^x.B192_255,
	}
	assert.True(t, CompareGeneric(x, y, types.T_decimal256) == 1)

	t1 := types.TimestampToTS(timestamp.Timestamp{
		PhysicalTime: 100,
		LogicalTime:  10,
	})
	t2 := t1.Next()
	assert.True(t, CompareGeneric(t1, t2, types.T_TS) == -1)

	{
		// Array Float32
		a1 := types.ArrayToBytes[float32]([]float32{1, 2, 3})
		b1 := types.ArrayToBytes[float32]([]float32{1, 2, 3})
		assert.True(t, CompareGeneric(a1, b1, types.T_array_float32) == 0)

		// Array Float64
		a1 = types.ArrayToBytes[float64]([]float64{1, 2, 3})
		b1 = types.ArrayToBytes[float64]([]float64{1, 2, 3})
		assert.True(t, CompareGeneric(a1, b1, types.T_array_float64) == 0)
	}

}

func TestCompareRowId(t *testing.T) {
	s := byte(0xA)
	var a, b []byte

	for i := 0; i < 24; i++ {
		a = append(a, s+byte(i))
		b = append(b, s+byte(i))
	}

	{
		ret1 := Compare(a, b, types.T_Rowid, 0, 0)
		require.Equal(t, 0, ret1)

		ret2 := CompareGeneric(types.Rowid(a), types.Rowid(b), types.T_Rowid)
		require.Equal(t, ret1, ret2)
	}

	{
		x := a[:types.ObjectidSize]
		ret1 := Compare(x, b, types.T_Rowid, 0, 0)
		require.Equal(t, 0, ret1)

		ret1 = Compare(b, x, types.T_Rowid, 0, 0)
		require.Equal(t, 0, ret1)
	}

	{
		x := a[:types.BlockidSize]
		ret := Compare(x, b, types.T_Rowid, 0, 0)
		require.Equal(t, 0, ret)

		ret = Compare(b, x, types.T_Rowid, 0, 0)
		require.Equal(t, 0, ret)
	}

	{
		x := a
		x[rand.Int()%len(x)] += byte(1)
		ret := Compare(x, b, types.T_Rowid, 0, 0)
		require.True(t, ret > 0)
		ret2 := CompareGeneric(types.Rowid(x), types.Rowid(b), types.T_Rowid)
		require.Equal(t, ret2, ret)

		ret = Compare(b, x, types.T_Rowid, 0, 0)
		require.True(t, ret < 0)
		ret2 = CompareGeneric(types.Rowid(b), types.Rowid(x), types.T_Rowid)
		require.Equal(t, ret2, ret)

	}
}

func TestCompareBlockId(t *testing.T) {
	obj := types.NewObjectid()

	var idx1, idx2 int
	for i := 0; i < 100; i++ {
		idx1 = rand.Int() % 999
		idx2 = rand.Int() % 999

		blk1 := types.NewBlockidWithObjectID(obj, uint16(idx1))
		blk2 := types.NewBlockidWithObjectID(obj, uint16(idx2))

		ret := Compare(blk1[:], blk2[:], types.T_Blockid, 0, 0)
		require.Equal(t, idx1 < idx2, ret < 0)

		ret2 := CompareGeneric(*blk1, *blk2, types.T_Blockid)
		require.Equal(t, ret, ret2, fmt.Sprintf("idx1: %d, idx2: %d", idx1, idx2))
	}

}
