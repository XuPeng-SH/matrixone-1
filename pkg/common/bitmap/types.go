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

package bitmap

import (
	"sync/atomic"
	"unsafe"
)

type Iterator interface {
	HasNext() bool
	Next() uint64
	PeekNext() uint64
}

const (
	kEmptyFlagEmpty    = 1
	kEmptyFlagNotEmpty = -1
	kEmptyFlagUnknown  = 0
)

const (
	// Why define FastBitmap_Empty|FastBitmap_NotEmpty|FastBitmap_Unknown?
	// when Reset the FastBitmap, it is expected the whole buffer is reset to all zero,
	// but the kEmptyFlagEmpty defined by bitmap.Bitmap is -1, which is not zero.
	// We cannot change the kEmptyFlagEmpty to 0, because it is persisted in the storage.

	// For example:
	// var buf []byte
	// put := bufPool.Get(&buf) // get buf from pool and buf is all zero
	// defer bufPool.Put(put) // put back to pool
	// bm := types.DecodeFixed[bitmap.FastBitmap](buf) // bm.IsEmpty() should be true
	// bm.Reset() // reset all bytes to zero

	FastBitmap_Empty    = 0
	FastBitmap_NotEmpty = -1
	FastBitmap_Unknown  = 1
	FastBitmapBits      = 8192
	FastBitmapBytes     = FastBitmapBits / 8
	FastBitmapTypeSize  = unsafe.Sizeof(FastBitmap{})
)

type IBitmapData interface {
	Word(i uint64) uint64
	Len() int64
}

// Bitmap represents line numbers of tuple's is null
type Bitmap struct {
	emptyFlag atomic.Int32 //default 0, not sure  when set to 1, must be empty. when set to -1, must be not empty
	// len represents the size of bitmap
	len  int64
	data []uint64
}

type BitmapIterator struct {
	i        uint64
	bm       IBitmapData
	has_next bool
}

// compared with Bitmap
// 1. FastBitmap is fixed size: 8192 bits
// 2. Reset only clear data
// 3. Cannot expand
type FastBitmap struct {
	emptyFlag int8
	data      [FastBitmapBytes / 8]uint64
}
