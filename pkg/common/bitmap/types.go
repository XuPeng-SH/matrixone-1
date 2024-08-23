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
	FixedSizeBitmapBits     = 8192
	FixedSizeBitmapBytes    = FixedSizeBitmapBits / 8
	FixedSizeBitmapTypeSize = unsafe.Sizeof(FixedSizeBitmap{})
)

type ISimpleBitmap interface {
	IBitmapData
	IsEmpty() bool
	Add(uint64)
	SafeAdd(uint64)
	Contains(uint64) bool
	Count() int
	ToArray() []uint64
	ToI64Array() []int64
	OrSimpleBitmap(ISimpleBitmap)
	IsFixedSize() bool
	Reset()
	TryExpandWithSize(size int)
}

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
// 1. FixedSizeBitmap is fixed size: 8192 bits
// 2. Reset only clear data
// 3. Cannot expand
type FixedSizeBitmap struct {
	emptyFlag int8
	data      [FixedSizeBitmapBytes / 8]uint64
}
