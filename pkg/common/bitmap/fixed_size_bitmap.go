package bitmap

import (
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func (bm *FixedSizeBitmap) Size() int { return FixedSizeBitmapBits }

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false
func (bm *FixedSizeBitmap) IsEmpty() bool {
	if bm.emptyFlag == kEmptyFlagEmpty {
		return true
	}
	for _, v := range bm.data {
		if v != 0 {
			bm.emptyFlag = kEmptyFlagNotEmpty
			return false
		}
	}
	bm.emptyFlag = kEmptyFlagEmpty
	return true
}

func (bm *FixedSizeBitmap) Reset() {
	if bm.emptyFlag == kEmptyFlagEmpty {
		return
	}
	for i := 0; i < len(bm.data); i++ {
		bm.data[i] = 0
	}
	bm.emptyFlag = kEmptyFlagEmpty
}

func (bm *FixedSizeBitmap) Add(row uint64) {
	if row >= FixedSizeBitmapBits {
		logutil.Fatalf("row %d is out of range", row)
	}
	bm.data[row>>6] |= 1 << (row & 63)
	bm.emptyFlag = kEmptyFlagNotEmpty
}

func (bm *FixedSizeBitmap) Remove(row uint64) {
	if row >= FixedSizeBitmapBits {
		logutil.Fatalf("row %d is out of range", row)
	}
	bm.data[row>>6] &^= 1 << (row & 63)
	bm.emptyFlag = kEmptyFlagUnknown
	return
}

func (bm *FixedSizeBitmap) Contains(row uint64) bool {
	if row >= FixedSizeBitmapBits {
		logutil.Fatalf("row %d is out of range", row)
	}
	return bm.data[row>>6]&(1<<(row&63)) != 0
}

func (bm *FixedSizeBitmap) Count() int {
	if bm.emptyFlag == kEmptyFlagEmpty {
		return 0
	}
	var cnt int
	for i := 0; i < len(bm.data); i++ {
		cnt += bits.OnesCount64(bm.data[i])
	}
	if offset := FixedSizeBitmapBits % 64; offset > 0 {
		start := (FixedSizeBitmapBits / 64) * 64
		for i, j := start, start+offset; i < j; i++ {
			if bm.Contains(uint64(i)) {
				cnt++
			}
		}
	}
	if cnt == 0 {
		bm.emptyFlag = kEmptyFlagEmpty
	} else {
		bm.emptyFlag = kEmptyFlagNotEmpty
	}
	return cnt
}

func (bm *FixedSizeBitmap) Iterator() Iterator {
	it := BitmapIterator{
		bm: bm,
		i:  0,
	}
	if first_1_pos, has_next := it.hasNext(0); has_next {
		it.i = first_1_pos
		it.has_next = true
		return &it
	}
	it.has_next = false

	return &it
}

func (bm *FixedSizeBitmap) OrBitmap(o *Bitmap) {
	if o.IsEmpty() {
		return
	}
	if o.Len() > FixedSizeBitmapBits {
		logutil.Fatalf("bitmap length %d is out of range", o.Len())
	}
	wordsCnt := int(o.Len()) / 64
	empty := true
	for i := 0; i < wordsCnt; i++ {
		bm.data[i] |= o.data[i]
		if bm.data[i] != 0 {
			empty = false
		}
	}
	if empty {
		bm.emptyFlag = FixedSizeBitmap_Empty
	} else {
		bm.emptyFlag = FixedSizeBitmap_NotEmpty
	}
}

func (bm *FixedSizeBitmap) Or(o *FixedSizeBitmap) {
	empty := true
	for i := 0; i < len(bm.data); i++ {
		bm.data[i] |= o.data[i]
		if bm.data[i] != 0 {
			empty = false
		}
	}
	if empty {
		bm.emptyFlag = FixedSizeBitmap_Empty
	} else {
		bm.emptyFlag = FixedSizeBitmap_NotEmpty
	}
}

func (bm *FixedSizeBitmap) Word(i uint64) uint64 {
	return bm.data[i]
}

func (bm *FixedSizeBitmap) Len() int64 {
	return FixedSizeBitmapBits
}

func (bm *FixedSizeBitmap) ToArray() []uint64 {
	return ToArrary[uint64](bm)
}

func (bm *FixedSizeBitmap) ToI64Arrary() []int64 {
	return ToArrary[int64](bm)
}

func ToArrary[T int64 | uint64](bm *FixedSizeBitmap) (rows []T) {
	if bm.IsEmpty() {
		return
	}
	rows = make([]T, 0, bm.Count())
	it := bm.Iterator()
	for it.HasNext() {
		rows = append(rows, T(it.Next()))
	}
	return
}
