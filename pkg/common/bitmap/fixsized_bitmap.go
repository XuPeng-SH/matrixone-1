package bitmap

import "math/bits"

func (bm *FixSizedBitmap) Size() int { return FixSizedBitmapBits }

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false
func (bm *FixSizedBitmap) IsEmpty() bool {
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

func (bm *FixSizedBitmap) Reset() {
	if bm.emptyFlag == kEmptyFlagEmpty {
		return
	}
	for i := 0; i < len(bm.data); i++ {
		bm.data[i] = 0
	}
	bm.emptyFlag = kEmptyFlagEmpty
}

func (bm *FixSizedBitmap) Add(row uint64) {
	bm.data[row>>6] |= 1 << (row & 63)
	bm.emptyFlag = kEmptyFlagNotEmpty
}

func (bm *FixSizedBitmap) Remove(row uint64) {
	if row >= FixSizedBitmapBits {
		return
	}
	bm.data[row>>6] &^= 1 << (row & 63)
	bm.emptyFlag = kEmptyFlagUnknown
	return
}

func (bm *FixSizedBitmap) Contains(row uint64) bool {
	if row >= FixSizedBitmapBits {
		return false
	}
	return bm.data[row>>6]&(1<<(row&63)) != 0
}

func (bm *FixSizedBitmap) Count() int {
	if bm.emptyFlag == kEmptyFlagEmpty {
		return 0
	}
	var cnt int
	for i := 0; i < len(bm.data); i++ {
		cnt += bits.OnesCount64(bm.data[i])
	}
	if offset := FixSizedBitmapBits % 64; offset > 0 {
		start := (FixSizedBitmapBits / 64) * 64
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

func (bm *FixSizedBitmap) Iterator() Iterator {
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

func (bm *FixSizedBitmap) Word(i uint64) uint64 {
	return bm.data[i]
}

func (bm *FixSizedBitmap) Len() int64 {
	return FixSizedBitmapBits
}

func (bm *FixSizedBitmap) ToArray() []uint64 {
	return ToArrary[uint64](bm)
}

func (bm *FixSizedBitmap) ToI64Arrary() []int64 {
	return ToArrary[int64](bm)
}

func ToArrary[T int64 | uint64](bm *FixSizedBitmap) (rows []T) {
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
