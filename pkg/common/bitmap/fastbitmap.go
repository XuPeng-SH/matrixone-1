package bitmap

import "math/bits"

func (bm *FastBitmap) Size() int { return FastBitmapBits }

// IsEmpty returns true if no bit in the Bitmap is set, otherwise it will return false
func (bm *FastBitmap) IsEmpty() bool {
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

func (bm *FastBitmap) Reset() {
	if bm.emptyFlag == kEmptyFlagEmpty {
		return
	}
	for i := 0; i < len(bm.data); i++ {
		bm.data[i] = 0
	}
	bm.emptyFlag = kEmptyFlagEmpty
}

func (bm *FastBitmap) Add(row uint64) {
	bm.data[row>>6] |= 1 << (row & 63)
	bm.emptyFlag = kEmptyFlagNotEmpty
}

func (bm *FastBitmap) Remove(row uint64) {
	if row >= FastBitmapBits {
		return
	}
	bm.data[row>>6] &^= 1 << (row & 63)
	bm.emptyFlag = kEmptyFlagUnknown
	return
}

func (bm *FastBitmap) Contains(row uint64) bool {
	if row >= FastBitmapBits {
		return false
	}
	return bm.data[row>>6]&(1<<(row&63)) != 0
}

func (bm *FastBitmap) Count() int {
	if bm.emptyFlag == kEmptyFlagEmpty {
		return 0
	}
	var cnt int
	for i := 0; i < len(bm.data); i++ {
		cnt += bits.OnesCount64(bm.data[i])
	}
	if offset := FastBitmapBits % 64; offset > 0 {
		start := (FastBitmapBits / 64) * 64
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
