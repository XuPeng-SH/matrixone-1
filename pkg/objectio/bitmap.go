package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

var BitmapPool = fileservice.NewPool(
	128,
	func() *bitmap.FixedSizeBitmap {
		var bm bitmap.FixedSizeBitmap
		return &bm
	},
	func(bm *bitmap.FixedSizeBitmap) {
		bm.Reset()
	},
	nil,
)

var NullReusableBitmap ReusableBitmap

type ReusableBitmap struct {
	bm  bitmap.ISimpleBitmap
	put func()
	idx int
}

func (r *ReusableBitmap) Idx() int {
	return r.idx
}

func (r *ReusableBitmap) Release() {
	if r.bm != nil {
		r.bm = nil
	}
	if r.put != nil {
		r.put()
		r.put = nil
	}
	r.idx = 0
}

func (r *ReusableBitmap) OrSimpleBitmap(o bitmap.ISimpleBitmap) {
	if o.IsEmpty() {
		return
	}
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.tryCowOrExpand(int(o.Len()))
	r.bm.OrSimpleBitmap(o)
}

func (r *ReusableBitmap) Reusable() bool {
	return r.put != nil
}

func (r *ReusableBitmap) Or(o ReusableBitmap) {
	if o.IsEmpty() {
		return
	}
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.tryCowOrExpand(int(o.bm.Len()))
	r.bm.OrSimpleBitmap(o.bm)
}

func (r *ReusableBitmap) SafeAdd(i uint64) {
	r.bm.SafeAdd(i)
}

func (r *ReusableBitmap) Add(i uint64) {
	if r == nil || r.bm == nil {
		logutil.Fatal("invalid bitmap")
	}
	r.tryCowOrExpand(int(i) + 1)
	r.bm.Add(i)
}

func (r *ReusableBitmap) tryCowOrExpand(nbits int) {
	if r.bm.IsFixedSize() {
		if nbits > bitmap.FixedSizeBitmapBits {
			logutil.Warn(
				"ReusableBitmap-COW",
				zap.Int("nbits", nbits),
			)
			var nbm bitmap.Bitmap
			nbm.TryExpandWithSize(nbits)
			nbm.OrSimpleBitmap(r.bm)
			r.Release()
			r.bm = &nbm
		}
	} else {
		r.bm.TryExpandWithSize(nbits)
	}
}

func (r *ReusableBitmap) ToI64Array() []int64 {
	if r.IsEmpty() {
		return nil
	}
	return r.bm.ToI64Array()
}

func (r *ReusableBitmap) ToArray() []uint64 {
	if r.IsEmpty() {
		return nil
	}
	return r.bm.ToArray()
}

func (r *ReusableBitmap) IsEmpty() bool {
	return r.bm == nil || r.bm.IsEmpty()
}

func (r *ReusableBitmap) Reset() {
	if r.bm != nil {
		r.bm.Reset()
	}
}

func (r *ReusableBitmap) Count() int {
	if r.bm == nil {
		return 0
	}
	return r.bm.Count()
}

func (r *ReusableBitmap) Contains(i uint64) bool {
	if r.bm == nil {
		return false
	}
	if i >= bitmap.FixedSizeBitmapBits && r.bm.IsFixedSize() {
		return false
	}
	return r.bm.Contains(i)
}

func (r *ReusableBitmap) IsValid() bool {
	return r != nil && r.bm != nil
}

func GetReusableBitmap() ReusableBitmap {
	var bm *bitmap.FixedSizeBitmap
	put := BitmapPool.Get(&bm)
	return ReusableBitmap{
		bm:  bm,
		put: put.Put,
		idx: put.Idx(),
	}
}

func GetReusableBitmapNoReuse() ReusableBitmap {
	return ReusableBitmap{
		bm: &bitmap.FixedSizeBitmap{},
	}
}
