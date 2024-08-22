package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var BitmapPool = fileservice.NewPool(
	256,
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
	bm  *bitmap.FixedSizeBitmap
	put func()
}

func (r *ReusableBitmap) Bitmap() *bitmap.FixedSizeBitmap {
	return r.bm
}

func (r *ReusableBitmap) Release() {
	if r.bm != nil {
		r.bm = nil
	}
	if r.put != nil {
		r.put()
		r.put = nil
	}
}

func (r *ReusableBitmap) OrBitmap(o *bitmap.Bitmap) {
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.bm.OrBitmap(o)
}

func (r *ReusableBitmap) Or(o ReusableBitmap) {
	if o.IsEmpty() {
		return
	}
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.bm.Or(o.bm)
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
	return r.bm.Contains(i)
}

func (r *ReusableBitmap) IsValid() bool {
	return r.bm != nil
}

func GetReusableBitmap() ReusableBitmap {
	var bm *bitmap.FixedSizeBitmap
	put := BitmapPool.Get(&bm)
	return ReusableBitmap{
		bm:  bm,
		put: put.Put,
	}
}

func GetReusableBitmapNoReuse() ReusableBitmap {
	return ReusableBitmap{
		bm: &bitmap.FixedSizeBitmap{},
	}
}
