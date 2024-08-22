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

var NullReusableFixedSizeBitmap ReusableFixedSizeBitmap

type ReusableFixedSizeBitmap struct {
	bm  *bitmap.FixedSizeBitmap
	put func()
}

func (r *ReusableFixedSizeBitmap) Bitmap() *bitmap.FixedSizeBitmap {
	return r.bm
}

func (r *ReusableFixedSizeBitmap) Release() {
	if r.bm != nil {
		r.bm = nil
	}
	if r.put != nil {
		r.put()
		r.put = nil
	}
}

func (r *ReusableFixedSizeBitmap) OrBitmap(o *bitmap.Bitmap) {
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.bm.OrBitmap(o)
}

func (r *ReusableFixedSizeBitmap) Or(o ReusableFixedSizeBitmap) {
	if o.IsEmpty() {
		return
	}
	if !r.IsValid() {
		logutil.Fatal("invalid bitmap")
	}
	r.bm.Or(o.bm)
}

func (r *ReusableFixedSizeBitmap) IsEmpty() bool {
	return r.bm == nil || r.bm.IsEmpty()
}

func (r *ReusableFixedSizeBitmap) Contains(i uint64) bool {
	if r.bm == nil {
		return false
	}
	return r.bm.Contains(i)
}

func (r *ReusableFixedSizeBitmap) IsValid() bool {
	return r.bm != nil
}

func GetReusableFixedSizeBitmap() ReusableFixedSizeBitmap {
	var bm *bitmap.FixedSizeBitmap
	put := BitmapPool.Get(&bm)
	return ReusableFixedSizeBitmap{
		bm:  bm,
		put: put.Put,
	}
}

func GetFixedSizeBitmap() ReusableFixedSizeBitmap {
	return ReusableFixedSizeBitmap{
		bm: &bitmap.FixedSizeBitmap{},
	}
}
