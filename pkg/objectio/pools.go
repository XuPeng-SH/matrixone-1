package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

var BitmapPool = fileservice.NewPool(
	256,
	func() *bitmap.FixSizedBitmap {
		var bm bitmap.FixSizedBitmap
		return &bm
	},
	func(bm *bitmap.FixSizedBitmap) {
		bm.Reset()
	},
	nil,
)
