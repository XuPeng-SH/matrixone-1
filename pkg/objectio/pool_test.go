package objectio

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/stretchr/testify/require"
)

func TestReusableBitmap(t *testing.T) {
	require.Equal(t, 0, BitmapPool.InUse())

	bm1 := GetReusableBitmap()
	require.True(t, bm1.Reusable())
	require.Equal(t, 1, BitmapPool.InUse())

	bm1.Add(bitmap.FixedSizeBitmapBits)
	require.False(t, bm1.Reusable())
	require.Equal(t, 0, BitmapPool.InUse())
	require.True(t, bm1.Contains(bitmap.FixedSizeBitmapBits))

	bm2 := GetReusableBitmap()
	require.True(t, bm2.Reusable())
	require.Equal(t, 1, BitmapPool.InUse())

	bm2.Or(bm1)
	require.False(t, bm2.Reusable())
	require.Equal(t, 0, BitmapPool.InUse())
	require.True(t, bm2.Contains(bitmap.FixedSizeBitmapBits))
	require.Equal(t, bm1.Count(), bm2.Count())
	require.Equal(t, 1, bm1.Count())
	require.Equal(t, []uint64{bitmap.FixedSizeBitmapBits}, bm1.ToArray())
	require.Equal(t, []uint64{bitmap.FixedSizeBitmapBits}, bm2.ToArray())
	require.Equal(t, []int64{bitmap.FixedSizeBitmapBits}, bm1.ToI64Array())
}
