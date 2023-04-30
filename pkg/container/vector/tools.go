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

package vector

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"golang.org/x/exp/constraints"
)

func MustFixedCol[T any](v *Vector) []T {
	// XXX hack.   Sometimes we generate an t_any, for untyped const null.
	// This should be handled more carefully and gracefully.
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	if v.class == CONSTANT {
		return v.col.([]T)[:1]
	}
	return v.col.([]T)[:v.length]
}

func MustBytesCol(v *Vector) [][]byte {
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	varcol := MustFixedCol[types.Varlena](v)
	if v.class == CONSTANT {
		return [][]byte{(&varcol[0]).GetByteSlice(v.area)}
	} else {
		ret := make([][]byte, v.length)
		for i := range varcol {
			ret[i] = (&varcol[i]).GetByteSlice(v.area)
		}
		return ret
	}
}

func MustStrCol(v *Vector) []string {
	if v.GetType().Oid == types.T_any || len(v.data) == 0 {
		return nil
	}
	varcol := MustFixedCol[types.Varlena](v)
	if v.class == CONSTANT {
		return []string{(&varcol[0]).GetString(v.area)}
	} else {
		ret := make([]string, v.length)
		for i := range varcol {
			ret[i] = (&varcol[i]).GetString(v.area)
		}
		return ret
	}
}

// ExpandFixedCol decode data and return decoded []T.
// For const/scalar vector we expand and return newly allocated slice.
func ExpandFixedCol[T any](v *Vector) []T {
	if v.IsConst() {
		vs := make([]T, v.Length())
		if len(v.data) > 0 {
			cols := v.col.([]T)
			for i := range vs {
				vs[i] = cols[0]
			}
		}
		return vs
	}
	return MustFixedCol[T](v)
}

func ExpandStrCol(v *Vector) []string {
	if v.IsConst() {
		vs := make([]string, v.Length())
		if len(v.data) > 0 {
			cols := v.col.([]types.Varlena)
			ss := cols[0].GetString(v.area)
			for i := range vs {
				vs[i] = ss
			}
		}
		return vs
	}
	return MustStrCol(v)
}

func ExpandBytesCol(v *Vector) [][]byte {
	if v.IsConst() {
		vs := make([][]byte, v.Length())
		if len(v.data) > 0 {
			cols := v.col.([]types.Varlena)
			ss := cols[0].GetByteSlice(v.area)
			for i := range vs {
				vs[i] = ss
			}
		}
		return vs
	}
	return MustBytesCol(v)
}

func MustVarlenaRawData(v *Vector) (data []types.Varlena, area []byte) {
	data = MustFixedCol[types.Varlena](v)
	area = v.area
	return
}

//func FromDNVector(typ types.Type, header []types.Varlena, storage []byte, cantFree bool) (vec *Vector, err error) {
//	vec = NewVec(typ)
//	vec.cantFreeData = cantFree
//	vec.cantFreeArea = cantFree
//	if typ.IsString() {
//		if len(header) > 0 {
//			vec.col = header
//			vec.data = unsafe.Slice((*byte)(unsafe.Pointer(&header[0])), typ.TypeSize()*cap(header))
//			vec.area = storage
//			vec.capacity = cap(header)
//			vec.length = len(header)
//		}
//	} else {
//		if len(storage) > 0 {
//			vec.data = storage
//			vec.length = len(storage) / typ.TypeSize()
//			vec.setupColFromData()
//		}
//	}
//	return
//}

// XXX extend will extend the vector's Data to accommodate rows more entry.
func extend(v *Vector, rows int, m *mpool.MPool) error {
	sz := v.typ.TypeSize()
	if tgtCap := v.length + rows; tgtCap > v.capacity {
		ndata, err := m.Grow(v.data, tgtCap*sz)
		if err != nil {
			return err
		}
		v.data = ndata[:cap(ndata)]
		v.setupColFromData()
	}
	v.data = v.data[:(v.length+rows)*sz]
	return nil
}

func (v *Vector) setupColFromData() {
	if v.GetType().IsVarlen() {
		v.col = DecodeFixedCol[types.Varlena](v)
	} else {
		// The followng switch attach the correct type to v.col
		// even though v.col is only an interface.
		switch v.typ.Oid {
		case types.T_bool:
			v.col = DecodeFixedCol[bool](v)
		case types.T_int8:
			v.col = DecodeFixedCol[int8](v)
		case types.T_int16:
			v.col = DecodeFixedCol[int16](v)
		case types.T_int32:
			v.col = DecodeFixedCol[int32](v)
		case types.T_int64:
			v.col = DecodeFixedCol[int64](v)
		case types.T_uint8:
			v.col = DecodeFixedCol[uint8](v)
		case types.T_uint16:
			v.col = DecodeFixedCol[uint16](v)
		case types.T_uint32:
			v.col = DecodeFixedCol[uint32](v)
		case types.T_uint64:
			v.col = DecodeFixedCol[uint64](v)
		case types.T_float32:
			v.col = DecodeFixedCol[float32](v)
		case types.T_float64:
			v.col = DecodeFixedCol[float64](v)
		case types.T_decimal64:
			v.col = DecodeFixedCol[types.Decimal64](v)
		case types.T_decimal128:
			v.col = DecodeFixedCol[types.Decimal128](v)
		case types.T_uuid:
			v.col = DecodeFixedCol[types.Uuid](v)
		case types.T_date:
			v.col = DecodeFixedCol[types.Date](v)
		case types.T_time:
			v.col = DecodeFixedCol[types.Time](v)
		case types.T_datetime:
			v.col = DecodeFixedCol[types.Datetime](v)
		case types.T_timestamp:
			v.col = DecodeFixedCol[types.Timestamp](v)
		case types.T_TS:
			v.col = DecodeFixedCol[types.TS](v)
		case types.T_Rowid:
			v.col = DecodeFixedCol[types.Rowid](v)
		case types.T_Blockid:
			v.col = DecodeFixedCol[types.Blockid](v)
		default:
			panic("unknown type")
		}
	}
	tlen := v.GetType().TypeSize()
	v.capacity = cap(v.data) / tlen
}

func VectorToProtoVector(vec *Vector) (*api.Vector, error) {
	nsp, err := vec.nsp.Show()
	if err != nil {
		return nil, err
	}
	sz := vec.typ.TypeSize()
	return &api.Vector{
		Nsp:      nsp,
		Nullable: true,
		Area:     vec.area,
		IsConst:  vec.IsConst(),
		Len:      uint32(vec.length),
		Type:     TypeToProtoType(vec.typ),
		Data:     vec.data[:vec.length*sz],
	}, nil
}

func ProtoVectorToVector(vec *api.Vector) (*Vector, error) {
	rvec := &Vector{
		area:         vec.Area,
		length:       int(vec.Len),
		typ:          ProtoTypeToType(vec.Type),
		cantFreeData: true,
		cantFreeArea: true,
	}
	if vec.IsConst {
		rvec.class = CONSTANT
	} else {
		rvec.class = FLAT
	}
	rvec.nsp = &nulls.Nulls{}
	if err := rvec.nsp.Read(vec.Nsp); err != nil {
		return nil, err
	}
	if rvec.IsConst() && rvec.nsp.Contains(0) {
		rvec.nsp = &nulls.Nulls{}
		return rvec, nil
	}
	rvec.data = vec.Data
	rvec.setupColFromData()
	return rvec, nil
}

func TypeToProtoType(typ types.Type) *plan.Type {
	return &plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

func ProtoTypeToType(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}

// CompareAndCheckIntersect  we use this method for eval expr by zonemap
func (v *Vector) CompareAndCheckIntersect(vec *Vector) (bool, error) {
	switch v.typ.Oid {
	case types.T_int8:
		return checkNumberIntersect[int8](v, vec)
	case types.T_int16:
		return checkNumberIntersect[int16](v, vec)
	case types.T_int32:
		return checkNumberIntersect[int32](v, vec)
	case types.T_int64:
		return checkNumberIntersect[int64](v, vec)
	case types.T_uint8:
		return checkNumberIntersect[uint8](v, vec)
	case types.T_uint16:
		return checkNumberIntersect[uint16](v, vec)
	case types.T_uint32:
		return checkNumberIntersect[uint32](v, vec)
	case types.T_uint64:
		return checkNumberIntersect[uint64](v, vec)
	case types.T_float32:
		return checkNumberIntersect[float32](v, vec)
	case types.T_float64:
		return checkNumberIntersect[float64](v, vec)
	case types.T_date:
		return checkNumberIntersect[types.Date](v, vec)
	case types.T_time:
		return checkNumberIntersect[types.Time](v, vec)
	case types.T_datetime:
		return checkNumberIntersect[types.Datetime](v, vec)
	case types.T_timestamp:
		return checkNumberIntersect[types.Timestamp](v, vec)
	case types.T_decimal64:
		return checkGeneralIntersect(v, vec, func(t1, t2 types.Decimal64) bool {
			return (t1.Compare(t2) >= 0)
		}, func(t1, t2 types.Decimal64) bool {
			return (t1.Compare(t2) <= 0)
		})
	case types.T_decimal128:
		return checkGeneralIntersect(v, vec, func(t1, t2 types.Decimal128) bool {
			return (t1.Compare(t2) >= 0)
		}, func(t1, t2 types.Decimal128) bool {
			return (t1.Compare(t2) <= 0)
		})
	case types.T_uuid:
		return checkGeneralIntersect(v, vec, func(t1, t2 types.Uuid) bool {
			return t1.Ge(t2)
		}, func(t1, t2 types.Uuid) bool {
			return t1.Le(t2)
		})
	case types.T_varchar, types.T_binary, types.T_varbinary, types.T_char, types.T_text:
		return checkStrIntersect(v, vec, func(t1, t2 string) bool {
			return strings.Compare(t1, t2) >= 0
		}, func(t1, t2 string) bool {
			return strings.Compare(t1, t2) <= 0
		})
	}
	return false, moerr.NewInternalErrorNoCtx("unsupport type to check intersect")
}

func checkNumberIntersect[T constraints.Integer | constraints.Float | types.Date | types.Datetime | types.Timestamp](v1, v2 *Vector) (bool, error) {
	cols1 := MustFixedCol[T](v1)
	cols2 := MustFixedCol[T](v2)
	return checkIntersect(cols1, cols2, func(i1, i2 T) bool {
		return i1 >= i2
	}, func(i1, i2 T) bool {
		return i1 <= i2
	})
}

func checkStrIntersect(v1, v2 *Vector, gtFun compFn[string], ltFun compFn[string]) (bool, error) {
	cols1 := MustStrCol(v1)
	cols2 := MustStrCol(v2)
	return checkIntersect(cols1, cols2, gtFun, ltFun)
}

func checkGeneralIntersect[T compT](v1, v2 *Vector, gtFun compFn[T], ltFun compFn[T]) (bool, error) {
	cols1 := MustFixedCol[T](v1)
	cols2 := MustFixedCol[T](v2)
	return checkIntersect(cols1, cols2, gtFun, ltFun)
}

func checkIntersect[T compT](cols1, cols2 []T, gtFun compFn[T], ltFun compFn[T]) (bool, error) {
	// get v1's min/max
	colLength := len(cols1)
	min := cols1[0]
	max := cols1[0]
	for i := 1; i < colLength; i++ {
		// cols1[i] <= min
		if ltFun(cols1[i], min) {
			min = cols1[i]
		} else if gtFun(cols1[i], max) {
			// cols1[i] >= max
			max = cols1[i]
		}
	}

	// check v2 if some item >= min && <= max
	for i := 0; i < len(cols2); i++ {
		// cols2[i] >= min && cols2[i] <= max
		if gtFun(cols2[i], min) && ltFun(cols2[i], max) {
			return true, nil
		}
	}
	return false, nil
}

// CompareAndCheckAnyResultIsTrue  we use this method for eval expr by zonemap
// funName must be ">,<,>=,<="
func (v *Vector) CompareAndCheckAnyResultIsTrue(ctx context.Context, vec *Vector, funName string) (bool, error) {
	if v.typ.Oid != vec.typ.Oid {
		return false, moerr.NewInternalErrorNoCtx("can not compare two vector because their type is not match")
	}
	if v.Length() != vec.Length() {
		return false, moerr.NewInternalErrorNoCtx("can not compare two vector because their length is not match")
	}
	if v.Length() == 0 {
		return false, moerr.NewInternalErrorNoCtx("can not compare two vector because their length is zero")
	}

	switch funName {
	case ">", "<", ">=", "<=":
	default:
		return false, moerr.NewInternalErrorNoCtx("unsupport compare function")
	}

	switch v.typ.Oid {
	case types.T_int8:
		return compareNumber[int8](ctx, v, vec, funName)
	case types.T_int16:
		return compareNumber[int16](ctx, v, vec, funName)
	case types.T_int32:
		return compareNumber[int32](ctx, v, vec, funName)
	case types.T_int64:
		return compareNumber[int64](ctx, v, vec, funName)
	case types.T_uint8:
		return compareNumber[uint8](ctx, v, vec, funName)
	case types.T_uint16:
		return compareNumber[uint16](ctx, v, vec, funName)
	case types.T_uint32:
		return compareNumber[uint32](ctx, v, vec, funName)
	case types.T_uint64:
		return compareNumber[uint64](ctx, v, vec, funName)
	case types.T_float32:
		return compareNumber[float32](ctx, v, vec, funName)
	case types.T_float64:
		return compareNumber[float64](ctx, v, vec, funName)
	case types.T_date:
		return compareNumber[types.Date](ctx, v, vec, funName)
	case types.T_time:
		return compareNumber[types.Time](ctx, v, vec, funName)
	case types.T_datetime:
		return compareNumber[types.Datetime](ctx, v, vec, funName)
	case types.T_timestamp:
		return compareNumber[types.Timestamp](ctx, v, vec, funName)
	case types.T_decimal64:
		switch funName {
		case ">":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Compare(t2) > 0
			}), nil
		case "<":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Compare(t2) < 0
			}), nil
		case ">=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Compare(t2) >= 0
			}), nil
		case "<=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal64) bool {
				return t1.Compare(t2) <= 0
			}), nil
		}
	case types.T_decimal128:
		switch funName {
		case ">":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Compare(t2) > 0
			}), nil
		case "<":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Compare(t2) < 0
			}), nil
		case ">=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Compare(t2) >= 0
			}), nil
		case "<=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Decimal128) bool {
				return t1.Compare(t2) <= 0
			}), nil
		}
	case types.T_uuid:
		switch funName {
		case ">":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Uuid) bool {
				return t1.Gt(t2)
			}), nil
		case "<":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Uuid) bool {
				return t1.Lt(t2)
			}), nil
		case ">=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Uuid) bool {
				return t1.Ge(t2)
			}), nil
		case "<=":
			return runCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 types.Uuid) bool {
				return t1.Le(t2)
			}), nil
		}
	case types.T_varchar, types.T_binary, types.T_varbinary, types.T_char:
		switch funName {
		case ">":
			return runStrCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 string) bool {
				return strings.Compare(t1, t2) == 1
			}), nil
		case "<":
			return runStrCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 string) bool {
				return strings.Compare(t1, t2) == -1
			}), nil
		case ">=":
			return runStrCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 string) bool {
				return strings.Compare(t1, t2) >= 0
			}), nil
		case "<=":
			return runStrCompareCheckAnyResultIsTrue(v, vec, func(t1, t2 string) bool {
				return strings.Compare(t1, t2) <= 0
			}), nil
		}
	default:
		return false, moerr.NewInternalErrorNoCtx("unsupport compare type")
	}

	return false, moerr.NewInternalErrorNoCtx("unsupport compare function")
}

type compT interface {
	constraints.Integer | constraints.Float | types.Decimal64 | types.Decimal128 |
		types.Date | types.Time | types.Datetime | types.Timestamp | types.Uuid | string
}

type compFn[T compT] func(T, T) bool
type numberType interface {
	constraints.Integer | constraints.Float | types.Date | types.Time | types.Datetime | types.Timestamp
}

func compareNumber[T numberType](ctx context.Context, v1, v2 *Vector, fnName string) (bool, error) {
	switch fnName {
	case ">":
		return runCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 T) bool {
			return t1 > t2
		}), nil
	case "<":
		return runCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 T) bool {
			return t1 < t2
		}), nil
	case ">=":
		return runCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 T) bool {
			return t1 >= t2
		}), nil
	case "<=":
		return runCompareCheckAnyResultIsTrue(v1, v2, func(t1, t2 T) bool {
			return t1 <= t2
		}), nil
	default:
		return false, moerr.NewInternalErrorNoCtx("unsupport compare function")
	}
}

func runCompareCheckAnyResultIsTrue[T compT](vec1, vec2 *Vector, fn compFn[T]) bool {
	// column_a operator column_b  -> return true
	// that means we don't known the return, just readBlock
	if vec1.IsConstNull() || vec2.IsConstNull() {
		return true
	}
	if nulls.Any(vec1.nsp) || nulls.Any(vec2.nsp) {
		return true
	}
	cols1 := MustFixedCol[T](vec1)
	cols2 := MustFixedCol[T](vec2)
	return compareCheckAnyResultIsTrue(cols1, cols2, fn)
}

func runStrCompareCheckAnyResultIsTrue(vec1, vec2 *Vector, fn compFn[string]) bool {
	// column_a operator column_b  -> return true
	// that means we don't known the return, just readBlock
	if vec1.IsConstNull() || vec2.IsConstNull() {
		return true
	}
	if nulls.Any(vec1.nsp) || nulls.Any(vec2.nsp) {
		return true
	}

	cols1 := MustStrCol(vec1)
	cols2 := MustStrCol(vec2)
	return compareCheckAnyResultIsTrue(cols1, cols2, fn)
}

func compareCheckAnyResultIsTrue[T compT](cols1, cols2 []T, fn compFn[T]) bool {
	for i := 0; i < len(cols1); i++ {
		for j := 0; j < len(cols2); j++ {
			if fn(cols1[i], cols2[j]) {
				return true
			}
		}
	}
	return false
}

func appendBytesToFixSized[T types.FixedSizeT](vec *Vector) func([]byte, bool, *mpool.MPool) error {
	return func(buf []byte, isNull bool, mp *mpool.MPool) (err error) {
		v := types.DecodeFixed[T](buf)
		return AppendFixed(vec, v, isNull, mp)
	}
}

func MakeAppendBytesFunc(vec *Vector) func([]byte, bool, *mpool.MPool) error {
	t := vec.GetType()
	if t.IsVarlen() {
		return func(v []byte, isNull bool, mp *mpool.MPool) (err error) {
			return AppendBytes(vec, v, isNull, mp)
		}
	}
	switch t.Oid {
	case types.T_bool:
		return appendBytesToFixSized[bool](vec)
	case types.T_int8:
		return appendBytesToFixSized[int8](vec)
	case types.T_int16:
		return appendBytesToFixSized[int16](vec)
	case types.T_int32:
		return appendBytesToFixSized[int32](vec)
	case types.T_int64:
		return appendBytesToFixSized[int64](vec)
	case types.T_uint8:
		return appendBytesToFixSized[uint8](vec)
	case types.T_uint16:
		return appendBytesToFixSized[uint16](vec)
	case types.T_uint32:
		return appendBytesToFixSized[uint32](vec)
	case types.T_uint64:
		return appendBytesToFixSized[uint64](vec)
	case types.T_float32:
		return appendBytesToFixSized[float32](vec)
	case types.T_float64:
		return appendBytesToFixSized[float64](vec)
	case types.T_date:
		return appendBytesToFixSized[types.Date](vec)
	case types.T_datetime:
		return appendBytesToFixSized[types.Datetime](vec)
	case types.T_time:
		return appendBytesToFixSized[types.Time](vec)
	case types.T_timestamp:
		return appendBytesToFixSized[types.Timestamp](vec)
	case types.T_decimal64:
		return appendBytesToFixSized[types.Decimal64](vec)
	case types.T_decimal128:
		return appendBytesToFixSized[types.Decimal128](vec)
	case types.T_uuid:
		return appendBytesToFixSized[types.Uuid](vec)
	case types.T_TS:
		return appendBytesToFixSized[types.TS](vec)
	case types.T_Rowid:
		return appendBytesToFixSized[types.Rowid](vec)
	case types.T_Blockid:
		return appendBytesToFixSized[types.Blockid](vec)
	}
	panic(fmt.Sprintf("unexpected type: %s", vec.GetType().String()))
}

func genericCompareMinMax[T any](
	col1, col2 []T,
	nsp1, nsp2 *nulls.Nulls,
	isV1Const, isV2Const bool,
	result *Vector,
	comp func(T, T, T, T) bool,
	m *mpool.MPool,
) (err error) {
	var v1Minv, v1Maxv, v2Minv, v2Maxv T
	for i := 0; i < len(col1); i += 2 {
		var v bool
		if nsp1.Contains(uint64(i)) || nsp2.Contains(uint64(i)) {
			v = true
		} else {
			if isV1Const {
				v1Minv = col1[0]
				v1Maxv = v1Minv
			} else {
				v1Minv = col1[i]
				v1Maxv = col1[i+1]
			}
			if isV2Const {
				v2Minv = col2[0]
				v2Maxv = v2Minv
			} else {
				v2Minv = col2[i]
				v2Maxv = col2[i+1]
			}
			v = comp(v1Minv, v1Maxv, v2Minv, v2Maxv)
		}
		if err = AppendFixedList[bool](result, []bool{v, v}, nil, m); err != nil {
			return
		}
	}
	return
}

func compareBytesMinMax(
	v1, v2, result *Vector,
	compType uint8,
	m *mpool.MPool,
) (err error) {
	data1, area1 := MustVarlenaRawData(v1)
	data2, area2 := MustVarlenaRawData(v2)
	var comp func([]byte, []byte, []byte, []byte) bool
	switch compType {
	case 0: /* '>'  */
		comp = func(_, v1Maxv, v2Minv, _ []byte) bool {
			return bytes.Compare(v1Maxv, v2Minv) > 0
		}
	case 1: /* '<'  */
		comp = func(v1Minv, _, _, v2Maxv []byte) bool {
			return bytes.Compare(v1Minv, v2Maxv) < 0
		}
	case 2: /* '>=' */
		comp = func(_, v1Maxv, v2Minv, _ []byte) bool {
			return bytes.Compare(v1Maxv, v2Minv) >= 0
		}
	case 3: /* '<=' */
		comp = func(v1Minv, _, _, v2Maxv []byte) bool {
			return bytes.Compare(v1Minv, v2Maxv) <= 0
		}
	case 4: /* '==' */
		comp = func(v1Minv, v1Maxv, v2Minv, v2Maxv []byte) bool {
			return bytes.Compare(v1Maxv, v2Minv) >= 0 && bytes.Compare(v1Minv, v2Maxv) <= 0
		}
	default:
		err = moerr.NewInternalErrorNoCtx("unsupport compare type: %d", compType)
	}
	isV1Const := v1.IsConst()
	isV2Const := v2.IsConst()
	var (
		v1Minv, v1Maxv []byte
		v2Minv, v2Maxv []byte
	)
	for i := 0; i < len(data1); i += 2 {
		var v bool
		if v1.nsp.Contains(uint64(i)) || v2.nsp.Contains(uint64(i)) {
			v = true
		} else {
			if isV1Const {
				v1Minv = data1[0].GetByteSlice(area1)
				v1Maxv = v1Minv
			} else {
				v1Minv, v1Maxv = data1[i].GetByteSlice(area1), data1[i+1].GetByteSlice(area1)
			}
			if isV2Const {
				v2Minv = data2[0].GetByteSlice(area2)
				v2Maxv = v2Minv
			} else {
				v2Minv, v2Maxv = data2[i].GetByteSlice(area2), data2[i+1].GetByteSlice(area2)
			}
			v = comp(v1Minv, v1Maxv, v2Minv, v2Maxv)
		}
		if err = AppendFixedList[bool](result, []bool{v, v}, nil, m); err != nil {
			return
		}
	}
	return
}

func compareFixeSizedMinMax[T types.FixedSizeT](
	v1, v2, result *Vector,
	elemComp func(T, T) int64,
	compType uint8,
	m *mpool.MPool,
) (err error) {
	col1 := MustFixedCol[T](v1)
	col2 := MustFixedCol[T](v2)
	var comp func(T, T, T, T) bool
	switch compType {
	case 0: /* '>'  */
		comp = func(_, v1Maxv, v2Minv, _ T) bool {
			return elemComp(v1Maxv, v2Minv) > 0
		}
	case 1: /* '<'  */
		comp = func(v1Minv, _, _, v2Maxv T) bool {
			return elemComp(v1Minv, v2Maxv) < 0
		}
	case 2: /* '>=' */
		comp = func(_, v1Maxv, v2Minv, _ T) bool {
			return elemComp(v1Maxv, v2Minv) >= 0
		}
	case 3: /* '<=' */
		comp = func(v1Minv, _, _, v2Maxv T) bool {
			return elemComp(v1Minv, v2Maxv) <= 0
		}
	case 4: /* '==' */
		comp = func(v1Minv, v1Maxv, v2Minv, v2Maxv T) bool {
			return elemComp(v1Maxv, v2Minv) >= 0 && elemComp(v1Minv, v2Maxv) <= 0
		}
	default:
		err = moerr.NewInternalErrorNoCtx("unsupport compare type: %d", compType)
	}
	err = genericCompareMinMax(
		col1, col2,
		v1.nsp, v2.nsp,
		v1.IsConst(), v2.IsConst(),
		result,
		comp,
		m)
	return
}

func compareOrderedMinMax[T types.OrderedT](
	v1, v2, result *Vector,
	compType uint8,
	m *mpool.MPool,
) (err error) {
	col1 := MustFixedCol[T](v1)
	col2 := MustFixedCol[T](v2)

	var (
		v1GetMin, v1GetMax, v2GetMin, v2GetMax func([]T, int) T
	)
	if v1.IsConst() {
		v1GetMin = func(vs []T, _ int) T {
			return vs[0]
		}
		v1GetMax = v1GetMin
	} else {
		v1GetMin = func(vs []T, i int) T {
			return vs[i]
		}
		v1GetMax = func(vs []T, i int) T {
			return vs[i+1]
		}
	}
	if v2.IsConst() {
		v2GetMin = func(vs []T, _ int) T {
			return vs[0]
		}
		v2GetMax = v2GetMin
	} else {
		v2GetMin = func(vs []T, i int) T {
			return vs[i]
		}
		v2GetMax = func(vs []T, i int) T {
			return vs[i+1]
		}
	}
	switch compType {
	case 0: /* '>'  */
		for i := 0; i < v1.Length(); i += 2 {
			var v bool
			if v1.nsp.Contains(uint64(i)) || v2.nsp.Contains(uint64(i)) {
				v = true
			} else {
				v1Maxv := v1GetMax(col1, i)
				v2Minv := v1GetMin(col2, i)
				v = v1Maxv > v2Minv
			}
			if err = AppendFixedList[bool](result, []bool{v, v}, nil, m); err != nil {
				return
			}
		}
	case 1: /* '<'  */
		for i := 0; i < v1.Length(); i += 2 {
			var v bool
			if v1.nsp.Contains(uint64(i)) || v2.nsp.Contains(uint64(i)) {
				v = true
			} else {
				v1Minv := v1GetMin(col1, i)
				v2Maxv := v2GetMax(col2, i)
				v = v1Minv < v2Maxv
			}
			if err = AppendFixedList(result, []bool{v, v}, nil, m); err != nil {
				return
			}
		}
	case 2: /* '>=' */
		for i := 0; i < v1.Length(); i += 2 {
			var v bool
			if v1.nsp.Contains(uint64(i)) || v2.nsp.Contains(uint64(i)) {
				v = true
			} else {
				v1Maxv := v1GetMax(col1, i)
				v2Minv := v2GetMin(col2, i)
				v = v1Maxv >= v2Minv
			}
			if err = AppendFixedList(result, []bool{v, v}, nil, m); err != nil {
				return
			}
		}
	case 3: /* '<=' */
		for i := 0; i < v1.Length(); i += 2 {
			var v bool
			if v1.nsp.Contains(uint64(i)) || v2.nsp.Contains(uint64(i)) {
				v = true
			} else {
				v1Minv := v1GetMin(col1, i)
				v2Maxv := v2GetMax(col2, i)
				v = v1Minv <= v2Maxv
			}
			if err = AppendFixedList(result, []bool{v, v}, nil, m); err != nil {
				return
			}
		}
	case 4: /* '==' */
		for i := 0; i < v1.Length(); i += 2 {
			var v bool
			if v1.nsp.Contains(uint64(i)) || v2.nsp.Contains(uint64(i)) {
				v = true
			} else {
				v1Minv, v1Maxv := v1GetMin(col1, i), v1GetMax(col1, i)
				v2Minv, v2Maxv := v2GetMin(col2, i), v2GetMax(col2, i)
				v = v1Maxv >= v2Minv && v1Minv <= v2Maxv
			}
			if err = AppendFixedList(result, []bool{v, v}, nil, m); err != nil {
				return
			}
		}
	default:
		err = moerr.NewInternalErrorNoCtx("unsupport compare type: %d", compType)
	}
	return
}

// Two consecutive rows are pairs of data, where even rows are min
// and odd rows are max
//
// [minv1,maxv1,minv2,maxv2,...,minvn,maxvn]
//
// CompType: compType
// 0 - '>'
// 1 - '<'
// 2 - '>='
// 3 - '<='
// 4 - '=='
//
// Compare Operators: v1,v2
// v1 >  v2:  v1.maxv >  v2.minv
// v1 <  v2:  v1.minv <  v2.maxv
// v1 >= v2:  v1.maxv >= v2.minv
// v1 <= v2:  v1.minv <= v2.maxv
// v1 == v2:  v1.maxv >= v2.minv && v1.minv <= v2.maxv
//
// Result: result
// result is of type bool vector with length v1.Length()
// the result of two consecutive rows should be always same.
func CompareWithMinMax(
	v1, v2, result *Vector,
	compType uint8,
	m *mpool.MPool,
) (err error) {
	t := v1.GetType()
	if t.Oid != v2.GetType().Oid {
		return moerr.NewInternalErrorNoCtx("can not compare two vector because their type is not match")
	}
	if v1.Length() != v2.Length() {
		return moerr.NewInternalErrorNoCtx("can not compare two vector because their length is not match")
	}

	if v1.IsConstNull() || v2.IsConstNull() {
		err = SetConstFixed[bool](result, true, v1.Length(), m)
		return
	}

	if t.IsVarlen() {
		return compareBytesMinMax(v1, v2, result, compType, m)
	}
	switch t.Oid {
	case types.T_int8:
		return compareOrderedMinMax[int8](v1, v2, result, compType, m)
	case types.T_int16:
		return compareOrderedMinMax[int16](v1, v2, result, compType, m)
	case types.T_int32:
		return compareOrderedMinMax[int32](v1, v2, result, compType, m)
	case types.T_int64:
		return compareOrderedMinMax[int64](v1, v2, result, compType, m)
	case types.T_uint8:
		return compareOrderedMinMax[uint8](v1, v2, result, compType, m)
	case types.T_uint16:
		return compareOrderedMinMax[uint16](v1, v2, result, compType, m)
	case types.T_uint32:
		return compareOrderedMinMax[uint32](v1, v2, result, compType, m)
	case types.T_uint64:
		return compareOrderedMinMax[uint64](v1, v2, result, compType, m)
	case types.T_decimal64:
		return compareFixeSizedMinMax[types.Decimal64](v1, v2, result, types.CompareDecimal64, compType, m)
	case types.T_decimal128:
		return compareFixeSizedMinMax[types.Decimal128](v1, v2, result, types.CompareDecimal128, compType, m)
	case types.T_decimal256:
		return compareFixeSizedMinMax[types.Decimal256](v1, v2, result, types.CompareDecimal256, compType, m)
	case types.T_float32:
		return compareOrderedMinMax[float32](v1, v2, result, compType, m)
	case types.T_float64:
		return compareOrderedMinMax[float64](v1, v2, result, compType, m)
	case types.T_timestamp:
		return compareOrderedMinMax[types.Timestamp](v1, v2, result, compType, m)
	case types.T_date:
		return compareOrderedMinMax[types.Date](v1, v2, result, compType, m)
	case types.T_time:
		return compareOrderedMinMax[types.Time](v1, v2, result, compType, m)
	case types.T_datetime:
		return compareOrderedMinMax[types.Datetime](v1, v2, result, compType, m)
	case types.T_uuid:
		return compareFixeSizedMinMax[types.Uuid](v1, v2, result, types.CompareUuid, compType, m)
	default:
		err = moerr.NewInternalErrorNoCtx("unsupport vector type \"%v\" to compare min max", t)
	}
	return
}
