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

package compute

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func CompareOrdered[T types.OrderedT](a, b T) int64 {
	if a > b {
		return 1
	} else if a < b {
		return -1
	}
	return 0
}

func CompareBool(a, b bool) int64 {
	if a && b {
		return 0
	} else if !a && !b {
		return 0
	} else if a {
		return 1
	}
	return 0
}

func CompareBytes(a, b []byte) int64 {
	res := bytes.Compare(a, b)
	if res > 0 {
		return 1
	} else if res < 0 {
		return -1
	} else {
		return 0
	}
}

func Compare(a, b []byte, t types.T, scale1, scale2 int32) int64 {
	switch t {
	case types.T_bool:
		return CompareBool(types.DecodeBool(a), types.DecodeBool(b))
	case types.T_int8:
		return CompareOrdered(types.DecodeInt8(a), types.DecodeInt8(b))
	case types.T_int16:
		return CompareOrdered(types.DecodeInt16(a), types.DecodeInt16(b))
	case types.T_int32:
		return CompareOrdered(types.DecodeInt32(a), types.DecodeInt32(b))
	case types.T_int64:
		return CompareOrdered(types.DecodeInt64(a), types.DecodeInt64(b))
	case types.T_uint8:
		return CompareOrdered(types.DecodeUint8(a), types.DecodeUint8(b))
	case types.T_uint16:
		return CompareOrdered(types.DecodeUint16(a), types.DecodeUint16(b))
	case types.T_uint32:
		return CompareOrdered(types.DecodeUint32(a), types.DecodeUint32(b))
	case types.T_uint64:
		return CompareOrdered(types.DecodeUint64(a), types.DecodeUint64(b))
	case types.T_decimal64:
		return types.CompareDecimal64WithScale(types.DecodeDecimal64(a), types.DecodeDecimal64(b), scale1, scale2)
	case types.T_decimal128:
		return types.CompareDecimal128WithScale(types.DecodeDecimal128(a), types.DecodeDecimal128(b), scale1, scale2)
	case types.T_decimal256:
		return types.CompareDecimal256(*(*types.Decimal256)(unsafe.Pointer(&a[0])), *(*types.Decimal256)(unsafe.Pointer(&b[0])))
	case types.T_float32:
		return CompareOrdered(types.DecodeFloat32(a), types.DecodeFloat32(b))
	case types.T_float64:
		return CompareOrdered(types.DecodeFloat64(a), types.DecodeFloat64(b))
	case types.T_timestamp:
		return CompareOrdered(types.DecodeTimestamp(a), types.DecodeTimestamp(b))
	case types.T_date:
		return CompareOrdered(types.DecodeDate(a), types.DecodeDate(b))
	case types.T_time:
		return CompareOrdered(types.DecodeTime(a), types.DecodeTime(b))
	case types.T_datetime:
		return CompareOrdered(types.DecodeDatetime(a), types.DecodeDatetime(b))
	case types.T_TS:
		return CompareBytes(a, b)
	case types.T_Rowid:
		return CompareBytes(a, b)
	case types.T_uuid:
		return types.CompareUuid(types.DecodeUuid(a), types.DecodeUuid(b))
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_json, types.T_text:
		return CompareBytes(a, b)
	case types.T_any:
		return 0
	default:
		panic(fmt.Sprintf("unsupported type: %v", t))
	}
}

func CompareGeneric(a, b any, t types.T) int64 {
	switch t {
	case types.T_bool:
		return CompareBool(a.(bool), b.(bool))
	case types.T_int8:
		return CompareOrdered[int8](a.(int8), b.(int8))
	case types.T_int16:
		return CompareOrdered[int16](a.(int16), b.(int16))
	case types.T_int32:
		return CompareOrdered[int32](a.(int32), b.(int32))
	case types.T_int64:
		return CompareOrdered[int64](a.(int64), b.(int64))
	case types.T_uint8:
		return CompareOrdered[uint8](a.(uint8), b.(uint8))
	case types.T_uint16:
		return CompareOrdered[uint16](a.(uint16), b.(uint16))
	case types.T_uint32:
		return CompareOrdered[uint32](a.(uint32), b.(uint32))
	case types.T_uint64:
		return CompareOrdered[uint64](a.(uint64), b.(uint64))
	case types.T_decimal64:
		return int64(a.(types.Decimal64).Compare(b.(types.Decimal64)))
	case types.T_decimal128:
		return int64(a.(types.Decimal128).Compare(b.(types.Decimal128)))
	case types.T_decimal256:
		return int64(a.(types.Decimal256).Compare(b.(types.Decimal256)))
	case types.T_float32:
		return CompareOrdered[float32](a.(float32), b.(float32))
	case types.T_float64:
		return CompareOrdered[float64](a.(float64), b.(float64))
	case types.T_timestamp:
		return CompareOrdered[types.Timestamp](a.(types.Timestamp), b.(types.Timestamp))
	case types.T_date:
		return CompareOrdered[types.Date](a.(types.Date), b.(types.Date))
	case types.T_time:
		return CompareOrdered[types.Time](a.(types.Time), b.(types.Time))
	case types.T_datetime:
		return CompareOrdered[types.Datetime](a.(types.Datetime), b.(types.Datetime))
	case types.T_TS:
		return int64(a.(types.TS).Compare(b.(types.TS)))
	case types.T_Rowid:
		return CompareBytes(a.([]byte), b.([]byte))
	case types.T_Blockid:
		return CompareBytes(a.([]byte), b.([]byte))
	case types.T_uuid:
		return types.CompareUuid(a.(types.Uuid), b.(types.Uuid))
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_json, types.T_text:
		return CompareBytes(a.([]byte), b.([]byte))
	case types.T_any:
		return 0
	default:
		panic(fmt.Sprintf("unsupported type: %s", t.String()))
	}
}
