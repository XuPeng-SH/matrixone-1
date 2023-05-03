// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func makeColExprForTest(idx int32, typ types.T) *plan.Expr {
	schema := []string{"a", "b", "c", "d"}
	containerType := typ.ToType()
	exprType := plan2.MakePlan2Type(&containerType)

	return &plan.Expr{
		Typ: exprType,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: idx,
				Name:   schema[idx],
			},
		},
	}
}

func makeFunctionExprForTest(name string, args []*plan.Expr) *plan.Expr {
	argTypes := make([]types.Type, len(args))
	for i, arg := range args {
		argTypes[i] = plan2.MakeTypeByPlan2Expr(arg)
	}

	funId, returnType, _, _ := function.GetFunctionByName(context.TODO(), name, argTypes)

	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&returnType),
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     funId,
					ObjName: name,
				},
				Args: args,
			},
		},
	}
}

func TestBlockMetaMarshal(t *testing.T) {
	location := []byte("test")
	var info catalog.BlockInfo
	info.SetMetaLocation(location)
	data := catalog.EncodeBlockInfo(&info)
	info2 := catalog.DecodeBlockInfo(data)
	require.Equal(t, info, *info2)
}

func TestCheckExprIsMonotonic(t *testing.T) {
	type asserts = struct {
		result bool
		expr   *plan.Expr
	}
	testCases := []asserts{
		// a > 1  -> true
		{true, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(10),
		})},
		// a >= b -> true
		{true, makeFunctionExprForTest(">=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		})},
		// abs(a) -> false
		{false, makeFunctionExprForTest("abs", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
		})},
	}

	t.Run("test checkExprIsMonotonic", func(t *testing.T) {
		for i, testCase := range testCases {
			isMonotonic := plan2.CheckExprIsMonotonic(context.TODO(), testCase.expr)
			if isMonotonic != testCase.result {
				t.Fatalf("checkExprIsMonotonic testExprs[%d] is different with expected", i)
			}
		}
	})
}

type testCase struct {
	expr      *plan.Expr
	expect    []bool
	expect64  []int64
	desc      string
	selectAll bool
}

var testCases = []testCase{
	{
		expect: []bool{false, false, true, true, true, true},
		expr: makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		}),
		desc: "a > b",
	},
	{
		expect: []bool{true, true, false, false, false, false},
		expr: makeFunctionExprForTest("<", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		}),
		desc: "a < b",
	},
	{
		expect: []bool{true, true, true, true, true, true},
		expr: makeFunctionExprForTest(">=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		}),
		desc: "a >= b",
	},
	{
		expect: []bool{true, true, true, true, false, false},
		expr: makeFunctionExprForTest("<=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		}),
		desc: "a <= b",
	},
	{
		expect: []bool{true, true, true, true, false, false},
		expr: makeFunctionExprForTest("=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeColExprForTest(1, types.T_int64),
		}),
		desc: "a = b",
	},
	{
		expect: []bool{false, false, true, true, true, true},
		expr: makeFunctionExprForTest(">=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(6),
		}),
		desc: "a >= 6",
	},
	{
		expect: []bool{false, false, false, false, false, false},
		expr: makeFunctionExprForTest(">=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(100),
		}),
		desc: "a >= 100",
	},
	{
		expect64: []int64{6, 3, 4, 7, 7, 10},
		expr: makeFunctionExprForTest("abs", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
		}),
		selectAll: true,
		desc:      "abs(a)",
	},
	{
		expect: []bool{true, true, false, false, true, true},
		expr: makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10000),
			}),
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		desc: "a > 10000 or b > 5",
	},
	{
		expect: []bool{true, true, false, false, false, false},
		expr: makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(5),
			}),
		}),
		desc: "a < 5 and b > 5",
	},
	{
		expect: []bool{false, false, true, true, true, true},
		expr: makeFunctionExprForTest(">", []*plan.Expr{
			makeFunctionExprForTest("+", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				makeColExprForTest(1, types.T_int64),
			}),
			plan2.MakePlan2Int64ConstExprWithType(10),
		}),
		desc: "a + b > 10",
	},
	{
		expect: []bool{true, true, true, true, true, true},
		expr: makeFunctionExprForTest(">=", []*plan.Expr{
			makeFunctionExprForTest("abs", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
			}),
			plan2.MakePlan2Int64ConstExprWithType(5),
		}),
		desc: "abs(a) >= 1",
	},
	{
		expect: []bool{false, false, false, false, true, true},
		expr: makeFunctionExprForTest("=", []*plan.Expr{
			makeColExprForTest(2, types.T_varchar),
			plan2.MakePlan2StringConstExprWithType("5"),
		}),
		desc: "c = \"5\"",
	},
}

func TestEvalFilterExpr1(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	proc := testutil.NewProcessWithMPool(m)
	attrs := []string{"a", "b", "c"}
	bat := batch.NewWithSize(len(attrs))

	vecA := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixedList[int64](vecA,
		[]int64{-6, -3, 4, 7, 7, 10},
		nil, m)
	bat.SetVector(0, vecA)

	vecB := vector.NewVec(types.T_int64.ToType())
	vector.AppendFixedList[int64](vecB,
		[]int64{-3, 7, 1, 4, 2, 6},
		nil, m)
	bat.SetVector(1, vecB)
	bat.SetZs(vecA.Length(), m)

	vecC := vector.NewVec(types.T_varchar.ToType())
	vector.AppendBytesList(vecC,
		[][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5"), []byte("6")},
		nil, m)
	bat.SetVector(2, vecC)
	bat.SetZs(vecC.Length(), m)

	for _, tcase := range testCases {
		outVec, stopped := colexec.EvalFilterExprWithMinMax(context.Background(), tcase.expr, bat, proc)
		require.False(t, stopped)
		if outVec.GetType().Oid == types.T_bool {
			require.Equal(t, tcase.expect, vector.MustFixedCol[bool](outVec))
		} else {
			require.Equal(t, tcase.expect64, vector.MustFixedCol[int64](outVec))
		}
		outVec.Free(m)
	}
	bat.Clean(m)
	t.Log(m.Report())
	require.Equal(t, int64(0), m.CurrNB())
}

func mockZMTestContexts() (def *plan.TableDef, meta objectio.ObjectMeta, vs0, vs1 []int64, vs2 [][]byte) {
	def = &plan.TableDef{
		Cols: []*plan.ColDef{
			&plan.ColDef{
				ColId: 0,
				Name:  "a",
				Typ:   &plan.Type{Id: int32(types.T_int64)},
			},
			&plan.ColDef{
				ColId: 1,
				Name:  "b",
				Typ:   &plan.Type{Id: int32(types.T_int64)},
			},
			&plan.ColDef{
				ColId: 2,
				Name:  "c",
				Typ:   &plan.Type{Id: int32(types.T_varchar)},
			},
		},
	}
	def.Name2ColIndex = make(map[string]int32)
	def.Name2ColIndex[def.Cols[0].Name] = 0
	def.Name2ColIndex[def.Cols[1].Name] = 1
	def.Name2ColIndex[def.Cols[2].Name] = 2
	meta = objectio.BuildMetaData(3, 3)
	vs0 = []int64{-6, -3, 4, 7, 7, 10}
	vs1 = []int64{-3, 7, 1, 4, 2, 6}
	vs2 = [][]byte{[]byte("1"), []byte("2"),
		[]byte("1"), []byte("4"), []byte("4"), []byte("6")}
	for i := 0; i < int(meta.BlockCount()); i++ {
		zm0 := index.NewZM(types.T_int64)
		zm0.Update(vs0[i*2])
		zm0.Update(vs0[i*2+1])
		zm1 := index.NewZM(types.T_int64)
		zm1.Update(vs1[i*2])
		zm1.Update(vs1[i*2+1])
		zm2 := index.NewZM(types.T_varchar)
		zm2.Update(vs2[i*2])
		zm2.Update(vs2[i*2+1])
		meta.GetColumnMeta(uint32(i), 0).SetZoneMap(*zm0)
		meta.GetColumnMeta(uint32(i), 1).SetZoneMap(*zm1)
		meta.GetColumnMeta(uint32(i), 2).SetZoneMap(*zm2)
	}
	return
}

func TestBuildZMVectors(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())

	def, meta, vs0, vs1, vs2 := mockZMTestContexts()

	vecs, err := buildObjectZMVectors(meta, []int{0, 1, 2}, def, m)
	require.NoError(t, err)
	require.Equal(t, vs0, vector.MustFixedCol[int64](vecs[0]))
	require.Equal(t, vs1, vector.MustFixedCol[int64](vecs[1]))
	require.Equal(t, vs2, vector.MustBytesCol(vecs[2]))
}

func TestEvalFilterOnObject(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	proc := testutil.NewProcessWithMPool(m)

	def, meta, _, _, _ := mockZMTestContexts()

	for _, tc := range testCases {
		colMap, defCols, exprCols, maxCol := plan2.GetColumnsByExpr(tc.expr, def)
		sels := filterExprOnObject(
			context.Background(),
			meta,
			tc.expr,
			def,
			colMap,
			defCols,
			exprCols,
			maxCol,
			proc,
		)
		if tc.selectAll {
			require.True(t, sels.IsConst(), tc.desc)
			require.True(t, vector.GetFixedAt[bool](sels, 0))
		} else {
			require.Equal(t, tc.expect, vector.MustFixedCol[bool](sels), tc.desc)
		}
		sels.Free(m)
	}
	t.Log(m.Report())
	require.Zero(t, m.CurrNB())
}

func TestGetNonIntPkValueByExpr(t *testing.T) {
	type asserts = struct {
		result bool
		data   any
		expr   *plan.Expr
		typ    types.T
	}

	testCases := []asserts{
		// a > "a"  false   only 'and', '=' function is supported
		{false, 0, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2StringConstExprWithType("a"),
		}), types.T_int64},
		// a = 100  true
		{true, int64(100),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(100),
			}), types.T_int64},
		// b > 10 and a = "abc"  true
		{true, []byte("abc"),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
			}), types.T_char},
	}

	t.Run("test getPkValueByExpr", func(t *testing.T) {
		for i, testCase := range testCases {
			result, data := getPkValueByExpr(testCase.expr, "a", testCase.typ)
			if result != testCase.result {
				t.Fatalf("test getPkValueByExpr at cases[%d], get result is different with expected", i)
			}
			if result {
				if a, ok := data.([]byte); ok {
					b := testCase.data.([]byte)
					if !bytes.Equal(a, b) {
						t.Fatalf("test getPkValueByExpr at cases[%d], data is not match", i)
					}
				} else {
					if data != testCase.data {
						t.Fatalf("test getPkValueByExpr at cases[%d], data is not match", i)
					}
				}
			}
		}
	})
}

func TestEvalZonemapFilter(t *testing.T) {
	m := mpool.MustNewNoFixed(t.Name())
	proc := testutil.NewProcessWithMPool(m)
	type myCase = struct {
		exprs  []*plan.Expr
		meta   objectio.BlockObject
		desc   []string
		expect []bool
	}

	zm0 := index.NewZM(types.T_float64)
	zm0.Update(float64(-10))
	zm0.Update(float64(20))
	zm1 := index.NewZM(types.T_float64)
	zm1.Update(float64(5))
	zm1.Update(float64(25))
	zm2 := index.NewZM(types.T_varchar)
	zm2.Update([]byte("abc"))
	zm2.Update([]byte("opq"))
	zm3 := index.NewZM(types.T_varchar)
	zm3.Update([]byte("efg"))
	zm3.Update(index.MaxBytesValue)
	cases := []myCase{
		{
			desc: []string{
				"a>10", "a>30", "a<=-10", "a<-10", "a+b>60", "a+b<-5", "a-b<-34", "a-b<-35", "a-b<=-35", "a>b",
				"a>b+15", "a>=b+15", "a>100 or b>10", "a>100 and b<0", "d>xyz", "d<=efg", "d<efg", "c>d", "c<d",
			},
			exprs: []*plan.Expr{
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(10),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(30),
				}),
				makeFunctionExprForTest("<=", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(-10),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					plan2.MakePlan2Float64ConstExprWithType(-10),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeFunctionExprForTest("+", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(60),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeFunctionExprForTest("+", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-5),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeFunctionExprForTest("-", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-34),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeFunctionExprForTest("-", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-35),
				}),
				makeFunctionExprForTest("<=", []*plan.Expr{
					makeFunctionExprForTest("-", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						makeColExprForTest(1, types.T_float64),
					}),
					plan2.MakePlan2Float64ConstExprWithType(-35),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					makeColExprForTest(1, types.T_float64),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					makeFunctionExprForTest("+", []*plan.Expr{
						makeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(15),
					}),
				}),
				makeFunctionExprForTest(">=", []*plan.Expr{
					makeColExprForTest(0, types.T_float64),
					makeFunctionExprForTest("+", []*plan.Expr{
						makeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(15),
					}),
				}),
				makeFunctionExprForTest("or", []*plan.Expr{
					makeFunctionExprForTest(">", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(100),
					}),
					makeFunctionExprForTest(">", []*plan.Expr{
						makeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(10),
					}),
				}),
				makeFunctionExprForTest("and", []*plan.Expr{
					makeFunctionExprForTest(">", []*plan.Expr{
						makeColExprForTest(0, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(100),
					}),
					makeFunctionExprForTest("<", []*plan.Expr{
						makeColExprForTest(1, types.T_float64),
						plan2.MakePlan2Float64ConstExprWithType(0),
					}),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(3, types.T_varchar),
					plan2.MakePlan2StringConstExprWithType("xyz"),
				}),
				makeFunctionExprForTest("<=", []*plan.Expr{
					makeColExprForTest(3, types.T_varchar),
					plan2.MakePlan2StringConstExprWithType("efg"),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeColExprForTest(3, types.T_varchar),
					plan2.MakePlan2StringConstExprWithType("efg"),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(2, types.T_varchar),
					makeColExprForTest(3, types.T_varchar),
				}),
				makeFunctionExprForTest("<", []*plan.Expr{
					makeColExprForTest(2, types.T_varchar),
					makeColExprForTest(3, types.T_varchar),
				}),
			},
			meta: func() objectio.BlockObject {
				objMeta := objectio.BuildMetaData(1, 4)
				meta := objMeta.GetBlockMeta(0)
				meta.MustGetColumn(0).SetZoneMap(*zm0)
				meta.MustGetColumn(1).SetZoneMap(*zm1)
				meta.MustGetColumn(2).SetZoneMap(*zm2)
				meta.MustGetColumn(3).SetZoneMap(*zm3)
				return meta
			}(),
			expect: []bool{
				true, false, true, false, false, false, true, false, true, true,
				false, true, true, false, true, true, false, true, true,
			},
		},
	}

	columnMap := map[int]int{0: 0, 1: 1, 2: 2, 3: 3}

	for _, tc := range cases {
		for i, expr := range tc.exprs {
			zm := filterExprOnBlock(context.Background(), tc.meta, expr, columnMap, proc)
			require.Equal(t, tc.expect[i], zm, tc.desc[i])
		}
	}
	require.Zero(t, m.CurrNB())
}

func TestComputeRangeByNonIntPk(t *testing.T) {
	type asserts = struct {
		result bool
		data   uint64
		expr   *plan.Expr
	}

	getHash := func(e *plan.Expr) uint64 {
		_, ret := getConstantExprHashValue(context.TODO(), e, testutil.NewProc())
		return ret
	}

	testCases := []asserts{
		// a > "a"  false   only 'and', '=' function is supported
		{false, 0, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2StringConstExprWithType("a"),
		})},
		// a > coalesce("a")  false,  the second arg must be constant
		{false, 0, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeFunctionExprForTest("coalesce", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2StringConstExprWithType("a"),
			}),
		})},
		// a = "abc"  true
		{true, getHash(plan2.MakePlan2StringConstExprWithType("abc")),
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2StringConstExprWithType("abc"),
			})},
		// a = "abc" and b > 10  true
		{true, getHash(plan2.MakePlan2StringConstExprWithType("abc")),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
			})},
		// b > 10 and a = "abc"  true
		{true, getHash(plan2.MakePlan2StringConstExprWithType("abc")),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
			})},
		// a = "abc" or b > 10  false
		{false, 0,
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
			})},
		// a = "abc" or a > 10  false
		{false, 0,
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2StringConstExprWithType("abc"),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(10),
				}),
			})},
	}

	t.Run("test computeRangeByNonIntPk", func(t *testing.T) {
		for i, testCase := range testCases {
			result, data := computeRangeByNonIntPk(context.TODO(), testCase.expr, "a", testutil.NewProc())
			if result != testCase.result {
				t.Fatalf("test computeRangeByNonIntPk at cases[%d], get result is different with expected", i)
			}
			if result {
				if data != testCase.data {
					t.Fatalf("test computeRangeByNonIntPk at cases[%d], data is not match", i)
				}
			}
		}
	})
}

func TestComputeRangeByIntPk(t *testing.T) {
	type asserts = struct {
		result bool
		items  []int64
		expr   *plan.Expr
	}

	testCases := []asserts{
		// a > abs(20)   not support now
		{false, []int64{21}, makeFunctionExprForTest("like", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			makeFunctionExprForTest("abs", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
		})},
		// a > 20
		{true, []int64{}, makeFunctionExprForTest(">", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(20),
		})},
		// a > 20 and b < 1  is equal a > 20
		{false, []int64{}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
		})},
		// 1 < b and a > 20   is equal a > 20
		{false, []int64{}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(1),
				makeColExprForTest(1, types.T_int64),
			}),
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
		})},
		// a > 20 or b < 1  false.
		{false, []int64{}, makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(1, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(1),
			}),
		})},
		// a = 20
		{true, []int64{20}, makeFunctionExprForTest("=", []*plan.Expr{
			makeColExprForTest(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(20),
		})},
		// a > 20 and a < =25
		{true, []int64{21, 22, 23, 24, 25}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(25),
			}),
		})},
		// a > 20 and a <=25 and b > 100   todo： unsupport now。  when compute a <=25 and b > 10, we get items too much.
		{false, []int64{21, 22, 23, 24, 25}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("and", []*plan.Expr{
				makeFunctionExprForTest("<=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(25),
				}),
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(1, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(100),
				}),
			}),
		})},
		// a > 20 and a < 10  => empty
		{false, []int64{}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest(">", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(10),
			}),
		})},
		// a < 20 or 100 < a
		{false, []int64{}, makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("<", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(20),
			}),
			makeFunctionExprForTest("<", []*plan.Expr{
				plan2.MakePlan2Int64ConstExprWithType(100),
				makeColExprForTest(0, types.T_int64),
			}),
		})},
		// a =1 or a = 2 or a=30
		{true, []int64{2, 1, 30}, makeFunctionExprForTest("or", []*plan.Expr{
			makeFunctionExprForTest("=", []*plan.Expr{
				makeColExprForTest(0, types.T_int64),
				plan2.MakePlan2Int64ConstExprWithType(2),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(1),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(30),
				}),
			}),
		})},
		// (a >5 or a=1) and (a < 8 or a =11) => 1,6,7,11  todo,  now can't compute now
		{false, []int64{6, 7, 11, 1}, makeFunctionExprForTest("and", []*plan.Expr{
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest(">", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(5),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(1),
				}),
			}),
			makeFunctionExprForTest("or", []*plan.Expr{
				makeFunctionExprForTest("<", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(8),
				}),
				makeFunctionExprForTest("=", []*plan.Expr{
					makeColExprForTest(0, types.T_int64),
					plan2.MakePlan2Int64ConstExprWithType(11),
				}),
			}),
		})},
	}

	t.Run("test computeRangeByIntPk", func(t *testing.T) {
		for i, testCase := range testCases {
			result, data := computeRangeByIntPk(testCase.expr, "a", "")
			if result != testCase.result {
				t.Fatalf("test computeRangeByIntPk at cases[%d], get result is different with expected", i)
			}
			if result {
				if len(data.items) != len(testCase.items) {
					t.Fatalf("test computeRangeByIntPk at cases[%d], data length is not match", i)
				}
				for j, val := range testCase.items {
					if data.items[j] != val {
						t.Fatalf("test computeRangeByIntPk at cases[%d], data[%d] is not match", i, j)
					}
				}
			}
		}
	})
}

// func TestGetListByRange(t *testing.T) {
// 	type asserts = struct {
// 		result []DNStore
// 		list   []DNStore
// 		r      [][2]int64
// 	}

// 	testCases := []asserts{
// 		{[]DNStore{{UUID: "1"}, {UUID: "2"}}, []DNStore{{UUID: "1"}, {UUID: "2"}}, [][2]int64{{14, 32324234234234}}},
// 		{[]DNStore{{UUID: "1"}}, []DNStore{{UUID: "1"}, {UUID: "2"}}, [][2]int64{{14, 14}}},
// 	}

// 	t.Run("test getListByRange", func(t *testing.T) {
// 		for i, testCase := range testCases {
// 			result := getListByRange(testCase.list, testCase.r)
// 			if len(result) != len(testCase.result) {
// 				t.Fatalf("test getListByRange at cases[%d], data length is not match", i)
// 			}
// 			/*
// 				for j, r := range testCase.result {
// 					if r.UUID != result[j].UUID {
// 						t.Fatalf("test getListByRange at cases[%d], result[%d] is not match", i, j)
// 					}
// 				}
// 			*/
// 		}
// 	})
// }
