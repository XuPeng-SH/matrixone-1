// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type FastFilterOp func(objectio.ObjectStats) (bool, error)
type LoadOp = func(
	context.Context, objectio.ObjectStats, objectio.ObjectMeta, objectio.BloomFilter,
) (objectio.ObjectMeta, objectio.BloomFilter, error)
type ObjectFilterOp func(objectio.ObjectMeta, objectio.BloomFilter) (bool, error)
type SeekFirstBlockOp func(objectio.ObjectDataMeta) int
type BlockFilterOp func(int, objectio.BlockObject, objectio.BloomFilter) (bool, bool, error)
type LoadOpFactory func(fileservice.FileService) LoadOp

var loadMetadataOnlyOpFactory LoadOpFactory
var loadMetadataAndBFOpFactory LoadOpFactory

func init() {
	loadMetadataAndBFOpFactory = func(fs fileservice.FileService) LoadOp {
		return func(
			ctx context.Context,
			obj objectio.ObjectStats,
			inMeta objectio.ObjectMeta,
			inBF objectio.BloomFilter,
		) (outMeta objectio.ObjectMeta, outBF objectio.BloomFilter, err error) {
			location := obj.ObjectLocation()
			outMeta = inMeta
			if outMeta == nil {
				if outMeta, err = objectio.FastLoadObjectMeta(
					ctx, &location, false, fs,
				); err != nil {
					return nil, nil, err
				}
			}
			outBF = inBF
			if outBF == nil {
				meta := outMeta.MustDataMeta()
				if outBF, err = objectio.LoadBFWithMeta(
					ctx, meta, location, fs,
				); err != nil {
					return nil, nil, err
				}
			}
			return outMeta, outBF, nil
		}
	}
	loadMetadataOnlyOpFactory = func(fs fileservice.FileService) LoadOp {
		return func(
			ctx context.Context,
			obj objectio.ObjectStats,
			inMeta objectio.ObjectMeta,
			inBF objectio.BloomFilter,
		) (outMeta objectio.ObjectMeta, outBF objectio.BloomFilter, err error) {
			outMeta = inMeta
			outBF = inBF
			if outMeta != nil {
				return
			}
			location := obj.ObjectLocation()
			if outMeta, err = objectio.FastLoadObjectMeta(
				ctx, &location, false, fs,
			); err != nil {
				return nil, nil, err
			}
			return outMeta, outBF, nil
		}
	}
}

func isSortedKey(colDef *plan.ColDef) (isPK, isSorted bool) {
	isPK, isCluster := colDef.Primary, colDef.ClusterBy
	isSorted = isPK || isCluster
	return
}

func getConstBytesFromExpr(exprs []*plan.Expr, colDef *plan.ColDef, proc *process.Process) ([][]byte, bool) {
	vals := make([][]byte, len(exprs))
	for idx := range exprs {
		constVal := getConstValueByExpr(exprs[idx], proc)
		if constVal == nil {
			return nil, false
		}
		colType := types.T(colDef.Typ.Id)
		val, ok := evalLiteralExpr2(constVal, colType)
		if !ok {
			return nil, ok
		}

		vals[idx] = val
	}

	return vals, true
}

func mustColVecValueFromBinaryFuncExpr(
	expr *plan.Expr_F, tableDef *plan.TableDef, proc *process.Process,
) (*plan.Expr_Col, []byte, bool) {
	var (
		colExpr *plan.Expr_Col
		valExpr *plan.Expr
		ok      bool
	)
	if colExpr, ok = expr.F.Args[0].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[1]
	} else if colExpr, ok = expr.F.Args[1].Expr.(*plan.Expr_Col); ok {
		valExpr = expr.F.Args[0]
	} else {
		return nil, nil, false
	}

	switch exprImpl := valExpr.Expr.(type) {
	case *plan.Expr_Vec:
		return colExpr, exprImpl.Vec.Data, true
	}
	return nil, nil, false
}

func mustColConstValueFromBinaryFuncExpr(
	expr *plan.Expr_F, tableDef *plan.TableDef, proc *process.Process,
) (*plan.Expr_Col, [][]byte, bool) {
	var (
		colExpr  *plan.Expr_Col
		tmpExpr  *plan.Expr_Col
		valExprs []*plan.Expr
		ok       bool
	)

	for idx := range expr.F.Args {
		if tmpExpr, ok = expr.F.Args[idx].Expr.(*plan.Expr_Col); !ok {
			valExprs = append(valExprs, expr.F.Args[idx])
		} else {
			colExpr = tmpExpr
		}
	}

	if len(valExprs) == 0 || colExpr == nil {
		return nil, nil, false
	}

	vals, ok := getConstBytesFromExpr(
		valExprs,
		tableDef.Cols[colExpr.Col.ColPos],
		proc,
	)
	if !ok {
		return nil, nil, false
	}
	return colExpr, vals, true
}

func CompileFilterExprs(
	exprs []*plan.Expr,
	proc *process.Process,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	canCompile bool,
) {
	canCompile = true
	if len(exprs) == 0 {
		return
	}
	if len(exprs) == 1 {
		return CompileFilterExpr(exprs[0], proc, tableDef, fs)
	}
	ops1 := make([]FastFilterOp, 0, len(exprs))
	ops2 := make([]LoadOp, 0, len(exprs))
	ops3 := make([]ObjectFilterOp, 0, len(exprs))
	ops4 := make([]BlockFilterOp, 0, len(exprs))
	ops5 := make([]SeekFirstBlockOp, 0, len(exprs))

	for _, expr := range exprs {
		expr_op1, expr_op2, expr_op3, expr_op4, expr_op5, can := CompileFilterExpr(expr, proc, tableDef, fs)
		if !can {
			return nil, nil, nil, nil, nil, false
		}
		if expr_op1 != nil {
			ops1 = append(ops1, expr_op1)
		}
		if expr_op2 != nil {
			ops2 = append(ops2, expr_op2)
		}
		if expr_op3 != nil {
			ops3 = append(ops3, expr_op3)
		}
		if expr_op4 != nil {
			ops4 = append(ops4, expr_op4)
		}
		if expr_op5 != nil {
			ops5 = append(ops5, expr_op5)
		}
	}
	fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
		for _, op := range ops1 {
			ok, err := op(obj)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
	loadOp = func(
		ctx context.Context,
		obj objectio.ObjectStats,
		inMeta objectio.ObjectMeta,
		inBF objectio.BloomFilter,
	) (meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
		for _, op := range ops2 {
			if meta != nil && bf != nil {
				continue
			}
			if meta, bf, err = op(ctx, obj, meta, bf); err != nil {
				return
			}
		}
		return
	}
	objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
		for _, op := range ops3 {
			ok, err := op(meta, bf)
			if !ok || err != nil {
				return ok, err
			}
		}
		return true, nil
	}
	blockFilterOp = func(
		blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
	) (bool, bool, error) {
		ok := true
		for _, op := range ops4 {
			thisCan, thisOK, err := op(blkIdx, blkMeta, bf)
			if err != nil {
				return false, false, err
			}
			if thisCan {
				return true, false, nil
			}
			ok = ok && thisOK
		}
		return false, ok, nil
	}

	seekOp = func(obj objectio.ObjectDataMeta) int {
		var pos int
		for _, op := range ops5 {
			pos2 := op(obj)
			if pos2 > pos {
				pos = pos2
			}
		}
		return pos
	}
	return
}

func CompileFilterExpr(
	expr *plan.Expr,
	proc *process.Process,
	tableDef *plan.TableDef,
	fs fileservice.FileService,
) (
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	canCompile bool,
) {
	canCompile = true
	if expr == nil {
		return
	}
	switch exprImpl := expr.Expr.(type) {
	// case *plan.Expr_Lit:
	// case *plan.Expr_Col:
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			leftFastOp, leftLoadOp, leftObjectOp, leftBlockOp, leftSeekOp, leftCan := CompileFilterExpr(
				exprImpl.F.Args[0], proc, tableDef, fs,
			)
			if !leftCan {
				return nil, nil, nil, nil, nil, false
			}
			rightFastOp, rightLoadOp, rightObjectOp, rightBlockOp, rightSeekOp, rightCan := CompileFilterExpr(
				exprImpl.F.Args[1], proc, tableDef, fs,
			)
			if !rightCan {
				return nil, nil, nil, nil, nil, false
			}
			if leftFastOp != nil || rightFastOp != nil {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if leftFastOp != nil {
						if ok, err := leftFastOp(obj); ok || err != nil {
							return ok, err
						}
					}
					if rightFastOp != nil {
						return rightFastOp(obj)
					}
					return true, nil
				}
			}
			if leftLoadOp != nil || rightLoadOp != nil {
				loadOp = func(
					ctx context.Context,
					obj objectio.ObjectStats,
					inMeta objectio.ObjectMeta,
					inBF objectio.BloomFilter,
				) (meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
					if leftLoadOp != nil {
						if meta, bf, err = leftLoadOp(ctx, obj, inMeta, inBF); err != nil {
							return
						}
						inMeta = meta
						inBF = bf
					}
					if rightLoadOp != nil {
						meta, bf, err = rightLoadOp(ctx, obj, inMeta, inBF)
					}
					return
				}
			}
			if leftObjectOp != nil || rightLoadOp != nil {
				objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
					if leftObjectOp != nil {
						if ok, err := leftObjectOp(meta, bf); ok || err != nil {
							return ok, err
						}
					}
					if rightObjectOp != nil {
						return rightObjectOp(meta, bf)
					}
					return true, nil
				}

			}
			if leftBlockOp != nil || rightBlockOp != nil {
				blockFilterOp = func(
					blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
				) (bool, bool, error) {
					can := true
					ok := false
					if leftBlockOp != nil {
						if thisCan, thisOK, err := leftBlockOp(blkIdx, blkMeta, bf); err != nil {
							return false, false, err
						} else {
							ok = ok || thisOK
							can = can && thisCan
						}
					}
					if rightBlockOp != nil {
						if thisCan, thisOK, err := rightBlockOp(blkIdx, blkMeta, bf); err != nil {
							return false, false, err
						} else {
							ok = ok || thisOK
							can = can && thisCan
						}
					}
					return can, ok, nil
				}
			}
			if leftSeekOp != nil || rightBlockOp != nil {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					var pos int
					if leftSeekOp != nil {
						pos = leftSeekOp(meta)
					}
					if rightSeekOp != nil {
						pos2 := rightSeekOp(meta)
						if pos2 < pos {
							pos = pos2
						}
					}
					return pos
				}
			}
		case "and":
			leftFastOp, leftLoadOp, leftObjectOp, leftBlockOp, leftSeekOp, leftCan := CompileFilterExpr(
				exprImpl.F.Args[0], proc, tableDef, fs,
			)
			if !leftCan {
				return nil, nil, nil, nil, nil, false
			}
			rightFastOp, rightLoadOp, rightObjectOp, rightBlockOp, rightSeekOp, rightCan := CompileFilterExpr(
				exprImpl.F.Args[1], proc, tableDef, fs,
			)
			if !rightCan {
				return nil, nil, nil, nil, nil, false
			}
			if leftFastOp != nil || rightFastOp != nil {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if leftFastOp != nil {
						if ok, err := leftFastOp(obj); !ok || err != nil {
							return ok, err
						}
					}
					if rightFastOp != nil {
						return rightFastOp(obj)
					}
					return true, nil
				}
			}
			if leftLoadOp != nil || rightLoadOp != nil {
				loadOp = func(
					ctx context.Context,
					obj objectio.ObjectStats,
					inMeta objectio.ObjectMeta,
					inBF objectio.BloomFilter,
				) (meta objectio.ObjectMeta, bf objectio.BloomFilter, err error) {
					if leftLoadOp != nil {
						if meta, bf, err = leftLoadOp(ctx, obj, inMeta, inBF); err != nil {
							return
						}
						inMeta = meta
						inBF = bf
					}
					if rightLoadOp != nil {
						meta, bf, err = rightLoadOp(ctx, obj, inMeta, inBF)
					}
					return
				}
			}
			if leftObjectOp != nil || rightLoadOp != nil {
				objectFilterOp = func(meta objectio.ObjectMeta, bf objectio.BloomFilter) (bool, error) {
					if leftObjectOp != nil {
						if ok, err := leftObjectOp(meta, bf); !ok || err != nil {
							return ok, err
						}
					}
					if rightObjectOp != nil {
						return rightObjectOp(meta, bf)
					}
					return true, nil
				}

			}
			if leftBlockOp != nil || rightBlockOp != nil {
				blockFilterOp = func(
					blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
				) (bool, bool, error) {
					ok := true
					if leftBlockOp != nil {
						if thisCan, thisOK, err := leftBlockOp(blkIdx, blkMeta, bf); err != nil {
							return false, false, err
						} else {
							if thisCan {
								return true, false, nil
							}
							ok = ok && thisOK
						}
					}
					if rightBlockOp != nil {
						if thisCan, thisOK, err := rightBlockOp(blkIdx, blkMeta, bf); err != nil {
							return false, false, err
						} else {
							if thisCan {
								return true, false, nil
							}
							ok = ok && thisOK
						}
					}
					return false, ok, nil
				}
			}
			if leftSeekOp != nil || rightSeekOp != nil {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					var pos int
					if leftSeekOp != nil {
						pos = leftSeekOp(meta)
					}
					if rightSeekOp != nil {
						pos2 := rightSeekOp(meta)
						if pos2 < pos {
							pos = pos2
						}
					}
					return pos
				}
			}
		case "<=":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyLEByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLEByValue(vals[0]), nil
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				ok := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLEByValue(vals[0])
				if isSorted {
					return !ok, ok, nil
				}
				return false, ok, nil
			}
		case ">=":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyGEByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0]), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					blkIdx := sort.Search(blockCnt, func(j int) bool {
						return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0])
					})
					return blkIdx
				}
			}
		case ">":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyGTByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(vals[0]), nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					blkIdx := sort.Search(blockCnt, func(j int) bool {
						return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGTByValue(vals[0])
					})
					return blkIdx
				}
			}
		case "<":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyLTByValue(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLTByValue(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				ok := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyLTByValue(vals[0])
				if isSorted {
					return !ok, ok, nil
				}
				return false, ok, nil
			}
		case "prefix_eq":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().PrefixEq(vals[0]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixEq(vals[0]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				// TODO: define canQuickBreak
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixEq(vals[0]), nil
			}
			// TODO: define seekOp
		case "prefix_between":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().PrefixBetween(vals[0], vals[1]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixBetween(vals[0], vals[1]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				// TODO: define canQuickBreak
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixBetween(vals[0], vals[1]), nil
			}
			// TODO: define seekOp
			// ok
		case "between":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().Between(vals[0], vals[1]), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().Between(vals[0], vals[1]), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				// TODO: define canQuickBreak
				return false, blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().Between(vals[0], vals[1]), nil
			}
			// TODO: define seekOp
		case "prefix_in":
			colExpr, val, ok := mustColVecValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			vec := vector.NewVec(types.T_any.ToType())
			_ = vec.UnmarshalBinary(val)
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			_, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().PrefixIn(vec), nil
				}
			}
			loadOp = loadMetadataOnlyOpFactory(fs)

			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixIn(vec), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				// TODO: define canQuickBreak
				if !blkMeta.IsEmpty() && !blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().PrefixIn(vec) {
					return false, false, nil
				}
				return false, true, nil
			}
			// TODO: define seekOp
			// ok
		case "isnull", "is_null":
			colExpr, _, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			fastFilterOp = nil
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).NullCnt() != 0, nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).NullCnt() != 0, nil
			}

			// ok
		case "isnotnull", "is_not_null":
			colExpr, _, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			fastFilterOp = nil
			loadOp = loadMetadataOnlyOpFactory(fs)
			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).NullCnt() < dataMeta.BlockHeader().Rows(), nil
			}
			blockFilterOp = func(
				_ int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				return false, blkMeta.MustGetColumn(uint16(seqNum)).NullCnt() < blkMeta.GetRows(), nil
			}

		case "in":
			colExpr, val, ok := mustColVecValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			vec := vector.NewVec(types.T_any.ToType())
			_ = vec.UnmarshalBinary(val)
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().AnyIn(vec), nil
				}
			}
			if isPK {
				loadOp = loadMetadataAndBFOpFactory(fs)
			} else {
				loadOp = loadMetadataOnlyOpFactory(fs)
			}

			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyIn(vec), nil
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				// TODO: define canQuickBreak
				if !blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap().AnyIn(vec) {
					return false, false, nil
				}
				if isPK {
					blkBf := bf.GetBloomFilter(uint32(blkIdx))
					blkBfIdx := index.NewEmptyBinaryFuseFilter()
					if err := index.DecodeBloomFilter(blkBfIdx, blkBf); err != nil {
						return false, false, err
					}
					if exist := blkBfIdx.MayContainsAny(vec); !exist {
						return false, false, nil
					}
				}
				return false, true, nil
			}
			// TODO: define seekOp
		case "=":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, proc)
			if !ok {
				canCompile = false
				return
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			isPK, isSorted := isSortedKey(colDef)
			if isSorted {
				fastFilterOp = func(obj objectio.ObjectStats) (bool, error) {
					if obj.ZMIsEmpty() {
						return true, nil
					}
					return obj.SortKeyZoneMap().ContainsKey(vals[0]), nil
				}
			}
			if isPK {
				loadOp = loadMetadataAndBFOpFactory(fs)
			} else {
				loadOp = loadMetadataOnlyOpFactory(fs)
			}

			seqNum := colDef.Seqnum
			objectFilterOp = func(meta objectio.ObjectMeta, _ objectio.BloomFilter) (bool, error) {
				if isSorted {
					return true, nil
				}
				dataMeta := meta.MustDataMeta()
				return dataMeta.MustGetColumn(uint16(seqNum)).ZoneMap().ContainsKey(vals[0]), nil
			}
			blockFilterOp = func(
				blkIdx int, blkMeta objectio.BlockObject, bf objectio.BloomFilter,
			) (bool, bool, error) {
				var (
					can, ok bool
				)
				zm := blkMeta.MustGetColumn(uint16(seqNum)).ZoneMap()
				if isSorted {
					can = !zm.AnyLEByValue(vals[0])
					if can {
						ok = false
					} else {
						ok = zm.ContainsKey(vals[0])
					}
				} else {
					can = false
					ok = zm.ContainsKey(vals[0])
				}
				if !ok {
					return can, ok, nil
				}
				if isPK {
					blkBf := bf.GetBloomFilter(uint32(blkIdx))
					blkBfIdx := index.NewEmptyBinaryFuseFilter()
					if err := index.DecodeBloomFilter(blkBfIdx, blkBf); err != nil {
						return false, false, err
					}
					exist, err := blkBfIdx.MayContainsKey(vals[0])
					if err != nil || !exist {
						return false, false, err
					}
				}
				return false, true, nil
			}
			if isSorted {
				seekOp = func(meta objectio.ObjectDataMeta) int {
					blockCnt := int(meta.BlockCount())
					blkIdx := sort.Search(blockCnt, func(j int) bool {
						return meta.GetBlockMeta(uint32(j)).MustGetColumn(uint16(seqNum)).ZoneMap().AnyGEByValue(vals[0])
					})
					return blkIdx
				}
			}
		default:
			canCompile = false
		}
	default:
		canCompile = false
	}
	return
}

func TryFastFilterBlocks(
	snapshotTS timestamp.Timestamp,
	tableDef *plan.TableDef,
	exprs []*plan.Expr,
	snapshot *logtailreplay.PartitionState,
	uncommittedObjects []objectio.ObjectStats,
	dirtyBlocks map[types.Blockid]struct{},
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
	proc *process.Process,
) (ok bool, err error) {
	fastFilterOp, loadOp, objectFilterOp, blockFilterOp, seekOp, ok := CompileFilterExprs(exprs, proc, tableDef, fs)
	if !ok {
		return false, nil
	}
	err = ExecuteBlockFilter(
		snapshotTS,
		fastFilterOp,
		loadOp,
		objectFilterOp,
		blockFilterOp,
		seekOp,
		snapshot,
		uncommittedObjects,
		dirtyBlocks,
		outBlocks,
		fs,
		proc,
	)
	return true, err
}

func ExecuteBlockFilter(
	snapshotTS timestamp.Timestamp,
	fastFilterOp FastFilterOp,
	loadOp LoadOp,
	objectFilterOp ObjectFilterOp,
	blockFilterOp BlockFilterOp,
	seekOp SeekFirstBlockOp,
	snapshot *logtailreplay.PartitionState,
	uncommittedObjects []objectio.ObjectStats,
	dirtyBlocks map[types.Blockid]struct{},
	outBlocks *objectio.BlockInfoSlice,
	fs fileservice.FileService,
	proc *process.Process,
) (err error) {

	hasDeletes := len(dirtyBlocks) > 0
	err = ForeachSnapshotObjects(
		snapshotTS,
		func(obj logtailreplay.ObjectInfo, isCommitted bool) (err2 error) {
			var ok bool
			objStats := obj.ObjectStats
			if fastFilterOp != nil {
				if ok, err2 = fastFilterOp(objStats); err2 != nil || !ok {
					return
				}
			}
			var (
				meta objectio.ObjectMeta
				bf   objectio.BloomFilter
			)
			if loadOp != nil {
				if meta, bf, err2 = loadOp(
					proc.Ctx, objStats, meta, bf,
				); err2 != nil {
					return
				}
			}
			if objectFilterOp != nil {
				if ok, err2 = objectFilterOp(meta, bf); err2 != nil || !ok {
					return
				}
			}
			var dataMeta objectio.ObjectDataMeta
			if meta != nil {
				dataMeta = meta.MustDataMeta()
			}
			var blockCnt int
			if dataMeta != nil {
				blockCnt = int(dataMeta.BlockCount())
			} else {
				blockCnt = int(objStats.BlkCnt())
			}

			name := objStats.ObjectName()
			extent := objStats.Extent()

			var pos int
			if seekOp != nil {
				pos = seekOp(dataMeta)
			}
			// TODO: cannot remove now. fix me later
			if dataMeta == nil && obj.Rows() == 0 {
				location := obj.ObjectLocation()
				if meta, err2 = objectio.FastLoadObjectMeta(
					proc.Ctx, &location, false, fs,
				); err2 != nil {
					return
				}
				dataMeta = meta.MustDataMeta()
			}

			for ; pos < blockCnt; pos++ {
				var blkMeta objectio.BlockObject
				if dataMeta != nil && blockFilterOp != nil {
					var (
						quickBreak, ok2 bool
					)
					blkMeta = dataMeta.GetBlockMeta(uint32(pos))
					if quickBreak, ok2, err2 = blockFilterOp(pos, blkMeta, bf); err2 != nil {
						return

					}
					// skip the following block checks
					if quickBreak {
						break
					}
					// skip this block
					if !ok2 {
						continue
					}
				}
				var rows uint32
				if objRows := objStats.Rows(); objRows != 0 {
					if pos < blockCnt-1 {
						rows = options.DefaultBlockMaxRows
					} else {
						rows = objRows - options.DefaultBlockMaxRows*uint32(pos)
					}
				} else {
					if blkMeta == nil {
						blkMeta = dataMeta.GetBlockMeta(uint32(pos))
					}
					rows = blkMeta.GetRows()
				}
				loc := objectio.BuildLocation(name, extent, rows, uint16(pos))
				blk := objectio.BlockInfo{
					BlockID:   *objectio.BuildObjectBlockid(name, uint16(pos)),
					SegmentID: name.SegmentId(),
					MetaLoc:   objectio.ObjectLocation(loc),
				}

				blk.Sorted = obj.Sorted
				blk.EntryState = obj.EntryState
				blk.CommitTs = obj.CommitTS
				if obj.HasDeltaLoc {
					deltaLoc, commitTs, ok := snapshot.GetBockDeltaLoc(blk.BlockID)
					if ok {
						blk.DeltaLoc = deltaLoc
						blk.CommitTs = commitTs
					}
				}

				if hasDeletes {
					if _, ok := dirtyBlocks[blk.BlockID]; !ok {
						blk.CanRemote = true
					}
					blk.PartitionNum = -1
					outBlocks.AppendBlockInfo(blk)
					continue
				}
				// store the block in ranges
				blk.CanRemote = true
				blk.PartitionNum = -1
				outBlocks.AppendBlockInfo(blk)
			}

			return
		},
		snapshot,
		uncommittedObjects...,
	)
	return
}

// -------------------------------------------
// ----------- Reader Filter -----------------
//--------------------------------------------

type AlgoOp uint8
type LogicOp uint8

const (
	Invalid_AlgoOp AlgoOp = iota
	EQ_AlgoOp
	IN_AlgoOp
	LT_AlgoOp
	LE_AlgoOp
	GT_AlgoOp
	GE_AlgoOp
	PrefixEQ_AlgoOp
	PrefixIN_AlgoOp
)

const (
	Invalid_LogicOp LogicOp = iota
	AND_LogicOp
	OR_LogicOp
)

type ReaderFilter func([]*vector.Vector, bool, bool) []int32
type InternalReaderFilter func([]*vector.Vector, []int32, *[]int32, bool, bool)

func evalFixSizedEQAndINFactory[T types.FixedSizeT](
	val []byte, vec *vector.Vector, colPos uint16, cmp func(T, T) int, isSortKey bool,
) ReaderFilter {
	var vals []T
	if vec != nil {
		vals = vector.MustFixedCol[T](vec)
	} else {
		vals = []T{types.DecodeFixed[T](val)}
	}
	return func(vecs []*vector.Vector, sorted bool, _ bool) []int32 {
		isSorted := isSortKey && sorted
		if isSorted {
			fn := vector.FixedSizedBinarySearchOffsetByValFactory(vals, cmp)
			return fn(vecs[colPos])
		}
		fn := vector.FixedSizeSearchOffsetByValFactory(vals, cmp)
		return fn(vecs[colPos])
	}
}

func evalOrderedEQAndINFactory[T types.OrderedT](
	val []byte, vec *vector.Vector, colPos uint16, isSortKey bool,
) ReaderFilter {
	var vals []T
	if vec != nil {
		vals = vector.MustFixedCol[T](vec)
	} else {
		vals = []T{types.DecodeFixed[T](val)}
	}
	return func(vecs []*vector.Vector, sorted bool, _ bool) []int32 {
		isSorted := isSortKey && sorted
		if isSorted {
			fn := vector.OrderedBinarySearchOffsetByValFactory(vals)
			return fn(vecs[colPos])
		}
		fn := vector.OrderedSearchOffsetByValFactory(vals)
		return fn(vecs[colPos])
	}
}

// PrefixEQ
func evalPrefixEQFactory(val []byte, colPos uint16, isSortKey bool) ReaderFilter {
	return func(vecs []*vector.Vector, sorted bool, _ bool) []int32 {
		isSorted := isSortKey && sorted
		if isSorted {
			return vector.CollectOffsetsByPrefixEqSortedFactory(val)(vecs[colPos])
		}
		return vector.CollectOffsetsByPrefixEqFactory(val)(vecs[colPos])
	}
}

// PrefixIN
func evalPrefixINFactory(vec *vector.Vector, colPos uint16, isSortKey bool) ReaderFilter {
	return func(vecs []*vector.Vector, sorted bool, _ bool) []int32 {
		isSorted := isSortKey && sorted
		if isSorted {
			return vector.CollectOffsetsByPrefixInSortedFactory(vec)(vecs[colPos])
		}
		return vector.CollectOffsetsByPrefixInFactory(vec)(vecs[colPos])
	}
}

// EQ and IN
func evalEQAndINFactory(
	val []byte, vec *vector.Vector, colPos uint16, colType types.T, isSortKey bool,
) ReaderFilter {
	switch colType {
	case types.T_bool:
		return func(vecs []*vector.Vector, sorted bool, _ bool) []int32 {
			isSorted := isSortKey && sorted
			var vals []bool
			if vec != nil {
				vals = vector.MustFixedCol[bool](vec)
			} else {
				vals = []bool{types.DecodeFixed[bool](val)}
			}
			cmp := func(lhs, rhs bool) int {
				if lhs == rhs {
					return 0
				}
				if lhs {
					return 1
				}
				return -1
			}
			if isSorted {
				return vector.FixedSizedBinarySearchOffsetByValFactory(vals, cmp)(vecs[colPos])
			}
			return vector.FixedSizeSearchOffsetByValFactory(vals, cmp)(vecs[colPos])
		}
	case types.T_int8:
		return evalOrderedEQAndINFactory[int8](val, vec, colPos, isSortKey)
	case types.T_int16:
		return evalOrderedEQAndINFactory[int16](val, vec, colPos, isSortKey)
	case types.T_int32:
		return evalOrderedEQAndINFactory[int32](val, vec, colPos, isSortKey)
	case types.T_int64:
		return evalOrderedEQAndINFactory[int64](val, vec, colPos, isSortKey)
	case types.T_uint8:
		return evalOrderedEQAndINFactory[uint8](val, vec, colPos, isSortKey)
	case types.T_uint16:
		return evalOrderedEQAndINFactory[uint16](val, vec, colPos, isSortKey)
	case types.T_uint32:
		return evalOrderedEQAndINFactory[uint32](val, vec, colPos, isSortKey)
	case types.T_uint64:
		return evalOrderedEQAndINFactory[uint64](val, vec, colPos, isSortKey)
	case types.T_float32:
		return evalOrderedEQAndINFactory[float32](val, vec, colPos, isSortKey)
	case types.T_float64:
		return evalOrderedEQAndINFactory[float64](val, vec, colPos, isSortKey)
	case types.T_date:
		return evalOrderedEQAndINFactory[types.Date](val, vec, colPos, isSortKey)
	case types.T_time:
		return evalOrderedEQAndINFactory[types.Time](val, vec, colPos, isSortKey)
	case types.T_datetime:
		return evalOrderedEQAndINFactory[types.Datetime](val, vec, colPos, isSortKey)
	case types.T_timestamp:
		return evalOrderedEQAndINFactory[types.Timestamp](val, vec, colPos, isSortKey)
	case types.T_enum:
		return evalOrderedEQAndINFactory[types.Enum](val, vec, colPos, isSortKey)
	case types.T_decimal64:
		return evalFixSizedEQAndINFactory[types.Decimal64](val, vec, colPos, types.CompareDecimal64, isSortKey)
	case types.T_decimal128:
		return evalFixSizedEQAndINFactory[types.Decimal128](val, vec, colPos, types.CompareDecimal128, isSortKey)
	}
	if !colType.IsFixedLen() {
		return func(vecs []*vector.Vector, sorted bool, _ bool) []int32 {
			isSorted := isSortKey && sorted
			var vals [][]byte
			if vec != nil {
				vals = vector.MustBytesCol(vec)
			} else {
				vals = [][]byte{val}
			}
			if isSorted {
				// TODO: optimize me later not to use MustBytesCol
				return vector.VarlenBinarySearchOffsetByValFactory(vals)(vecs[colPos])
			}
			// TODO: optimize me later not to use MustBytesCol
			return vector.VarlenSearchOffsetByValFactory(vals)(vecs[colPos])
		}
	}
	return nil
}

type leafNode struct {
	op        AlgoOp
	colPos    uint16
	colType   types.T
	val       []byte
	vec       *vector.Vector
	closer    func()
	isSortKey bool
}

func (n *leafNode) CanCompile() bool {
	return n.op != Invalid_AlgoOp
}

func (n *leafNode) Close() {
	if n.closer != nil {
		n.closer()
		n.closer = nil
	}
}

func (n *leafNode) Eval() ReaderFilter {
	if !n.CanCompile() {
		return nil
	}
	switch n.op {
	case EQ_AlgoOp:
		return evalEQAndINFactory(n.val, n.vec, n.colPos, n.colType, n.isSortKey)
	case IN_AlgoOp:
		return evalEQAndINFactory(n.val, n.vec, n.colPos, n.colType, n.isSortKey)
	case LT_AlgoOp:
		// TODO: implement me
	case LE_AlgoOp:
		// TODO: implement me
	case GT_AlgoOp:
		// TODO: implement me
	case GE_AlgoOp:
		// TODO: implement me
	case PrefixEQ_AlgoOp:
		return evalPrefixEQFactory(n.val, n.colPos, n.isSortKey)
	case PrefixIN_AlgoOp:
		return evalPrefixINFactory(n.vec, n.colPos, n.isSortKey)
	}
	return nil
}

type intermediateNode struct {
	canCompile bool
	nodes      []leafNode
	logicOps   []LogicOp
}

func (inode *intermediateNode) addEQLeaf(colPos uint16, colType types.T, val []byte, isSortKey bool) {
	inode.nodes = append(inode.nodes, leafNode{
		op:        EQ_AlgoOp,
		colPos:    colPos,
		colType:   colType,
		val:       val,
		isSortKey: isSortKey,
	})
}

func (inode *intermediateNode) addINLeaf(colPos uint16, colType types.T, vec *vector.Vector, isSortKey bool) {
	inode.nodes = append(inode.nodes, leafNode{
		op:        IN_AlgoOp,
		colPos:    colPos,
		colType:   colType,
		vec:       vec,
		isSortKey: isSortKey,
	})
}

func (inode *intermediateNode) addPrefixEQLeaf(colPos uint16, val []byte, isSortKey bool) {
	inode.nodes = append(inode.nodes, leafNode{
		op:        PrefixEQ_AlgoOp,
		colPos:    colPos,
		val:       val,
		isSortKey: isSortKey,
	})
}

func (inode *intermediateNode) addPrefixINLeaf(colPos uint16, vec *vector.Vector, isSortKey bool) {
	inode.nodes = append(inode.nodes, leafNode{
		op:        PrefixIN_AlgoOp,
		colPos:    colPos,
		vec:       vec,
		isSortKey: isSortKey,
	})
}

func (inode *intermediateNode) Eval() ReaderFilter {
	if !inode.canCompile || len(inode.nodes) == 0 {
		return nil
	}
	if len(inode.nodes) == 1 {
		return inode.nodes[0].Eval()
	}
	return nil
}

func CompileReaderFilterExpr(
	tableDef *plan.TableDef, expr *plan.Expr, proc *process.Process,
) (filter ReaderFilter, err error) {
	if expr == nil {
		return
	}
	return
}

func doCompileReaderFilterExpr(
	tableDef *plan.TableDef, expr *plan.Expr, node *intermediateNode, proc *process.Process,
) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			if !doCompileReaderFilterExpr(tableDef, exprImpl.F.Args[0], node) {
				return false
			}
			node.logicOps = append(node.logicOps, OR_LogicOp)
			if !doCompileReaderFilterExpr(tableDef, exprImpl.F.Args[1], node) {
				return false
			}
			return true
		case "and":
			if !doCompileReaderFilterExpr(tableDef, exprImpl.F.Args[0], node) {
				return
			}
			node.logicOps = append(node.logicOps, AND_LogicOp)
			if !doCompileReaderFilterExpr(tableDef, exprImpl.F.Args[1], node) {
				return
			}
			return true
		case "=":
			colExpr, vals, ok := mustColConstValueFromBinaryFuncExpr(exprImpl, tableDef, nil)
			if !ok {
				return false
			}
			colDef := getColDefByName(colExpr.Col.Name, tableDef)
			_, isSorted := isSortedKey(colDef)
		case "in":
		case "prefix_eq":
		case "prefix_in":
		case "prefix_between":
		}
	}
	return false
}
