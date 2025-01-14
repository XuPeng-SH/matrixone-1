// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildDelete(stmt *tree.Delete, ctx CompilerContext) (*Plan, error) {
	aliasMap := make(map[string][2]string)
	for _, tbl := range stmt.TableRefs {
		getAliasToName(ctx, tbl, "", aliasMap)
	}
	tblInfo, err := getDmlTableInfo(ctx, stmt.Tables, stmt.With, aliasMap, "delete")
	if err != nil {
		return nil, err
	}
	builder := NewQueryBuilder(plan.Query_SELECT, ctx)
	bindCtx := NewBindContext(builder, nil)

	rewriteInfo := &dmlSelectInfo{
		typ:     "delete",
		rootId:  -1,
		tblInfo: tblInfo,
	}

	canTruncate := false
	if tblInfo.haveConstraint {
		bindCtx.groupTag = builder.genNewTag()
		bindCtx.aggregateTag = builder.genNewTag()
		bindCtx.projectTag = builder.genNewTag()

		// if delete table have constraint
		err = initDeleteStmt(builder, bindCtx, rewriteInfo, stmt)
		if err != nil {
			return nil, err
		}

		for i, tableDef := range tblInfo.tableDefs {
			err = rewriteDmlSelectInfo(builder, bindCtx, rewriteInfo, tableDef, rewriteInfo.derivedTableId, i)
			if err != nil {
				return nil, err
			}
		}

		// append ProjectNode
		rewriteInfo.rootId = builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: rewriteInfo.projectList,
			Children:    []int32{rewriteInfo.rootId},
			BindingTags: []int32{bindCtx.projectTag},
		}, bindCtx)
		bindCtx.results = rewriteInfo.projectList
	} else {
		// if delete table have no constraint
		if stmt.Where == nil && stmt.Limit == nil {
			canTruncate = true
		}
		rewriteInfo.rootId, err = deleteToSelect(builder, bindCtx, stmt, false, tblInfo)
		if err != nil {
			return nil, err
		}
	}

	builder.qry.Steps = append(builder.qry.Steps, rewriteInfo.rootId)
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}

	// append delete node
	deleteCtx := &plan.DeleteCtx{
		CanTruncate: canTruncate,
		Ref:         rewriteInfo.tblInfo.objRef,
		Idx:         make([]*plan.IdList, len(rewriteInfo.tblInfo.tableDefs)),

		IdxRef: rewriteInfo.onIdxTbl,
		IdxIdx: rewriteInfo.onIdx,

		OnRestrictRef: rewriteInfo.onRestrictTbl,
		OnRestrictIdx: rewriteInfo.onRestrict,
		OnCascadeRef:  rewriteInfo.onCascadeRef,
		OnCascadeIdx:  make([]int32, len(rewriteInfo.onCascade)),

		OnSetRef:       rewriteInfo.onSetRef,
		OnSetIdx:       make([]*plan.IdList, len(rewriteInfo.onSet)),
		OnSetDef:       rewriteInfo.onSetTableDef,
		OnSetUpdateCol: make([]*plan.ColPosMap, len(rewriteInfo.onSetUpdateCol)),
	}
	rowIdIdx := int64(0)
	for i, table_def := range rewriteInfo.tblInfo.tableDefs {
		if table_def.Pkey == nil {
			deleteCtx.Idx[i] = &plan.IdList{
				List: []int64{rowIdIdx},
			}
			rowIdIdx = rowIdIdx + 1
		} else {
			// have pk
			deleteCtx.Idx[i] = &plan.IdList{
				List: []int64{rowIdIdx, rowIdIdx + 1},
			}
			rowIdIdx = rowIdIdx + 2
		}
	}
	for i, idxList := range rewriteInfo.onCascade {
		deleteCtx.OnCascadeIdx[i] = int32(idxList[0])
	}
	for i, setList := range rewriteInfo.onSet {
		deleteCtx.OnSetIdx[i] = &plan.IdList{
			List: setList,
		}
	}
	for i, idxMap := range rewriteInfo.onSetUpdateCol {
		deleteCtx.OnSetUpdateCol[i] = &plan.ColPosMap{
			Map: idxMap,
		}
	}

	node := &Node{
		NodeType:  plan.Node_DELETE,
		ObjRef:    nil,
		TableDef:  nil,
		Children:  []int32{query.Steps[len(query.Steps)-1]},
		NodeId:    int32(len(query.Nodes)),
		DeleteCtx: deleteCtx,
	}
	query.Nodes = append(query.Nodes, node)
	query.Steps[len(query.Steps)-1] = node.NodeId
	query.StmtType = plan.Query_DELETE

	return &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}, err
}
