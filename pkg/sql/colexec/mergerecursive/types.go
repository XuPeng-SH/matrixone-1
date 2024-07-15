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

package mergerecursive

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MergeRecursive)

type container struct {
	colexec.ReceiverOperator
	bats []*batch.Batch
	buf  *batch.Batch
	last bool
}

type MergeRecursive struct {
	ctr *container

	vm.OperatorBase
}

func (mergeRecursive *MergeRecursive) GetOperatorBase() *vm.OperatorBase {
	return &mergeRecursive.OperatorBase
}

func init() {
	reuse.CreatePool[MergeRecursive](
		func() *MergeRecursive {
			return &MergeRecursive{}
		},
		func(a *MergeRecursive) {
			*a = MergeRecursive{}
		},
		reuse.DefaultOptions[MergeRecursive]().
			WithEnableChecker(),
	)
}

func (mergeRecursive MergeRecursive) TypeName() string {
	return opName
}

func NewArgument() *MergeRecursive {
	return reuse.Alloc[MergeRecursive](nil)
}

func (mergeRecursive *MergeRecursive) Release() {
	if mergeRecursive != nil {
		reuse.Free[MergeRecursive](mergeRecursive, nil)
	}
}

func (mergeRecursive *MergeRecursive) Reset(proc *process.Process, pipelineFailed bool, err error) {
	mergeRecursive.Free(proc, pipelineFailed, err)
}

func (mergeRecursive *MergeRecursive) Free(proc *process.Process, pipelineFailed bool, err error) {
	if mergeRecursive.ctr != nil {
		mergeRecursive.ctr.FreeMergeTypeOperator(pipelineFailed)
		for _, b := range mergeRecursive.ctr.bats {
			if b != nil {
				b.Clean(proc.Mp())
			}
			mergeRecursive.ctr.bats = nil
		}
		if mergeRecursive.ctr.buf != nil {
			mergeRecursive.ctr.buf.Clean(proc.Mp())
			mergeRecursive.ctr.buf = nil
		}
		mergeRecursive.ctr = nil
	}
}
