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

package dbutils

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func MakeBlockCompactionDesc(scope common.ID) string {
	return fmt.Sprintf("Compact Block %s", scope.String())
}

func MakeDelSegDesc(scopes []common.ID) string {
	var w bytes.Buffer
	_, _ = w.WriteString("Delete Segments ")
	for i, scope := range scopes {
		if i > 0 {
			_, _ = w.WriteString(", ")
		}
		_, _ = w.WriteString(scope.SegmentString())
	}
	return w.String()
}

func MakeMergeBlocksDesc(scopes []common.ID) string {
	var w bytes.Buffer
	_, _ = w.WriteString("Merge Blocks ")
	for i, scope := range scopes {
		if i > 0 {
			_, _ = w.WriteString(", ")
		}
		_, _ = w.WriteString(scope.BlockString())
	}
	return w.String()
}
