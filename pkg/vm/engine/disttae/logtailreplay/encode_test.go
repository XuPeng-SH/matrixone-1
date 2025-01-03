// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func BenchmarkEncode(b *testing.B) {
	packer := types.NewPacker()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ioutil.EncodePrimaryKey(int64(i), packer)
	}
	b.StopTimer()
	packer.Close()
	b.StartTimer()
}
