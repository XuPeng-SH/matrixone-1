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

package objectio

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const defaultCheckpointDir = "ckp/"

const (
	CheckpointExt = "ckp"
	CompactedExt  = "cpt"

	PrefixMetadata = "meta"
	SuffixMetadata = ".ckp"
)

func GetCheckpointDir() string {
	return defaultCheckpointDir
}

type CKPMeta struct {
	start types.TS
	end   types.TS
	name  string
	idx   int
	ext   string
}

func (m *CKPMeta) GetStart() *types.TS {
	return &m.start
}

func (m *CKPMeta) GetEnd() *types.TS {
	return &m.end
}

func (m *CKPMeta) GetExt() string {
	return m.ext
}

func (m *CKPMeta) GetName() string {
	return m.name
}

func (m *CKPMeta) GetFullName() string {
	return GetCheckpointDir() + m.name
}

func (m *CKPMeta) GetIdx() int {
	return m.idx
}

func (m *CKPMeta) SetIdx(idx int) {
	m.idx = idx
}

// with dirname
func EncodeCKPMetadataFullName(
	start, end types.TS,
) string {
	return fmt.Sprintf(
		"%s/%s_%s_%s.%s",
		GetCheckpointDir(),
		PrefixMetadata,
		start.ToString(),
		end.ToString(),
		CheckpointExt,
	)
}

// without dirname
func EncodeCKPMetadataName(
	start, end types.TS,
) string {
	return fmt.Sprintf(
		"%s_%s_%s.%s",
		PrefixMetadata,
		start.ToString(),
		end.ToString(),
		CheckpointExt,
	)
}

func DecodeCKPMetaName(name string) (meta CKPMeta) {
	fileName := strings.Split(name, ".")
	info := strings.Split(fileName[0], "_")
	meta.start = types.StringToTS(info[1])
	meta.end = types.StringToTS(info[2])
	meta.ext = fileName[1]
	meta.name = name
	return
}
