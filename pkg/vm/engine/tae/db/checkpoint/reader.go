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

package checkpoint

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"go.uber.org/zap"
)

func MakeMetafileFullName(dir, name string) string {
	return dir + name
}

func MakeMetadataBatch() *batch.Batch {
	return batch.NewWithSchema(
		false,
		CheckpointSchema.Attrs(),
		CheckpointSchema.Types(),
	)
}

func MakeMetafilesReaderFromDir(
	ctx context.Context,
	sid string,
	dir string,
	fs fileservice.FileService,
) (*MetafilesReader, error) {
	var (
		entries []fileservice.DirEntry
		err     error
	)
	if entries, err = fileservice.SortedList(fs.List(ctx, dir)); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	return MakeMetafilesReaderFromSortedDirEntries(
		sid, dir, entries, fs,
	), nil
}

func MakeMetafilesReaderFromSortedDirEntries(
	sid string,
	dir string,
	sortedDirEntries []fileservice.DirEntry,
	fs fileservice.FileService,
) *MetafilesReader {
	files := make([]string, 0, len(sortedDirEntries))
	for _, entry := range sortedDirEntries {
		if entry.IsDir {
			continue
		}
		// FIXME: check file name is valid
		files = append(files, entry.Name)
	}
	return NewMetafilesReader(sid, dir, files, fs)
}

func NewMetafilesReader(
	sid string,
	dir string,
	files []string,
	fs fileservice.FileService,
) *MetafilesReader {
	return &MetafilesReader{
		sid:   sid,
		dir:   dir,
		files: files,
		fs:    fs,
	}
}

type MetafilesReader struct {
	sid   string
	dir   string
	files []string
	idx   int
	fs    fileservice.FileService
}

func (r *MetafilesReader) Read(
	ctx context.Context,
	mp *mpool.MPool,
) (bats []*batch.Batch, release func(), isEnd bool, err error) {
	if r.idx >= len(r.files) {
		isEnd = true
		return
	}
	var (
		now  = time.Now()
		name = r.files[r.idx]
	)
	defer func() {
		logger := logutil.Error
		if err == nil {
			r.idx++
			logger = logutil.Info
		}
		var (
			allocated int
			rows      int
		)
		for _, bat := range bats {
			rows += bat.RowCount()
			allocated += bat.Allocated()
		}
		logger(
			"Read-CKP-MF-File",
			zap.Error(err),
			zap.Int("bat-cnt", len(bats)),
			zap.Int("rows", rows),
			zap.Int("allocated", allocated),
			zap.String("name", name),
			zap.Duration("cost", time.Since(now)),
		)
	}()

	select {
	case <-ctx.Done():
		err = context.Cause(ctx)
		return
	default:
	}

	fname := MakeMetafileFullName(r.dir, name)

	var reader *blockio.BlockReader
	if reader, err = blockio.NewFileReader(
		r.sid,
		r.fs,
		fname,
	); err != nil {
		return
	}

	bats, release, err = reader.LoadAllColumns(
		ctx, nil, mp,
	)
	return
}
