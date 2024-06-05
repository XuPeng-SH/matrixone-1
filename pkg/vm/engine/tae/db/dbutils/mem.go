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
	"encoding/base64"
	"fmt"
	"runtime"
	"runtime/debug"
	"runtime/pprof"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/shirou/gopsutil/v3/mem"
	"go.uber.org/zap"
)

func GetMemoryLimit() uint64 {
	memoryLimit, _ := memlimit.FromCgroup()
	if memoryLimit == 0 {
		memStats, err := mem.VirtualMemory()
		if err != nil {
			panic(err)
		}
		memoryLimit = memStats.Total
	}
	return memoryLimit
}

func MakeDefaultMediumStringPool(name string) *containers.VectorPool {
	var (
		limit    int
		capacity int
	)
	memoryLimit := GetMemoryLimit()

	if memoryLimit > mpool.GB*100 {
		limit = mpool.MB * 5
		capacity = 400
	} else if memoryLimit > mpool.GB*64 {
		limit = mpool.MB * 5
		capacity = 200
	} else if memoryLimit > mpool.GB*32 {
		limit = mpool.MB * 4
		capacity = 200
	} else if memoryLimit > mpool.GB*16 {
		limit = mpool.MB * 4
		capacity = 100
	} else if memoryLimit > mpool.GB*8 {
		limit = mpool.MB * 4
		capacity = 50
	} else if memoryLimit > mpool.GB*4 {
		limit = mpool.MB * 4
		capacity = 20
	} else {
		limit = mpool.MB * 4
		capacity = 10
	}
	return containers.NewVectorPool(
		name,
		capacity,
		containers.WithVarlenMaxLimit(limit),
		containers.WithFixedSizeRatio(0),
	)
}

func MakeDefaultSmallPool(name string) *containers.VectorPool {
	var (
		fixedSizeLimit int
		varlenLimit    int
		capacity       int
	)
	memoryLimit := GetMemoryLimit()
	logutil.Infof("MemoryLimit:%s", common.HumanReadableBytes(int(memoryLimit)))

	if memoryLimit >= mpool.GB*64 {
		fixedSizeLimit = mpool.KB * 64
		varlenLimit = mpool.KB * 512
		capacity = 10240
	} else if memoryLimit >= mpool.GB*32 {
		fixedSizeLimit = mpool.KB * 64
		varlenLimit = mpool.KB * 256
		capacity = 9216
	} else if memoryLimit > mpool.GB*20 {
		fixedSizeLimit = mpool.KB * 32
		varlenLimit = mpool.KB * 256
		capacity = 8192
	} else if memoryLimit > mpool.GB*10 {
		fixedSizeLimit = mpool.KB * 16
		varlenLimit = mpool.KB * 128
		capacity = 8192
	} else if memoryLimit > mpool.GB*5 {
		fixedSizeLimit = mpool.KB * 8
		varlenLimit = mpool.KB * 64
		capacity = 8192
	} else {
		fixedSizeLimit = mpool.KB * 4
		varlenLimit = mpool.KB * 32
		capacity = 7168
	}

	return containers.NewVectorPool(
		name,
		capacity,
		containers.WithFixedSizeMaxLimit(fixedSizeLimit),
		containers.WithVarlenMaxLimit(varlenLimit),
		containers.WithMPool(common.SmallAllocator),
	)
}

func MakeDefaultTransientPool(name string) *containers.VectorPool {
	var (
		fixedSizeLimit int
		varlenLimit    int
		capacity       int
	)
	memoryLimit := GetMemoryLimit()
	if memoryLimit > mpool.GB*100 {
		fixedSizeLimit = mpool.KB * 200
		varlenLimit = mpool.MB * 2
		capacity = 1280
	} else if memoryLimit > mpool.GB*64 {
		fixedSizeLimit = mpool.KB * 200
		varlenLimit = mpool.MB * 3 / 2
		capacity = 1024
	} else if memoryLimit > mpool.GB*32 {
		fixedSizeLimit = mpool.KB * 200
		varlenLimit = mpool.MB * 3 / 2
		capacity = 768
	} else if memoryLimit > mpool.GB*20 {
		fixedSizeLimit = mpool.KB * 200
		varlenLimit = mpool.MB * 3 / 2
		capacity = 512
	} else if memoryLimit > mpool.GB*10 {
		fixedSizeLimit = mpool.KB * 200
		varlenLimit = mpool.MB * 3 / 2
		capacity = 384
	} else if memoryLimit > mpool.GB*5 {
		fixedSizeLimit = mpool.KB * 200
		varlenLimit = mpool.MB * 3 / 2
		capacity = 256
	} else {
		fixedSizeLimit = mpool.KB * 200
		varlenLimit = mpool.MB * 3 / 2
		capacity = 128
	}

	return containers.NewVectorPool(
		name,
		capacity,
		containers.WithFixedSizeMaxLimit(fixedSizeLimit),
		containers.WithVarlenMaxLimit(varlenLimit),
	)
}
func FormatMemStats(memstats runtime.MemStats) string {
	return fmt.Sprintf(
		"TotalAlloc:%dMB Sys:%dMB HeapAlloc:%dMB HeapSys:%dMB HeapIdle:%dMB HeapReleased:%dMB HeapInuse:%dMB NextGC:%dMB NumGC:%d PauseNs:%d",
		memstats.TotalAlloc/mpool.MB,
		memstats.Sys/mpool.MB,
		memstats.HeapAlloc/mpool.MB,
		memstats.HeapSys/mpool.MB,
		memstats.HeapIdle/mpool.MB,
		memstats.HeapReleased/mpool.MB,
		memstats.HeapInuse/mpool.MB,
		memstats.NextGC/mpool.MB,
		memstats.NumGC,
		memstats.PauseTotalNs,
	)
}

var prevHeapInuse uint64

func PrintMemStats() {
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)

	// found a spike in heapInuse
	if prevHeapInuse > 0 && memstats.HeapInuse > prevHeapInuse &&
		memstats.HeapInuse-prevHeapInuse > common.Const1GBytes*10 {
		heapp := pprof.Lookup("heap")
		buf := &bytes.Buffer{}
		heapp.WriteTo(buf, 0)
		mlimit := debug.SetMemoryLimit(-1)
		log := buf.Bytes()
		chunkSize := 150 * 1024
		for len(log) > chunkSize {
			logutil.Info(base64.RawStdEncoding.EncodeToString(log[:chunkSize]))
			log = log[chunkSize:]
		}
		logutil.Info(
			base64.RawStdEncoding.EncodeToString(log),
			zap.String("mlimit", common.HumanReadableBytes(int(mlimit))))
	}

	prevHeapInuse = memstats.HeapInuse
	logutil.Infof("HeapInfo:%s", FormatMemStats(memstats))
}
