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

package objectio

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/util/fault"
)

const (
	FJ_CommitDelete  = "fj/commit/delete"
	FJ_CommitSlowLog = "fj/commit/slowlog"
	FJ_TransferSlow  = "fj/transfer/slow"
	FJ_FlushTimeout  = "fj/flush/timeout"

	FJ_TraceRanges         = "fj/trace/ranges"
	FJ_TracePartitionState = "fj/trace/partitionstate"

	FJ_Debug19524 = "fj/debug/19524"
	FJ_Debug19787 = "fj/debug/19787"

	FJ_LogReader    = "fj/log/reader"
	FJ_LogWorkspace = "fj/log/workspace"
)

const (
	FJ_LogLevel0 = iota
	FJ_LogLevel1
	FJ_LogLevel2
	FJ_LogLevel3
)

// ParseFJIntArgs parses the injected fault integer argument
// and returns the log level and function id
// logLevel = iarg % 10 and funcId = iarg / 10
// iarg quick mapping:
// 0->3: logLevel = 0->3, database name equal
// 10->13: logLevel = 0->3, database name contains
// 20->23: logLevel = 0->3, table name equal
// 30->33: logLevel = 0->3, table name contains
// 40->43: logLevel = 0->3, database name and table name contains
func ParseLoggingIntArgs(iarg int) (logLevel int, funcId int) {
	logLevel = iarg % 10
	if logLevel >= FJ_LogLevel3 {
		logLevel = FJ_LogLevel3
	}
	funcId = iarg / 10
	return
}

func ParseLoggingSArgs(sarg string, funcId int, args ...string) bool {
	if len(args) == 0 {
		return false
	}
	switch funcId {
	case 0: // equal args[0]
		return sarg == args[0] || args[0] == ""
	case 1: // contains args[0]
		return strings.Contains(sarg, args[0])
	case 2: // equal args[1]
		if len(args) < 2 {
			return false
		}
		return sarg == args[1] || args[1] == ""
	case 3: // contains args[1]
		if len(args) < 2 {
			return false
		}
		return strings.Contains(sarg, args[1])
	case 4: // contains args[0] and args[1]
		params := strings.Split(sarg, ".")
		if len(params) != 2 {
			return false
		}
		return strings.Contains(params[0], args[0]) && strings.Contains(params[1], args[1])
	default:
		return false
	}
}

func makeInjectIntArg(level, funcId int) int {
	return level + funcId*10
}

func MakeInjectTableIntArg(level int) int {
	// 0 means equal database name
	// 1 means contains database name
	// 2 means equal table name
	// 3 means contains table name
	// 4 means contains database name and table name
	return makeInjectIntArg(level, 0)
}

func checkLoggingArgs(
	iarg int, sarg string, inputArgs ...string,
) (bool, int) {
	level, funcId := ParseLoggingIntArgs(iarg)
	ok := ParseLoggingSArgs(sarg, funcId, inputArgs...)
	if !ok {
		return false, 0
	}
	return ok, level
}

func LogWorkspaceInjected(args ...string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_LogWorkspace)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, args...)
}

// `name` is the table name
// return injected, logLevel
func LogReaderInjected(args ...string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_LogReader)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, args...)
}

// inject log reader and partition state
// `name` is the table name
func InjectLog1(
	tableName string,
	level int,
) (rmFault func(), err error) {
	iarg := int64(MakeInjectTableIntArg(level))
	rmFault = func() {}
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_LogReader,
		":::",
		"echo",
		iarg,
		tableName,
	); err != nil {
		return
	}
	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_TracePartitionState,
		":::",
		"echo",
		iarg,
		tableName,
	); err != nil {
		fault.RemoveFaultPoint(context.Background(), FJ_LogReader)
		return
	}

	if err = fault.AddFaultPoint(
		context.Background(),
		FJ_LogWorkspace,
		":::",
		"echo",
		iarg,
		tableName,
	); err != nil {
		fault.RemoveFaultPoint(context.Background(), FJ_LogReader)
		fault.RemoveFaultPoint(context.Background(), FJ_TracePartitionState)
		return
	}

	rmFault = func() {
		fault.RemoveFaultPoint(context.Background(), FJ_LogWorkspace)
		fault.RemoveFaultPoint(context.Background(), FJ_TracePartitionState)
		fault.RemoveFaultPoint(context.Background(), FJ_LogReader)
	}
	return
}

func Debug19524Injected() bool {
	_, _, injected := fault.TriggerFault(FJ_Debug19524)
	return injected
}

func Debug19787Injected() bool {
	_, _, injected := fault.TriggerFault(FJ_Debug19787)
	return injected
}

func RangesLogInjected(dbName, tableName string) (bool, int) {
	_, sarg, injected := fault.TriggerFault(FJ_TraceRanges)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(0, sarg, dbName, tableName)
}

func InjectRanges(
	ctx context.Context,
	tableName string,
) (rmFault func(), err error) {
	rmFault = func() {}
	if err = fault.AddFaultPoint(
		ctx,
		FJ_TraceRanges,
		":::",
		"echo",
		int64(MakeInjectTableIntArg(0)),
		tableName,
	); err != nil {
		return
	}
	rmFault = func() {
		fault.RemoveFaultPoint(ctx, FJ_TraceRanges)
	}
	return
}

func PartitionStateInjected(dbName, tableName string) (bool, int) {
	iarg, sarg, injected := fault.TriggerFault(FJ_TracePartitionState)
	if !injected {
		return false, 0
	}
	return checkLoggingArgs(int(iarg), sarg, dbName, tableName)
}
