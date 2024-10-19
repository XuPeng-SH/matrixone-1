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

package frontend

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	getSpecialTablesInfoFormat = "" +
		"select " +
		"	cast (count(distinct md.dat_id) as bigint), " +
		"	cast (count(distinct mt.rel_id) as bigint) " +
		"from mo_catalog.mo_tables as mt, mo_catalog.mo_database as md " +
		"where" +
		"	mt.relkind in ('v','e','r','cluster') and " +
		"	mt.account_id = %d and" +
		"	md.account_id = %d;"

	getAccountInfoFormatV2 = "" +
		"WITH db_tbl_counts AS (" +
		"	SELECT" +
		"		CAST(mt.account_id AS BIGINT) AS account_id," +
		"		COUNT(DISTINCT md.dat_id) AS db_count," +
		"		COUNT(DISTINCT mt.rel_id) AS tbl_count" +
		"	FROM" +
		"		mo_catalog.mo_tables AS mt" +
		"	JOIN" +
		"		mo_catalog.mo_database AS md" +
		"	ON " +
		"		mt.account_id = md.account_id AND" +
		"		mt.relkind IN ('v','e','r','cluster') " +
		"	GROUP BY" +
		"		mt.account_id" +
		")," +
		"final_result AS (" +
		"	SELECT" +
		"		CAST(ma.account_id AS BIGINT) AS account_id," +
		"		ma.account_name," +
		"		ma.admin_name," +
		"		ma.created_time," +
		"		ma.status," +
		"		ma.suspended_time," +
		"		db_tbl_counts.db_count," +
		"		db_tbl_counts.tbl_count," +
		"		CAST(0 AS DOUBLE) AS size," +
		"		CAST(0 AS DOUBLE) AS snapshot_size," +
		"		ma.comments" +
		"		%s" + // possible placeholder for object count
		"	FROM" +
		"		db_tbl_counts" +
		"	JOIN" +
		"		mo_catalog.mo_account AS ma " +
		"	ON " +
		"		db_tbl_counts.account_id = ma.account_id " +
		"		%s" + // where clause
		")" +
		"SELECT * FROM final_result;"
)

const idxOfAccountId = 0
const idxOfObjectCount = idxOfComment + 1

const (
	// column index in the result set generated by
	// the sql getAllAccountInfoFormat, getAccountInfoFormat
	idxOfAccountName = iota
	idxOfAdminName
	idxOfCreatedTime
	idxOfStatus
	idxOfSuspendedTime
	idxOfDbCount
	idxOfTableCount
	idxOfSize
	idxOfSnapshotSize
	idxOfComment
)

var cnUsageCache = logtail.NewStorageUsageCache(
	logtail.WithLazyThreshold(5))

func getSqlForAccountInfo(like *tree.ComparisonExpr, accId int64, needObjectCount bool) string {
	var likePattern = ""
	var where = ""
	var and = ""
	var account = ""
	if like != nil {
		likePattern = fmt.Sprintf("ma.account_name like '%s'", strings.TrimSpace(like.Right.String()))
	}

	if accId != -1 {
		account = fmt.Sprintf("ma.account_id = %s", strconv.FormatInt(accId, 10))
	}

	if len(likePattern) != 0 && len(account) != 0 {
		and = "and"
	}

	if len(likePattern) != 0 || len(account) != 0 {
		where = "where"
	}

	clause := fmt.Sprintf("%s %s %s %s", where, likePattern, and, account)

	var objectCountExpr = ""
	if needObjectCount {
		objectCountExpr = ", CAST(0 AS BIGINT) AS object_count"
	}

	return fmt.Sprintf(getAccountInfoFormatV2, objectCountExpr, clause)
}

func requestStorageUsage(ctx context.Context, ses *Session, accIds [][]int64) (resp any, tried bool, err error) {
	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payload := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		req := cmd_util.StorageUsageReq{}
		for x := range accIds {
			req.AccIds = append(req.AccIds, accIds[x]...)
		}

		return req.Marshal()
	}

	responseUnmarshaler := func(payload []byte) (any, error) {
		usage := &cmd_util.StorageUsageResp_V3{}
		if err := usage.Unmarshal(payload); err != nil {
			return nil, err
		}
		return usage, nil
	}

	txnOperator := ses.txnHandler.GetTxn()

	// create a new proc for `handler`
	proc := process.NewTopProcess(ctx, ses.proc.GetMPool(),
		ses.proc.Base.TxnClient, txnOperator,
		ses.proc.Base.FileService, ses.proc.Base.LockService,
		ses.proc.Base.QueryClient, ses.proc.Base.Hakeeper,
		ses.proc.Base.UdfService, ses.proc.Base.Aicm,
	)

	handler := ctl.GetTNHandlerFunc(api.OpCode_OpStorageUsage, whichTN, payload, responseUnmarshaler)
	result, err := handler(proc, "DN", "", ctl.MoCtlTNCmdSender)
	if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
		// try the previous RPC method
		payload_V0 := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) { return nil, nil }
		responseUnmarshaler_V0 := func(payload []byte) (interface{}, error) {
			usage := &cmd_util.StorageUsageResp_V0{}
			if err := usage.Unmarshal(payload); err != nil {
				return nil, err
			}
			return usage, nil
		}

		tried = true
		CmdMethod_StorageUsage := api.OpCode(14)
		handler = ctl.GetTNHandlerFunc(CmdMethod_StorageUsage, whichTN, payload_V0, responseUnmarshaler_V0)
		result, err = handler(proc, "DN", "", ctl.MoCtlTNCmdSender)

		if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
			return nil, tried, moerr.NewNotSupportedNoCtx("current tn version not supported `show accounts`")
		}
	}

	if err != nil {
		return nil, tried, err
	}

	return result.Data.([]any)[0], tried, nil
}

func handleStorageUsageResponse_V0(
	ctx context.Context,
	sid string,
	fs fileservice.FileService,
	usage *cmd_util.StorageUsageResp_V0,
	logger SessionLogger,
) (map[int64][]uint64, error) {
	result := make(map[int64][]uint64, 0)
	for idx := range usage.CkpEntries {
		version := usage.CkpEntries[idx].Version
		location := usage.CkpEntries[idx].Location

		ckpData, err := logtail.LoadSpecifiedCkpBatch(ctx, sid, location, version, logtail.StorageUsageInsIDX, fs)
		if err != nil {
			return nil, err
		}

		storageUsageBat := ckpData.GetBatches()[logtail.StorageUsageInsIDX]
		accIDVec := vector.MustFixedColWithTypeCheck[uint64](
			storageUsageBat.GetVectorByName(catalog.SystemColAttr_AccID).GetDownstreamVector(),
		)
		sizeVec := vector.MustFixedColWithTypeCheck[uint64](
			storageUsageBat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize).GetDownstreamVector(),
		)

		size := uint64(0)
		length := len(accIDVec)
		for i := 0; i < length; i++ {
			if result[int64(accIDVec[i])] == nil {
				result[int64(accIDVec[i])] = make([]uint64, 2)
			}
			result[int64(accIDVec[i])][0] += sizeVec[i]
			size += sizeVec[i]
		}

		ckpData.Close()
	}

	// [account_id, db_id, table_id, obj_id, table_total_size]
	for _, info := range usage.BlockEntries {
		result[int64(info.Info[0])][0] += info.Info[3]
	}

	return result, nil
}

func handleStorageUsageResponse(
	ctx context.Context,
	usage *cmd_util.StorageUsageResp_V3,
) (map[int64][]uint64, error) {
	result := make(map[int64][]uint64, 0)

	for x := range usage.AccIds {
		if result[usage.AccIds[x]] == nil {
			result[usage.AccIds[x]] = make([]uint64, 2)
		}
		result[usage.AccIds[x]][0] += usage.Sizes[x]
		result[usage.AccIds[x]][1] += usage.SnapshotSizes[x]
	}

	return result, nil
}

func checkStorageUsageCache(accIds [][]int64) (result map[int64][]uint64, succeed bool) {
	cnUsageCache.Lock()
	defer cnUsageCache.Unlock()

	if cnUsageCache.IsExpired() {
		return nil, false
	}

	result = make(map[int64][]uint64)
	for x := range accIds {
		for y := range accIds[x] {
			size, snapshotSize, exist := cnUsageCache.GatherAccountSize(uint64(accIds[x][y]))
			if !exist {
				// one missed, update all
				return nil, false
			}
			result[accIds[x][y]] = make([]uint64, 2)
			result[accIds[x][y]][0] = size
			result[accIds[x][y]][1] = snapshotSize

		}
	}

	return result, true
}

func updateStorageUsageCache(usages *cmd_util.StorageUsageResp_V3) {

	if len(usages.AccIds) == 0 {
		return
	}

	cnUsageCache.Lock()
	defer cnUsageCache.Unlock()

	// step 1: delete stale accounts
	cnUsageCache.ClearForUpdate()

	// step 2: update
	for x := range usages.AccIds {
		usage := logtail.UsageData{
			AccId:        uint64(usages.AccIds[x]),
			Size:         usages.Sizes[x],
			SnapshotSize: usages.SnapshotSizes[x],
			ObjectAbstract: logtail.ObjectAbstract{
				TotalObjCnt: int(usages.ObjCnts[x]),
				TotalBlkCnt: int(usages.BlkCnts[x]),
				TotalRowCnt: int(usages.RowCnts[x]),
			},
		}
		//if old, exist := cnUsageCache.Get(usage); exist {
		//	usage.Size += old.Size
		//}

		cnUsageCache.SetOrReplace(usage)
	}
}

// getAccountStorageUsage calculates the storage usage of all accounts
// by handling checkpoint
func getAccountsStorageUsage(ctx context.Context, ses *Session, accIds [][]int64) (map[int64][]uint64, error) {
	if len(accIds) == 0 {
		return nil, nil
	}

	// step 1: check cache
	if usage, succeed := checkStorageUsageCache(accIds); succeed {
		return usage, nil
	}

	// step 2: query to tn
	response, tried, err := requestStorageUsage(ctx, ses, accIds)
	if err != nil {
		return nil, err
	}

	if tried {
		usage, ok := response.(*cmd_util.StorageUsageResp_V0)
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx("storage usage response decode failed, retry later")
		}

		fs, err := fileservice.Get[fileservice.FileService](getPu(ses.GetService()).FileService, defines.SharedFileServiceName)
		if err != nil {
			return nil, err
		}
		// step 3: handling these pulled data
		return handleStorageUsageResponse_V0(ctx, ses.GetService(), fs, usage, ses.GetLogger())

	} else {
		usage, ok := response.(*cmd_util.StorageUsageResp_V3)
		if !ok || usage.Magic != logtail.StorageUsageMagic {
			return nil, moerr.NewInternalErrorNoCtx("storage usage response decode failed, retry later")
		}

		updateStorageUsageCache(usage)

		// step 3: handling these pulled data
		return handleStorageUsageResponse(ctx, usage)
	}
}

func updateStorageSize(ori *vector.Vector, size uint64, rowIdx int) {
	vector.SetFixedAtWithTypeCheck(ori, rowIdx, math.Round(float64(size)/1048576.0*1e6)/1e6)
}

func updateCount(ori *vector.Vector, delta int64, rowIdx int) {
	old := vector.GetFixedAtWithTypeCheck[int64](ori, rowIdx)
	vector.SetFixedAtWithTypeCheck[int64](ori, rowIdx, old+delta)
}

func updateObjectCount(ori *vector.Vector, cnt int64, rowIdx int) {
	vector.SetFixedAtWithTypeCheck[int64](ori, rowIdx, cnt)
}

func doShowAccounts(ctx context.Context, ses *Session, sa *tree.ShowAccounts) (err error) {
	var sql string
	var accIds [][]int64
	var accInfosBatches []*batch.Batch
	var eachAccountInfo []*batch.Batch
	var tempBatch *batch.Batch
	var specialTableCnt, specialDBCnt int64

	mp := ses.GetMemPool()

	defer func() {
		for _, b := range accInfosBatches {
			if b == nil {
				continue
			}
			b.Clean(mp)
		}

		for _, b := range eachAccountInfo {
			if b == nil {
				continue
			}
			b.Clean(mp)
		}
		if tempBatch != nil {
			tempBatch.Clean(mp)
		}
	}()

	bh := ses.GetRawBatchBackgroundExec(ctx)
	defer bh.Close()

	account := ses.GetTenantInfo()

	err = bh.Exec(ctx, "begin;")
	t0 := time.Now()
	defer func() {
		now := time.Now()
		v2.TaskShowAccountsTotalDurationHistogram.Observe(now.Sub(t0).Seconds())
		err = finishTxn(ctx, bh, err)
	}()

	if err != nil {
		return err
	}

	var needUpdateObjectCountMetric bool
	if account.IsSysTenant() &&
		sa.Like == nil &&
		ses.GetTxnCompileCtx().GetDatabase() == mometric.MetricDBConst {
		// storage usage cron task try to get storage usage for all accounts,
		// adding an extra col to return object count val for all accounts.
		needUpdateObjectCountMetric = true
	}

	if account.IsSysTenant() {
		sql = getSqlForAccountInfo(sa.Like, -1, needUpdateObjectCountMetric)
		if accInfosBatches, accIds, err = getAccountInfo(ctx, bh, sql, mp); err != nil {
			return err
		}

		// normal account
	} else {
		if sa.Like != nil {
			return moerr.NewInternalError(ctx, "only sys account can use LIKE clause")
		}
		// switch to the sys account to get account info
		newCtx := defines.AttachAccountId(ctx, uint32(sysAccountID))
		sql = getSqlForAccountInfo(nil, int64(account.GetTenantID()), needUpdateObjectCountMetric)
		if accInfosBatches, accIds, err = getAccountInfo(newCtx, bh, sql, mp); err != nil {
			return err
		}

		if len(accInfosBatches) != 1 {
			return moerr.NewInternalErrorf(ctx, "no such account %v", account.GetTenantID())
		}
	}

	specialTableCnt, specialDBCnt, err = getSpecialTableCnt(ctx, bh, accIds)
	t1 := time.Now()
	v2.TaskShowAccountsGetTableStatsDurationHistogram.Observe(t1.Sub(t0).Seconds())
	if err != nil {
		return err
	}

	usage, err := getAccountsStorageUsage(ctx, ses, accIds)
	v2.TaskShowAccountsGetUsageDurationHistogram.Observe(time.Since(t1).Seconds())
	if err != nil {
		return err
	}

	var abstract map[uint64]logtail.ObjectAbstract
	if needUpdateObjectCountMetric {
		abstract = cnUsageCache.GatherObjectAbstractForAccounts()
	}

	for x := range accIds {
		for y := range accIds[x] {
			var size, snapshotSize uint64
			if len(usage[accIds[x][y]]) > 0 {
				size = usage[accIds[x][y]][0]
				snapshotSize = usage[accIds[x][y]][1]
			}
			updateStorageSize(accInfosBatches[x].Vecs[idxOfSize], size, y)
			updateStorageSize(accInfosBatches[x].Vecs[idxOfSnapshotSize], snapshotSize, y)
			if accIds[x][y] != sysAccountID {
				updateCount(accInfosBatches[x].Vecs[idxOfDbCount], specialDBCnt, y)
				updateCount(accInfosBatches[x].Vecs[idxOfTableCount], specialTableCnt, y)
			}

			if needUpdateObjectCountMetric {
				updateObjectCount(
					accInfosBatches[x].Vecs[idxOfObjectCount],
					int64(abstract[uint64(accIds[x][y])].TotalObjCnt), y)
			}
		}
	}

	backSes := bh.(*backExec)
	resultSet := backSes.backSes.allResultSet[0]
	columnDef := backSes.backSes.rs
	bh.ClearExecResultSet()

	outputRS := &MysqlResultSet{}
	if err = initOutputRs(outputRS, resultSet, ctx); err != nil {
		return err
	}

	for _, b := range accInfosBatches {
		if err = fillResultSet(ctx, b, ses, outputRS); err != nil {
			return err
		}
	}

	ses.SetMysqlResultSet(outputRS)

	ses.rs = columnDef

	if canSaveQueryResult(ctx, ses) {
		err = saveQueryResult(ctx, ses,
			func() ([]*batch.Batch, error) {
				return accInfosBatches, nil
			},
			nil,
		)
		if err != nil {
			return err
		}
	}

	return err
}

func initOutputRs(dest *MysqlResultSet, src *MysqlResultSet, ctx context.Context) error {
	for idx := idxOfAccountName; idx < int(src.GetColumnCount()); idx++ {
		o, err := src.GetColumn(ctx, uint64(idx))
		if err != nil {
			return err
		}
		dest.AddColumn(o)
	}
	return nil
}

// getAccountInfo gets account info from mo_account under sys account
func getAccountInfo(ctx context.Context,
	bh BackgroundExec,
	sql string,
	mp *mpool.MPool) ([]*batch.Batch, [][]int64, error) {
	var err error
	var accountIds [][]int64
	var rsOfMoAccount []*batch.Batch

	bh.ClearExecResultBatches()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	//the resultBatches referred outside this function far away
	rsOfMoAccount = Copy[*batch.Batch](bh.GetExecResultBatches())
	if len(rsOfMoAccount) == 0 {
		return nil, nil, moerr.NewInternalError(ctx, "no account info")
	}

	//copy rsOfMoAccount from backgroundExec
	//the original batches to be released at the ClearExecResultBatches or the backgroundExec.Close
	//the rsOfMoAccount to be released at the end of the doShowAccounts
	for i := 0; i < len(rsOfMoAccount); i++ {
		originBat := rsOfMoAccount[i]

		rsOfMoAccount[i], err = originBat.Dup(mp)
		if err != nil {
			return nil, nil, err
		}
	}

	batchCount := len(rsOfMoAccount)
	accountIds = make([][]int64, batchCount)
	for i := 0; i < batchCount; i++ {
		vecLen := rsOfMoAccount[i].Vecs[0].Length()
		for row := 0; row < vecLen; row++ {
			accountIds[i] = append(accountIds[i], vector.GetFixedAtWithTypeCheck[int64](rsOfMoAccount[i].Vecs[0], row))
		}
	}

	// maybe it's tricky here
	// remove the account id column
	{
		for _, b := range rsOfMoAccount {
			accIdVec := b.Vecs[idxOfAccountId]
			b.Vecs = b.Vecs[idxOfAccountId+1:]
			accIdVec.Free(mp)
		}

		backSes := bh.(*backExec)
		backSes.backSes.allResultSet[0].Columns = backSes.backSes.allResultSet[0].Columns[idxOfAccountId+1:]
		backSes.backSes.rs.ResultCols = backSes.backSes.rs.ResultCols[idxOfAccountId+1:]
	}

	return rsOfMoAccount, accountIds, err
}

func getSpecialTableCnt(ctx context.Context, bh BackgroundExec, accIds [][]int64) (dbCnt, tblCnt int64, err error) {
	for x := range accIds {
		for y := range accIds[x] {
			if accIds[x][y] == sysAccountID {
				continue
			}
			dbCnt, tblCnt, err = getSpecialTableInfo(ctx, bh, accIds[x][y])
			return
		}
	}
	return
}

func getSpecialTableInfo(ctx context.Context, bh BackgroundExec, accId int64) (dbCnt, tblCnt int64, err error) {
	sql := fmt.Sprintf(getSpecialTablesInfoFormat, sysAccountID, sysAccountID)
	newCtx := defines.AttachAccountId(ctx, uint32(accId))

	bh.ClearExecResultBatches()
	err = bh.Exec(newCtx, sql)
	if err != nil {
		return 0, 0, err
	}

	ret := bh.GetExecResultBatches()
	if len(ret) == 0 {
		return 0, 0, moerr.NewInternalError(ctx, "no special table info")
	}

	dbCnt = vector.MustFixedColWithTypeCheck[int64](ret[0].Vecs[1])[0]
	tblCnt = vector.MustFixedColWithTypeCheck[int64](ret[0].Vecs[0])[0]
	return dbCnt, tblCnt, nil
}
