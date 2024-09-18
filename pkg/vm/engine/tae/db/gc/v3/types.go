package gc

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

var ObjectTableAttrs []string
var ObjectTableTypes []types.Type

func init() {
	ObjectTableAttrs = []string{
		"stats",
		"created_ts",
		"deleted_ts",
		"db_id",
		"table_id",
	}
	ObjectTableTypes = []types.Type{
		objectio.VarcharType,
		objectio.TSType,
		objectio.TSType,
		objectio.Uint64Type,
		objectio.Uint64Type,
	}
}

func NewObjectTableBatch() *Batch {
	ret := batch.New(false, ObjectTableAttrs)
	ret.SetVector(0, vector.NewVec(ObjectTableTypes[0]))
	ret.SetVector(1, vector.NewVec(ObjectTableTypes[1]))
	ret.SetVector(2, vector.NewVec(ObjectTableTypes[2]))
	ret.SetVector(3, vector.NewVec(ObjectTableTypes[3]))
	ret.SetVector(4, vector.NewVec(ObjectTableTypes[4]))
	return ret
}
