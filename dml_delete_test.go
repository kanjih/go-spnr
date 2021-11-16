package spnr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var testDMLRepository = NewDML("Test")

func TestDML_buildDeleteStmt(t *testing.T) {
	stmt := testDMLRepository.buildDeleteStmt(testRecord1)
	assert.Equal(t, "DELETE FROM `Test` WHERE `String`=@w_String AND `Int64`=@w_Int64", stmt.SQL)
	assert.Equal(t, testRecord1.String, stmt.Params["w_String"].(string))
}

func TestDML_buildDeleteAllStmt(t *testing.T) {
	stmt := testDMLRepository.buildDeleteAllStmt(&([]*Test{testRecord1, testRecord2}))
	assert.Equal(t, "DELETE FROM `Test` WHERE (`String`=@w_String_0 AND `Int64`=@w_Int64_0) OR (`String`=@w_String_1 AND `Int64`=@w_Int64_1)", stmt.SQL)
	assert.Equal(t, testRecord1.String, stmt.Params["w_String_0"].(string))
	assert.Equal(t, testRecord1.Int64, stmt.Params["w_Int64_0"].(int64))
	assert.Equal(t, testRecord2.String, stmt.Params["w_String_1"].(string))
	assert.Equal(t, testRecord2.Int64, stmt.Params["w_Int64_1"].(int64))
}
