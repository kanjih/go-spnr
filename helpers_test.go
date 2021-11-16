package spnr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestValidateType(t *testing.T) {
	var str string
	var stru Test
	var sl []string
	var structSl []Test

	err := validateStructType(&stru)
	assert.Nil(t, err)
	err = validateSliceType(&sl)
	assert.Nil(t, err)
	err = validateStructSliceType(&structSl)
	assert.Nil(t, err)

	err = validateStructType(&sl)
	assert.NotNil(t, err)
	assert.Equal(t, "final argument must be struct but got slice", err.Error())
	err = validateSliceType(&str)
	assert.NotNil(t, err)
	assert.Equal(t, "final argument must be slice but got string", err.Error())
	err = validateStructSliceType(&sl)
	assert.NotNil(t, err)
	assert.Equal(t, "final argument must be slice of struct but got slice of string", err.Error())
}
