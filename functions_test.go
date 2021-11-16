package spnr

import (
	"cloud.google.com/go/spanner"
	"fmt"
	"gotest.tools/assert"
	"testing"
)

func TestToKeySets(t *testing.T) {
	expected := spanner.KeySetFromKeys(spanner.Key{"a"}, spanner.Key{"b"}, spanner.Key{"c"})
	actual := ToKeySets([]string{"a", "b", "c"})
	assert.Equal(t, fmt.Sprintf("%+v", expected), fmt.Sprintf("%+v", actual))
}
