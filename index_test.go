package db_test

import (
	"testing"

	"github.com/caravan/db"
	"github.com/stretchr/testify/assert"
)

func TestIndexName(t *testing.T) {
	as := assert.New(t)
	tbl, _ := makeTestTable()
	idx, ok := tbl.Index("my-index")
	as.NotNil(idx)
	as.True(ok)
	as.Equal(db.IndexName("my-index"), idx.Name())
}
