package internal_test

import (
	"fmt"
	"testing"

	"github.com/caravan/db/index"
	"github.com/caravan/db/internal"
	"github.com/caravan/db/table"
	"github.com/caravan/db/value"
	"github.com/stretchr/testify/assert"
)

func TestIndexName(t *testing.T) {
	as := assert.New(t)
	tbl, _ := makeTestTable()
	idx, ok := tbl.Index("unique-index")
	as.NotNil(idx)
	as.True(ok)
	as.Equal(index.Name("unique-index"), idx.Name())
}

func TestUniqueIndexInsert(t *testing.T) {
	as := assert.New(t)
	tbl, _ := makeTestTable()
	err := tbl.MutateWith(func(mutate table.Mutator) error {
		return mutate.Insert(value.NewKey(), tableRow1)
	})
	as.NotNil(err)
	as.EqualError(err,
		fmt.Sprintf(internal.ErrUniqueConstraintFailed, "unique-index"),
	)
}

func TestUniqueIndexUpdate(t *testing.T) {
	as := assert.New(t)
	tbl, _ := makeTestTable()
	err := tbl.MutateWith(func(mutate table.Mutator) error {
		_, err := mutate.Update(tableKey2, tableRow1)
		return err
	})
	as.NotNil(err)
	as.EqualError(err,
		fmt.Sprintf(internal.ErrUniqueConstraintFailed, "unique-index"),
	)
}
