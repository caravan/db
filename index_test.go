package db_test

import (
	"fmt"
	"testing"

	"github.com/caravan/db"
	"github.com/stretchr/testify/assert"
)

func TestIndexName(t *testing.T) {
	as := assert.New(t)
	tbl, _ := makeTestTable()
	idx, ok := tbl.Index("unique-index")
	as.NotNil(idx)
	as.True(ok)
	as.Equal(db.IndexName("unique-index"), idx.Name())
}

func TestUniqueIndexInsert(t *testing.T) {
	as := assert.New(t)
	tbl, _ := makeTestTable()
	err := tbl.MutateWith(func(mutate db.TableMutator) error {
		return mutate.Insert(db.NewKey(), tableRow1)
	})
	as.NotNil(err)
	as.EqualError(err,
		fmt.Sprintf(db.ErrUniqueConstraintFailed, "unique-index"),
	)
}

func TestUniqueIndexUpdate(t *testing.T) {
	as := assert.New(t)
	tbl, _ := makeTestTable()
	err := tbl.MutateWith(func(mutate db.TableMutator) error {
		_, err := mutate.Update(tableKey2, tableRow1)
		return err
	})
	as.NotNil(err)
	as.EqualError(err,
		fmt.Sprintf(db.ErrUniqueConstraintFailed, "unique-index"),
	)
}
