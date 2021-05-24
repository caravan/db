package internal_test

import (
	"fmt"
	"testing"

	"github.com/caravan/db/database"
	"github.com/caravan/db/internal"
	"github.com/caravan/db/value"
	"github.com/stretchr/testify/assert"
)

func TestUniqueIndexInsert(t *testing.T) {
	as := assert.New(t)

	d, _ := makeTestDatabase()
	err := d(func(d database.Database) error {
		tbl, ok := d.Table("test-table")
		as.True(ok)

		return tbl.Insert(value.NewKey(), tableRow1)
	})
	as.NotNil(err)
	as.EqualError(err,
		fmt.Sprintf(internal.ErrUniqueConstraintFailed, "unique-index"),
	)
}

func TestUniqueIndexUpdate(t *testing.T) {
	as := assert.New(t)
	d, _ := makeTestDatabase()
	err := d(func(d database.Database) error {
		tbl, ok := d.Table("test-table")
		as.True(ok)

		_, err := tbl.Update(tableKey2, tableRow1)
		return err
	})
	as.NotNil(err)
	as.EqualError(err,
		fmt.Sprintf(internal.ErrUniqueConstraintFailed, "unique-index"),
	)
}
