package internal_test

import (
	"fmt"
	"testing"

	"github.com/caravan/db"
	"github.com/caravan/db/column"
	"github.com/caravan/db/database"
	"github.com/caravan/db/internal"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/table"
	"github.com/caravan/db/value"
	"github.com/stretchr/testify/assert"
)

var (
	tableKey1 = value.NewKey()
	tableKey2 = value.NewKey()
	tableRow1 = relation.Row{
		value.String("first str"), value.String("second str"),
	}
	tableRow2 = relation.Row{
		value.String("third str"), value.String("fourth str"),
	}
	tableRow3 = relation.Row{
		value.String("fifth str"), value.String("sixth str"),
	}
)

func makeTestDatabase() (database.Transactor, error) {
	d := internal.NewDatabase()
	return d(func(d database.Database) error {
		tbl, err := d.CreateTable("test-table",
			column.Make("first"),
			column.Make("second"),
		)
		if err != nil {
			return err
		}

		err = tbl.CreateIndex(db.UniqueIndex, "unique-index", "first", "second")
		if err != nil {
			return err
		}

		err = tbl.CreateIndex(db.StandardIndex, "standard-index", "first")
		if err != nil {
			return err
		}

		if err = tbl.Insert(tableKey1, tableRow1); err != nil {
			return err
		}
		if err = tbl.Insert(tableKey2, tableRow2); err != nil {
			return err
		}
		return nil
	})
}

func TestMakeTable(t *testing.T) {
	as := assert.New(t)

	d, _ := makeTestDatabase()
	d, err := d(func(d database.Database) error {
		tbl, ok := d.Table("test-table")
		as.NotNil(tbl)
		as.True(ok)

		as.Equal(table.Name("test-table"), tbl.Name())
		cols := tbl.Columns()
		as.Equal(2, len(cols))
		as.Equal(column.Name("first"), cols[0].Name())
		as.Equal(column.Name("second"), cols[1].Name())

		as.Equal(2, len(tbl.Indexes()))
		return nil
	})
	as.NotNil(d)
	as.Nil(err)
}

func TestTable(t *testing.T) {
	as := assert.New(t)

	d, _ := makeTestDatabase()
	d, err := d(func(d database.Database) error {
		tbl, ok := d.Table("test-table")
		as.True(ok)

		_, ok = d.Table("missing-table")
		as.False(ok)

		row, ok := tbl.Select(tableKey1)
		as.True(ok)
		as.Equal(tableRow1, row)

		row, ok = tbl.Select(tableKey2)
		as.True(ok)
		as.Equal(tableRow2, row)

		row, ok = tbl.Select(value.NewKey())
		as.False(ok)
		as.Nil(row)

		old, err := tbl.Update(tableKey1, tableRow3)
		as.Nil(err)
		as.Equal(tableRow1, old)

		row, ok = tbl.Select(tableKey1)
		as.True(ok)
		as.Equal(tableRow3, row)
		as.Nil(err)
		return nil
	})
	as.NotNil(d)
	as.Nil(err)
}

func TestTableDelete(t *testing.T) {
	as := assert.New(t)

	d, _ := makeTestDatabase()
	d, err := d(func(d database.Database) error {
		tbl, ok := d.Table("test-table")
		as.True(ok)

		old, ok := tbl.Delete(tableKey1)
		as.True(ok)
		as.Equal(tableRow1, old)

		row, ok := tbl.Select(tableKey1)
		as.False(ok)
		as.Nil(row)
		return nil
	})
	as.NotNil(d)
	as.Nil(err)
}

func TestTableTruncate(t *testing.T) {
	as := assert.New(t)

	d, _ := makeTestDatabase()
	d, err := d(func(d database.Database) error {
		tbl, ok := d.Table("test-table")
		as.True(ok)

		tbl.Truncate()

		old, ok := tbl.Select(tableKey1)
		as.Nil(old)
		as.False(ok)
		return nil
	})
	as.NotNil(d)
	as.Nil(err)
}

func TestTableMutateWithErrors(t *testing.T) {
	as := assert.New(t)

	d, _ := makeTestDatabase()
	d, err := d(func(d database.Database) error {
		tbl, ok := d.Table("test-table")
		as.True(ok)

		err := tbl.Insert(tableKey1, tableRow3)
		as.EqualError(err, fmt.Sprintf(internal.ErrKeyAlreadyExists, tableKey1))

		tableKey3 := value.NewKey()
		old, err := tbl.Update(tableKey3, tableRow3)
		as.Nil(old)
		as.EqualError(err, fmt.Sprintf(internal.ErrKeyNotFound, tableKey3))

		old, ok = tbl.Delete(tableKey3)
		as.Nil(old)
		as.False(ok)
		return nil
	})
	as.NotNil(d)
	as.Nil(err)
}

func TestTableCreateIndexError(t *testing.T) {
	as := assert.New(t)

	d, _ := makeTestDatabase()
	d, err := d(func(d database.Database) error {
		tbl, ok := d.Table("test-table")
		as.True(ok)

		err := tbl.CreateIndex(db.UniqueIndex, "unique-index")
		as.NotNil(err)
		as.EqualError(err, fmt.Sprintf(internal.ErrIndexAlreadyExists, "unique-index"))

		err = tbl.CreateIndex(db.UniqueIndex, "another-index", "not found")
		as.NotNil(err)
		as.EqualError(err, fmt.Sprintf(relation.ErrColumnNotFound, "not found"))
		return nil
	})
	as.NotNil(d)
	as.Nil(err)
}
