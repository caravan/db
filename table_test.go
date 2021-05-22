package db_test

import (
	"fmt"
	"testing"

	"github.com/caravan/db"
	"github.com/stretchr/testify/assert"
)

var (
	tableKey1 = db.NewKey()
	tableKey2 = db.NewKey()
	tableRow1 = db.Row{S("first str"), S("second str")}
	tableRow2 = db.Row{S("third str"), S("fourth str")}
	tableRow3 = db.Row{S("fifth str"), S("sixth str")}
)

func makeTestTable() (db.Table, error) {
	d := db.NewDatabase()
	tbl, err := d.CreateTable("test-table",
		db.MakeColumn("first"),
		db.MakeColumn("second"),
	)
	if err != nil {
		return nil, err
	}

	_, err = tbl.CreateIndex(db.UniqueIndex, "unique-index", "first", "second")
	if err != nil {
		return nil, err
	}

	_, err = tbl.CreateIndex(db.StandardIndex, "standard-index", "first")
	if err != nil {
		return nil, err
	}

	return tbl, tbl.MutateWith(func(mutate db.TableMutator) error {
		if err := mutate.Insert(tableKey1, tableRow1); err != nil {
			return err
		}
		if err := mutate.Insert(tableKey2, tableRow2); err != nil {
			return err
		}
		return nil
	})
}

func TestMakeTable(t *testing.T) {
	as := assert.New(t)

	tbl, err := makeTestTable()
	as.NotNil(tbl)
	as.Nil(err)

	as.Equal(db.TableName("test-table"), tbl.Name())
	cols := tbl.Columns()
	as.Equal(2, len(cols))
	as.Equal(db.ColumnName("first"), cols[0].Name())
	as.Equal(db.ColumnName("second"), cols[1].Name())

	as.Equal(2, len(tbl.Indexes()))
	idx, ok := tbl.Index("not found")
	as.Nil(idx)
	as.False(ok)
}

func TestTableMutateWith(t *testing.T) {
	as := assert.New(t)

	tbl, _ := makeTestTable()
	err := tbl.MutateWith(func(mutate db.TableMutator) error {
		row, ok := mutate.Select(tableKey1)
		as.True(ok)
		as.Equal(tableRow1, row)

		row, ok = mutate.Select(tableKey2)
		as.True(ok)
		as.Equal(tableRow2, row)

		row, ok = mutate.Select(db.NewKey())
		as.False(ok)
		as.Nil(row)
		return nil
	})
	as.Nil(err)

	err = tbl.MutateWith(func(mutate db.TableMutator) error {
		old, err := mutate.Update(tableKey1, tableRow3)
		as.Nil(err)
		as.Equal(tableRow1, old)
		return nil
	})
	as.Nil(err)

	err = tbl.MutateWith(func(mutate db.TableMutator) error {
		row, ok := mutate.Select(tableKey1)
		as.True(ok)
		as.Equal(tableRow3, row)
		return nil
	})
	as.Nil(err)
}

func TestTableMutateWithDelete(t *testing.T) {
	as := assert.New(t)

	tbl, _ := makeTestTable()
	err := tbl.MutateWith(func(mutate db.TableMutator) error {
		old, ok := mutate.Delete(tableKey1)
		as.True(ok)
		as.Equal(tableRow1, old)
		return nil
	})
	as.Nil(err)

	err = tbl.MutateWith(func(mutate db.TableMutator) error {
		row, ok := mutate.Select(tableKey1)
		as.False(ok)
		as.Nil(row)
		return nil
	})
	as.Nil(err)
}

func TestTableMutateWithError(t *testing.T) {
	as := assert.New(t)

	tbl, _ := makeTestTable()
	err := tbl.MutateWith(func(mutate db.TableMutator) error {
		old, err := mutate.Update(tableKey1, tableRow3)
		as.Nil(err)
		as.Equal(tableRow1, old)
		return fmt.Errorf("should not update")
	})
	as.NotNil(err)
	as.EqualError(err, "should not update")

	err = tbl.MutateWith(func(mutate db.TableMutator) error {
		row, ok := mutate.Select(tableKey1)
		as.True(ok)
		as.Equal(tableRow1, row)
		return nil
	})
	as.Nil(err)
}

func TestTableMutateWithTruncate(t *testing.T) {
	as := assert.New(t)

	tbl, _ := makeTestTable()
	err := tbl.MutateWith(func(mutate db.TableMutator) error {
		mutate.Truncate()
		return nil
	})
	as.Nil(err)

	err = tbl.MutateWith(func(mutate db.TableMutator) error {
		old, ok := mutate.Select(tableKey1)
		as.Nil(old)
		as.False(ok)
		return nil
	})
	as.Nil(err)
}

func TestTableMutateWithErrors(t *testing.T) {
	as := assert.New(t)

	tbl, _ := makeTestTable()
	err := tbl.MutateWith(func(mutate db.TableMutator) error {
		err := mutate.Insert(tableKey1, tableRow3)
		as.EqualError(err, fmt.Sprintf(db.ErrKeyAlreadyExists, tableKey1))

		tableKey3 := db.NewKey()
		old, err := mutate.Update(tableKey3, tableRow3)
		as.Nil(old)
		as.EqualError(err, fmt.Sprintf(db.ErrKeyNotFound, tableKey3))

		old, ok := mutate.Delete(tableKey3)
		as.Nil(old)
		as.False(ok)

		return nil
	})
	as.Nil(err)
}

func TestTableCreateIndexError(t *testing.T) {
	as := assert.New(t)
	tbl, _ := makeTestTable()

	idx, err := tbl.CreateIndex(db.UniqueIndex, "unique-index")
	as.Nil(idx)
	as.NotNil(err)
	as.EqualError(err, fmt.Sprintf(db.ErrIndexAlreadyExists, "unique-index"))

	idx, err = tbl.CreateIndex(db.UniqueIndex, "another-index", "not found")
	as.Nil(idx)
	as.NotNil(err)
	as.EqualError(err, fmt.Sprintf(db.ErrColumnNotFound, "not found"))
}
