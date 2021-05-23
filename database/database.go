package database

import (
	"github.com/caravan/db/column"
	"github.com/caravan/db/table"
	"github.com/caravan/db/transaction"
)

// Database is an interface that manages a set of Tables and other
// data management structures
type Database interface {
	CreateTransaction() transaction.Txn

	CreateTable(table.Name, ...column.Column) (table.Table, error)
	Tables() table.Names
	Table(table.Name) (table.Table, bool)
}
