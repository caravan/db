package database

import (
	"github.com/caravan/db/column"
	"github.com/caravan/db/table"
)

type (
	// Transactor provides access to a Database's internal state
	Transactor func(Query) (Transactor, error)

	// Query is a function that can perform Database queries or mutations
	Query func(Database) error

	// Database is an interface that manages a set of Tables and other
	// data management structures
	Database interface {
		Tables() table.Names
		Table(table.Name) (table.Table, bool)
		CreateTable(table.Name, ...column.Column) (table.Table, error)
	}
)
