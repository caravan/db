package table

import (
	"github.com/caravan/db/column"
	"github.com/caravan/db/index"
	"github.com/caravan/db/relation"
	"github.com/caravan/db/value"
)

type (
	// Name identifies a Table
	Name string

	// Names are a set of Name
	Names []Name

	// Table is an interface that associates a Key with a Row and provides
	// additional capabilities around this association
	Table interface {
		Name() Name
		Columns() column.Columns

		Indexes() index.Names
		CreateIndex(index.Type, index.Name, ...column.Name) error

		Insert(value.Key, relation.Row) error
		Update(value.Key, relation.Row) (relation.Row, error)
		Delete(value.Key) (relation.Row, bool)
		Truncate()

		Select(value.Key) (relation.Row, bool)
	}
)
