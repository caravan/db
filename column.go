package db

type (
	// ColumnName identifies a Column
	ColumnName string

	// ColumnNames are a set of ColumnName
	ColumnNames []ColumnName

	// Column describes a column to be selected from a Table. The
	// description includes the column's name and a TableSelector for
	// retrieving the column's value from an Event
	Column interface {
		Name() ColumnName
	}

	// Columns are a set of Column
	Columns []Column

	// Offset is the location of a Column within a set of Columns
	Offset int

	// Offsets are a set of Offset
	Offsets []Offset

	// NamedOffsets allows an Offset to be retrieved by ColumnName
	NamedOffsets map[ColumnName]Offset

	// column is the internal implementation of a column
	column struct {
		name ColumnName
	}
)

// MakeColumn instantiates a new column instance
func MakeColumn(n ColumnName) Column {
	return &column{
		name: n,
	}
}

func (c *column) Name() ColumnName {
	return c.name
}

// MakeNamedOffsets takes a set of Columns and returns its NamedOffsets
func MakeNamedOffsets(cols ...Column) NamedOffsets {
	res := make(NamedOffsets, len(cols))
	for i, c := range cols {
		res[c.Name()] = Offset(i)
	}
	return res
}
