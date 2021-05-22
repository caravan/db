package db

import "fmt"

type (
	// Relation describes a set of associated Values
	Relation []Value

	// Row is a storage-level Relation
	Row Relation

	// Selector is a function that takes a Row and returns a Relation
	Selector func(Row) Relation
)

// Error messages
const (
	ErrColumnNotFound = "column not found in table: %s"
)

// MakeOffsets takes Columns and a set of ColumnName and returns the
// Offsets needed to retrieve the specified ColumnNames
func MakeOffsets(cols Columns, names ...ColumnName) (Offsets, error) {
	named := MakeNamedOffsets(cols...)
	off := make(Offsets, len(names))
	for i, n := range names {
		if o, ok := named[n]; ok {
			off[i] = o
		} else {
			return nil, fmt.Errorf(ErrColumnNotFound, n)
		}
	}
	return off, nil
}

// MakeNamedSelector takes a Columns and a set of ColumnName and returns a
// Selector that can be used to convert a Row to the desired Relation
func MakeNamedSelector(cols Columns, names ...ColumnName) (Selector, error) {
	off, err := MakeOffsets(cols, names...)
	if err != nil {
		return nil, err
	}
	return MakeOffsetSelector(off...), nil
}

// MakeOffsetSelector returns a Selector based on the specified Offsets
func MakeOffsetSelector(offsets ...Offset) Selector {
	l := len(offsets)
	return func(r Row) Relation {
		res := make(Relation, l)
		for i, o := range offsets {
			res[i] = r[o]
		}
		return res
	}
}
