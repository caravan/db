# Caravan DB

[![Go Report Card](https://goreportcard.com/badge/github.com/caravan/db)](https://goreportcard.com/report/github.com/caravan/db) [![Build Status](https://app.travis-ci.com/caravan/db.svg?branch=main)](https://app.travis-ci.com/caravan/db) [![Test Coverage](https://api.codeclimate.com/v1/badges/6b5bfbfd0266530ed754/test_coverage)](https://codeclimate.com/github/caravan/db/test_coverage) [![Maintainability](https://api.codeclimate.com/v1/badges/6b5bfbfd0266530ed754/maintainability)](https://codeclimate.com/github/caravan/db/maintainability) [![GitHub](https://img.shields.io/github/license/caravan/db?cache=0)](https://github.com/caravan/db/blob/main/LICENSE.md)

Caravan is a set of in-process event streaming tools for [Go](https://golang.org/) applications. Think ["Kafka"](https://kafka.apache.org), but for the internal workings of your software. Caravan DB includes features for managing Relational Tables and Indexes.

_This is a work in progress. Not at all ready to be used for any purpose_

## Introduction

Caravan DB is a "Database as a Function." What this means is that the database manages a single persistent data structure exposed via a transactor function. The transactor function will invoke a provided query and automatically return a new version of the database if that query is successful.

For example:

```go
package main

import (
  "github.com/caravan/db"
  "github.com/caravan/db/column"
  "github.com/caravan/db/database"
)

func main() {
  empty := db.NewDatabase()
  stuff, _ := empty(func(d database.Database) error {
    tbl, _ := d.CreateTable("some-table",
      column.Make("first_name"),
      column.Make("last_name"),
    )

    _ = tbl.CreateIndex(db.StandardIndex, "full-name",
      "last_name", "first_name",
    )
    return nil
  })
}
```

The variable `empty` will point to the original  empty database, whereas the variable `stuff` will point to a new version of the database that contains a table called `"some-table"`.
