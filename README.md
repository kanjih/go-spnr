<img src="https://user-images.githubusercontent.com/21299899/142083327-cd45a119-2d2b-4cec-b0d3-4b57d843877f.png" width="400px">

ORM for Cloud Spanner to boost your productivity 🚀

[![Go Reference](https://pkg.go.dev/badge/github.com/kanjih/go-spnr/v2.svg)](https://pkg.go.dev/github.com/kanjih/go-spnr/v2)
[![Actions Status](https://github.com/kanjih/go-spnr/workflows/test/badge.svg?branch=main)](https://github.com/kanjih/go-spnr/actions)


## Example 🔧
```go
package main

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/kanjih/go-spnr"
)

type Singer struct {
	// spnr supports 2 types of tags.
	// - spanner: spanner column name
	// - pk: primary key order
	SingerID string `spanner:"SingerId" pk:"1"`
	Name     string `spanner:"Name"`
}

func main() {
	ctx := context.Background()
	client, _ := spanner.NewClient(ctx, "projects/{project_id}/instances/{instance_id}/databases/{database_id}")

	// initialize
	singerStore := spnr.New("Singers") // specify table name

	// save record (spnr supports both Mutation API & DML!)
	singerStore.ApplyInsertOrUpdate(ctx, client, &Singer{SingerID: "a", Name: "Alice"})

	// fetch record
	var singer Singer
	singerStore.Reader(ctx, client.Single()).FindOne(spanner.Key{"a"}, &singer)

	// fetch record using raw query
	var singers []Singer
	query := "select * from Singers where SingerId=@singerId"
	params := map[string]any{"singerId": "a"}
	singerStore.Reader(ctx, client.Single()).Query(query, params, &singers)
}
```

## Features
- Supports both **Mutation API** & **DML**
- Supports code generation to map records
- Supports raw SQLs for complicated cases

spnr is designed ...
- 🙆‍♂️ for reducing boliderplate codes (i.e. mapping selected records to struct or write simple insert/update/delete operations)
- 🙅‍♀️ not for hiding queries executed in background (spnr doesn't support abstractions for complicated operations)

## Table of contents
- [Installation](#installation)
- spnr APIs
  - [Read operations](#read-operations)
  - [Mutation API](#mutation-api)
  - [DML](#dml)
- [Embedding](#embedding)
- [Code generation](#code-generation)
- [Helper functions](#helper-functions)

## Installation
```
go get github.com/kanjih/go-spnr/v2
```
\* v2 requires go 1.18 or later. If you use previous go versions please use v1.

## Read operations
spnr provides the following types of read operations 💪
1. Select records using primary keys
2. Select one column using primary keys
3. Select records using query
4. Select one value using query

### 1. Select records using primary keys
```go
var singer Singer
singerStore.Reader(ctx, tx).FindOne(spanner.Key{"a"}, &singer)

var singers []Singer
keys := spanner.KeySetFromKeys(spanner.Key{"a"}, spanner.Key{"b"})
singerStore.Reader(ctx, tx).FindAll(keys, &singers)
```

#### 📝 Note
`tx` is the transaction object. You can get it by calling `spanner.Client.ReadOnly(ReadWrite)Transaction`, or `spanner.Client.Single` method.

### 2. Select one column using primary keys
```go
var name string
singerStore.Reader(ctx, tx).GetColumn(spanner.Key{"a"}, "Name", &name)

var names []string
keys := spanner.KeySetFromKeys(spanner.Key{"a"}, spanner.Key{"b"})
singerStore.Reader(ctx, tx).GetColumnAll(keys, "Name", &names)
```

#### In the case you want to fetch multiple columns
Making temporal struct to map columns is the best solution.
```go
type cols struct {
  Name string `spanner:"Name"`
  Score spanner.NullInt64 `spanner:"Score"`
}
var res cols
singerStore.Reader(ctx, tx).FindOne(spanner.Key{"1"}, &res)
```

### 3. Select records using query
```go
var singer Singer
query := "select * from `Singers` where SingerId=@singerId"
params := map[string]any{"singerId": "a"}
singerStore.Reader(ctx, tx).QueryOne(query, params, &singer)

var singers []Singer
query = "select * from Singers"
singerStore.Reader(ctx, tx).Query(query, nil, &singers)
```

### 4. Select one value using query
```go
var cnt int64
query := "select count(*) as cnt from Singers"
singerStore.Reader(ctx, tx).QueryValue(query, nil, &cnt)
```

### * Notes
- `FindOne`, `GetColumn` method uses `ReadRow` method of `spanner.ReadWrite(ReadOnly)Transaction`.
- `FindAll`, `GetColumnAll` method uses `Read` method.
- `QueryOne`, `Query` Method uses `Query` method.

## Mutation API
Executing mutation API using spnr is badly simple! Here's the example 👇
```go
singer := &Singer{SingerID: "a", Name: "Alice"}
singers := []Singer{{SingerID: "b", Name: "Bob"}, {SingerID: "c", Name: "Carol"}}

singerStore := spnr.New("Singers") // specify table name

singerStore.InsertOrUpdate(tx, singer)  // Insert or update
singerStore.InsertOrUpdate(tx, &singers) // Insert or update multiple records

singerStore.Update(tx, singer)  // Update
singerStore.Update(tx, &singers) // Update multple records

singerStore.Delete(tx, singer)  // Delete
singerStore.Delete(tx, &singers) // Delete multiple records
```

Don't want to use in transaction? You can use `ApplyXXX`.
```go
singerStore.ApplyInsertOrUpdate(ctx, client, singer) // client is spanner.Dataclient
singerStore.ApplyDelete(ctx, client, &singers)
```

## DML
spnr parses struct then build DML 💪
```go
singer := &Singer{SingerID: "a", Name: "Alice"}
singers := []Singer{{SingerID: "b", Name: "Bob"}, {SingerID: "c", Name: "Carol"}}

singerStore := spnr.NewDML("Singers") // specify table name

singerStore.Insert(ctx, tx, singer)
// -> INSERT INTO `Singers` (`SingerId`, `Name`) VALUES (@SingerId, @Name)

singerStore.Insert(ctx, tx, &singers)
// -> INSERT INTO `Singers` (`SingerId`, `Name`) VALUES (@SingerId_0, @Name_0), (@SingerId_1, @Name_1)

singerStore.Update(ctx, tx, singer)
// -> UPDATE `Singers` SET `Name`=@Name WHERE `SingerId`=@w_SingerId
singerStore.Update(ctx, tx, &singers)
// -> UPDATE `Singers` SET `Name`=@Name WHERE `SingerId`=@w_SingerId
// -> UPDATE `Singers` SET `Name`=@Name WHERE `SingerId`=@w_SingerId

singerStore.Delete(ctx, tx, singer)
// -> DELETE FROM `Singers` WHERE `SingerId`=@w_SingerId
```

### Want to use raw SQL?
You don't need spnr in this case! Plain spanner SDK is enough.
```go
sql := "UPDATE `Singers` SET `Name` = xx WHERE `Id` = @Id"
params := map[string]any
spannerClient.Update(tx, spanner.Statement{SQL: sql, Params: params})
```

## Embedding
spnr is also designed to use with embedding.<br/>
You can make structs to manipulate records for each table & can add any methods you want.

```go
type SingerStore struct {
	spnr.DML // use spnr.Mutation for mutation API
}

func NewSingerStore() *SingerStore {
	return &SingerStore{DML: *spnr.NewDML("Singers")}
}

// Any methods you want to add
func (s *SingerStore) GetCount(ctx context.Context, tx spnr.Transaction, cnt any) error {
	query := "select count(*) as cnt from Singers"
	return s.Reader(ctx, tx).Query(query, nil, &cnt)
}

func useSingerStore(ctx context.Context, client *spanner.Client) {
	singerStore := NewSingerStore()

	client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		// You can use all operations that spnr.DML has 
		singerStore.Insert(ctx, tx, &Singer{SingerID: "a", Name: "Alice"})
		var singer Singer
		singerStore.Reader(ctx, tx).FindOne(spanner.Key{"a"}, &singer)

		// And you can use the methods you added !!
		var cnt int
		singerStore.GetCount(ctx, tx, &cnt)

		return nil
	})
}
```

## Code generation
Tired to write struct code to map records for every table?<br/>
Don't worry! spnr provides code generation 🚀
```sh
go install github.com/kanjih/go-spnr/cmd/spnr@latest
spnr build -p {PROJECT_ID} -i {INSTANCE_ID} -d {DATABASE_ID} -n {PACKAGE_NAME} -o {OUTPUT_DIR}
```

## Helper functions
spnr provides some helper functions to reduce boilerplates.
- **`NewNullXXX`**
  - `spanner.NullString{StringVal: "a", Valid: true}` can be `spnr.NewNullString("a")`
- **`ToKeySets`**
  - You can convert slice to keysets using `spnr.ToKeySets([]string{"a", "b"})`

Love reporting issues! 

[godev-image]: https://pkg.go.dev/badge/github.com/kanjih/go-spnr
[godev-url]: https://pkg.go.dev/github.com/kanjih/go-spnr
