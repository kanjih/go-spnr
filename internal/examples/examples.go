package examples

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/kanjih/go-spnr"
)

const (
	ddlSingers = `CREATE TABLE Singers (
	SingerId STRING(MAX) NOT NULL,
	Name STRING(MAX) NOT NULL,
) PRIMARY KEY (SingerId)`
	ddlAlbums = `CREATE TABLE Albums (
	SingerId STRING(MAX) NOT NULL,
	AlbumId INT64 NOT NULL,
	Title STRING(MAX),
) PRIMARY KEY (SingerId, AlbumId)`
)

type Singer struct {
	SingerID string `spanner:"SingerId" pk:"1"`
	Name     string `spanner:"Name"`
}

func looksLike(ctx context.Context, client *spanner.Client) {
	// initialize
	store := spnr.New()

	// save record (spnr provides many other functions for Mutation API & DML!)
	store.ApplyInsertOrUpdate(ctx, client, &Singer{SingerID: "a", Name: "Alice"})

	// fetch record
	var singer Singer
	store.Reader(ctx, client.Single()).FindOne(spanner.Key{"a"}, &singer)

	// fetch record using raw query
	var singers []Singer
	query := "select * from Singers where SingerId=@singerId"
	params := map[string]interface{}{"singerId": "a"}
	store.Reader(ctx, client.Single()).Query(query, params, &singers)
}

func example(ctx context.Context, client *spanner.Client) {
	// initialize
	store := spnr.New()

	// save record
	store.ApplyInsertOrUpdate(ctx, client, &Singer{"a", "Alice"})

	// fetch record
	var singer Singer
	store.Reader(ctx, client.Single()).FindOne(spanner.Key{"a"}, &singer)
}

func selectRecordsUsingPrimaryKeys(ctx context.Context, tx spnr.Transaction, store *spnr.Mutation) {
	var singer Singer
	store.Reader(ctx, tx).FindOne(spanner.Key{"a"}, &singer)

	var singers []Singer
	keys := spanner.KeySetFromKeys(spanner.Key{"a"}, spanner.Key{"b"})
	store.Reader(ctx, tx).FindAll(keys, &singers)
}

func selectOneColumnUsingPrimaryKeys(ctx context.Context, tx spnr.Transaction, store *spnr.Mutation) {
	var name string
	store.Reader(ctx, tx).FindColumnsOne(spanner.Key{"a"}, "Name", &name)

	var names []string
	keys := spanner.KeySetFromKeys(spanner.Key{"a"}, spanner.Key{"b"})
	store.Reader(ctx, tx).FindColumnsAll(keys, "Name", &names)
}

type Album struct {
	SingerID string             `spanner:"SingerId" pk:"1"`
	AlbumID  int64              `spanner:"AlbumId"`
	Title    spanner.NullString `spanner:"Title"`
}

func selectMultipleColumnsUsingPrimaryKeys(ctx context.Context, tx spnr.Transaction, store *spnr.Mutation) {
	type cols struct {
		AlbumID int64              `spanner:"AlbumId"`
		Title   spanner.NullString `spanner:"Title"`
	}
	var res cols
	store.Reader(ctx, tx).FindOne(spanner.Key{1}, &res)
}

func selectRecordsUsingQuery(ctx context.Context, tx spnr.Transaction, store *spnr.Mutation) {
	var singer Singer
	query := "select * from `Singers` where SingerId=@singerId"
	params := map[string]interface{}{"singerId": "a"}
	store.Reader(ctx, tx).QueryOne(query, params, &singer)

	var singers []Singer
	query = "select * from Singers"
	store.Reader(ctx, tx).Query(query, nil, &singers)
}

func selectOneValueUsingQuery(ctx context.Context, tx spnr.Transaction, store *spnr.Mutation) {
	var cnt int64
	query := "select count(*) as cnt from Singers"
	store.Reader(ctx, tx).QueryValue(query, nil, &cnt)
}

func mutationAPI(ctx context.Context, client *spanner.Client, tx *spanner.ReadWriteTransaction) {
	singer := &Singer{SingerID: "a", Name: "Alice"}
	singers := []Singer{{SingerID: "b", Name: "Bob"}, {SingerID: "c", Name: "Carol"}}

	store := spnr.NewMutationWithOptions(&spnr.Options{TableName: "Singers"}) // specify table name

	store.InsertOrUpdate(tx, singer)   // Insert or update
	store.InsertOrUpdate(tx, &singers) // Insert or update multiple records

	store.Update(tx, singer)   // Update
	store.Update(tx, &singers) // Update multple records

	store.Delete(tx, singer)   // Delete
	store.Delete(tx, &singers) // Delete multiple records
}

func mutationAPIApply(ctx context.Context, client *spanner.Client) {
	singer := &Singer{SingerID: "a", Name: "Alice"}

	store := spnr.NewMutationWithOptions(&spnr.Options{TableName: "Singers"})
	store.ApplyInsertOrUpdate(ctx, client, singer)
}

func DML(ctx context.Context, client *spanner.Client, tx *spanner.ReadWriteTransaction) {
	singer := &Singer{SingerID: "a", Name: "Alice"}
	singers := []Singer{{SingerID: "b", Name: "Bob"}, {SingerID: "c", Name: "Carol"}}

	store := spnr.NewDMLWithOptions(&spnr.Options{TableName: "Singers"}) // specify table name

	store.Insert(ctx, tx, singer)
	// -> INSERT INTO `Singers` (`SingerId`, `Name`) VALUES (@SingerId, @Name)
	store.Insert(ctx, tx, &singers)
	// -> INSERT INTO `Singers` (`SingerId`, `Name`) VALUES (@SingerId_0, @Name_0), (@SingerId_1, @Name_1)

	store.Update(ctx, tx, singer)
	// -> UPDATE `Singers` SET `Name`=@Name WHERE `SingerId`=@w_SingerId
	store.Update(ctx, tx, &singers)
	// -> UPDATE `Singers` SET `Name`=@Name WHERE `SingerId`=@w_SingerId
	// -> UPDATE `Singers` SET `Name`=@Name WHERE `SingerId`=@w_SingerId

	store.Delete(ctx, tx, singer)
	// -> DELETE FROM `Singers` WHERE `SingerId`=@w_SingerId
	store.Delete(ctx, tx, &singers)
	// -> DELETE FROM `Singers` WHERE (`SingerId`=@w_SingerId_0) OR (`SingerId`=@w_SingerId_1)
}

// Embedding examples
type SingerStore struct {
	spnr.DML // use spnr.Mutation for mutation API
}

func NewSingerStore() *SingerStore {
	return &SingerStore{DML: *spnr.NewDMLWithOptions(&spnr.Options{TableName: "Singers"})}
}

// Any methods you want to add
func (s *SingerStore) GetCount(ctx context.Context, tx spnr.Transaction, cnt interface{}) error {
	query := "select count(*) as cnt from Singers"
	return s.Reader(ctx, tx).QueryValue(query, nil, cnt)
}

func useSingerStore(ctx context.Context, client *spanner.Client) {
	store := NewSingerStore()

	client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		// You can use all operations that spnr.DML has 
		store.Insert(ctx, tx, &Singer{SingerID: "a", Name: "Alice"})
		var singer Singer
		store.Reader(ctx, tx).FindOne(spanner.Key{"a"}, &singer)

		// And you can use the methods you added !!
		var cnt int
		store.GetCount(ctx, tx, &cnt)

		return nil
	})
}
