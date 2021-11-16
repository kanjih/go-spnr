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
	singerStore := spnr.New("Singers") // specify table name

	// save record (spnr provides many other functions for Mutation API & DML!)
	singerStore.ApplyInsertOrUpdate(ctx, client, &Singer{SingerID: "a", Name: "Alice"})

	// fetch record
	var singer Singer
	singerStore.Reader(ctx, client.Single()).FindOne(spanner.Key{"a"}, &singer)

	// fetch record using raw query
	var singers []Singer
	query := "select * from Singers where SingerId=@singerId"
	params := map[string]interface{}{"singerId": "a"}
	singerStore.Reader(ctx, client.Single()).Query(query, params, &singers)
}

func example(ctx context.Context, client *spanner.Client) {
	// initialize
	singerStore := spnr.New("Singers")

	// save record
	singerStore.ApplyInsertOrUpdate(ctx, client, &Singer{"a", "Alice"})

	// fetch record
	var singer Singer
	singerStore.Reader(ctx, client.Single()).FindOne(spanner.Key{"a"}, &singer)
}

func selectRecordsUsingPrimaryKeys(ctx context.Context, tx spnr.Transaction, singerStore *spnr.Mutation) {
	var singer Singer
	singerStore.Reader(ctx, tx).FindOne(spanner.Key{"a"}, &singer)

	var singers []Singer
	keys := spanner.KeySetFromKeys(spanner.Key{"a"}, spanner.Key{"b"})
	singerStore.Reader(ctx, tx).FindAll(keys, &singers)
}

func selectOneColumnUsingPrimaryKeys(ctx context.Context, tx spnr.Transaction, singerStore *spnr.Mutation) {
	var name string
	singerStore.Reader(ctx, tx).GetColumn(spanner.Key{"a"}, "Name", &name)

	var names []string
	keys := spanner.KeySetFromKeys(spanner.Key{"a"}, spanner.Key{"b"})
	singerStore.Reader(ctx, tx).GetColumnAll(keys, "Name", &names)
}

type Album struct {
	SingerID string             `spanner:"SingerId" pk:"1"`
	AlbumID  int64              `spanner:"AlbumId"`
	Title    spanner.NullString `spanner:"Title"`
}

func selectMultipleColumnsUsingPrimaryKeys(ctx context.Context, tx spnr.Transaction, albumStore *spnr.Mutation) {
	type cols struct {
		AlbumID int64              `spanner:"AlbumId"`
		Title   spanner.NullString `spanner:"Title"`
	}
	var res cols
	albumStore.Reader(ctx, tx).FindOne(spanner.Key{1}, &res)
}

func selectRecordsUsingQuery(ctx context.Context, tx spnr.Transaction, singerStore *spnr.Mutation) {
	var singer Singer
	query := "select * from `Singers` where SingerId=@singerId"
	params := map[string]interface{}{"singerId": "a"}
	singerStore.Reader(ctx, tx).QueryOne(query, params, &singer)

	var singers []Singer
	query = "select * from Singers"
	singerStore.Reader(ctx, tx).Query(query, nil, &singers)
}

func selectOneValueUsingQuery(ctx context.Context, tx spnr.Transaction, singerStore *spnr.Mutation) {
	var cnt int64
	query := "select count(*) as cnt from Singers"
	singerStore.Reader(ctx, tx).QueryValue(query, nil, &cnt)
}

func mutationAPI(ctx context.Context, client *spanner.Client, tx *spanner.ReadWriteTransaction) {
	singer := &Singer{SingerID: "a", Name: "Alice"}
	singers := []Singer{{SingerID: "b", Name: "Bob"}, {SingerID: "c", Name: "Carol"}}

	singerStore := spnr.New("Singers") // specify table name

	singerStore.InsertOrUpdate(tx, singer)   // Insert or update
	singerStore.InsertOrUpdate(tx, &singers) // Insert or update multiple records

	singerStore.Update(tx, singer)   // Update
	singerStore.Update(tx, &singers) // Update multple records

	singerStore.Delete(tx, singer)   // Delete
	singerStore.Delete(tx, &singers) // Delete multiple records
}

func mutationAPIApply(ctx context.Context, client *spanner.Client) {
	singer := &Singer{SingerID: "a", Name: "Alice"}

	singerStore := spnr.New("Singers")
	singerStore.ApplyInsertOrUpdate(ctx, client, singer)
}

func DML(ctx context.Context, client *spanner.Client, tx *spanner.ReadWriteTransaction) {
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
	singerStore.Delete(ctx, tx, &singers)
	// -> DELETE FROM `Singers` WHERE (`SingerId`=@w_SingerId_0) OR (`SingerId`=@w_SingerId_1)
}

// Embedding examples
type SingerStore struct {
	spnr.DML // use spnr.Mutation for mutation API
}

func NewSingerStore() *SingerStore {
	return &SingerStore{DML: *spnr.NewDML("Singers")}
}

// Any methods you want to add
func (s *SingerStore) GetCount(ctx context.Context, tx spnr.Transaction, cnt interface{}) error {
	query := "select count(*) as cnt from Singers"
	return s.Reader(ctx, tx).QueryValue(query, nil, cnt)
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
