package examples

import (
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"context"
	"fmt"
	"github.com/kanjih/go-spnr"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"gotest.tools/assert"
	"os"
	"testing"
)

const (
	instanceName = "test"
	databaseName = "test"
	projectID    = "projects/test-project"
	instanceID   = projectID + "/instances/" + instanceName
	databaseID   = instanceID + "/databases/" + databaseName
)

var (
	insAdminClient *instance.InstanceAdminClient
	adminClient    *database.DatabaseAdminClient
	client         *spanner.Client
	singer         = &Singer{SingerID: "a", Name: "Alice"}
	singers        = []Singer{{SingerID: "b", Name: "Bob"}, {SingerID: "c", Name: "Carol"}}
)

func TestExample(t *testing.T) {
	singer := &Singer{SingerID: "a", Name: "Alice"}
	ctx := context.Background()
	singerStore := spnr.New("Singers")
	_, err := singerStore.ApplyInsertOrUpdate(ctx, client, singer)
	assert.NilError(t, err)

	var fetched Singer
	err = singerStore.Reader(ctx, client.Single()).FindOne(spanner.Key{"a"}, &fetched)
	assert.NilError(t, err)
	assert.Equal(t, *singer, fetched)

	var singers []Singer
	query := "select * from Singers where SingerId=@singerId"
	params := map[string]interface{}{"singerId": "a"}
	err = singerStore.Reader(ctx, client.Single()).Query(query, params, &singers)
	assert.NilError(t, err)
	assert.Equal(t, 1, len(singers))
	assert.Equal(t, *singer, singers[0])
	assert.NilError(t, deleteAllSingers())

}

func TestSelectRecordsUsingPrimaryKeys(t *testing.T) {
	ctx := context.Background()
	singerStore := spnr.New("Singers")
	_, err := singerStore.ApplyInsertOrUpdate(ctx, client, singer)
	assert.NilError(t, err)
	_, err = singerStore.ApplyInsertOrUpdate(ctx, client, &singers)
	assert.NilError(t, err)

	var fetched Singer
	err = singerStore.Reader(ctx, client.Single()).FindOne(spanner.Key{"a"}, &fetched)
	assert.NilError(t, err)
	assert.Equal(t, *singer, fetched)

	var fetchedSingers []Singer
	keys := spanner.KeySetFromKeys(spanner.Key{"a"}, spanner.Key{"b"})
	err = singerStore.Reader(ctx, client.Single()).FindAll(keys, &fetchedSingers)
	assert.NilError(t, err)
	assert.Equal(t, *singer, fetchedSingers[0])
	assert.Equal(t, singers[0], fetchedSingers[1])

	var name string
	err = singerStore.Reader(ctx, client.Single()).GetColumn(spanner.Key{"a"}, "Name", &name)
	assert.NilError(t, err)
	assert.Equal(t, singer.Name, name)

	var names []string
	err = singerStore.Reader(ctx, client.Single()).GetColumnAll(keys, "Name", &names)
	assert.NilError(t, err)
	assert.Equal(t, singer.Name, names[0])
	assert.Equal(t, singers[0].Name, names[1])

	assert.NilError(t, deleteAllSingers())
}

func TestSelectMultipleColumnsUsingPrimaryKeys(t *testing.T) {
	ctx := context.Background()
	album := &Album{
		SingerID: "a",
		AlbumID:  1,
		Title:    spnr.NewNullString("test"),
	}
	albumStore := spnr.NewMutationWithOptions("Albums", &spnr.Options{LogEnabled: true})
	_, err := albumStore.ApplyInsertOrUpdate(ctx, client, album)
	assert.NilError(t, err)

	type cols struct {
		AlbumID int64              `spanner:"AlbumId"`
		Title   spanner.NullString `spanner:"Title"`
	}
	var res cols
	err = albumStore.Reader(ctx, client.Single()).FindOne(spanner.Key{"a", 1}, &res)
	assert.NilError(t, err)
	assert.Equal(t, album.AlbumID, res.AlbumID)
	assert.Equal(t, album.Title, res.Title)
}

func TestSelectRecordsUsingQuery(t *testing.T) {
	ctx := context.Background()
	singerStore := spnr.New("Singers")
	_, err := singerStore.ApplyInsertOrUpdate(ctx, client, singer)
	assert.NilError(t, err)
	_, err = singerStore.ApplyInsertOrUpdate(ctx, client, &singers)
	assert.NilError(t, err)

	var fetched Singer
	query := "select * from `Singers` where SingerId=@singerId"
	params := map[string]interface{}{"singerId": "a"}
	err = singerStore.Reader(ctx, client.Single()).QueryOne(query, params, &fetched)
	assert.NilError(t, err)
	assert.Equal(t, *singer, fetched)

	var fetchedSingers []Singer
	query = "select * from Singers order by SingerId"
	err = singerStore.Reader(ctx, client.Single()).Query(query, nil, &fetchedSingers)
	assert.NilError(t, err)
	assert.Equal(t, *singer, fetchedSingers[0])
	assert.Equal(t, singers[0], fetchedSingers[1])
	assert.Equal(t, singers[1], fetchedSingers[2])

	assert.NilError(t, deleteAllSingers())
}

func TestSelectOneValueUsingQuery(t *testing.T) {
	ctx := context.Background()
	singerStore := spnr.New("Singers")
	_, err := singerStore.ApplyInsertOrUpdate(ctx, client, &singers)
	assert.NilError(t, err)

	var cnt int64
	query := "select count(*) as cnt from Singers"
	err = singerStore.Reader(ctx, client.Single()).QueryValue(query, nil, &cnt)
	assert.NilError(t, err)
	assert.Equal(t, int64(2), cnt)

	assert.NilError(t, deleteAllSingers())
}

func TestMutationAPI(t *testing.T) {
	ctx := context.Background()
	singerStore := spnr.New("Singers")

	client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		err := singerStore.InsertOrUpdate(tx, singer)
		assert.NilError(t, err)
		err = singerStore.InsertOrUpdate(tx, &singers)
		assert.NilError(t, err)
		var cnt int64
		query := "select count(*) as cnt from Singers"
		err = singerStore.Reader(ctx, tx).QueryValue(query, nil, &cnt)
		assert.NilError(t, err)
		assert.Equal(t, int64(0), cnt)
		return nil
	})
	var cnt int64
	query := "select count(*) as cnt from Singers"
	err := singerStore.Reader(ctx, client.Single()).QueryValue(query, nil, &cnt)
	assert.NilError(t, err)
	assert.Equal(t, int64(3), cnt)

	var fetched Singer
	updatedSinger := *singer
	updatedSinger.Name = "Mallory"

	var fetchedSingers []Singer
	updatedSinger1 := singers[0]
	updatedSinger2 := singers[1]
	updatedSinger1.Name = "Marvin"
	updatedSinger2.Name = "Mallet"
	keySet := spanner.KeySetFromKeys(spanner.Key{singers[0].SingerID}, spanner.Key{singers[1].SingerID})

	client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		err = singerStore.Update(tx, &updatedSinger)
		assert.NilError(t, err)

		err = singerStore.Reader(ctx, tx).FindOne(spanner.Key{singer.SingerID}, &fetched)
		assert.NilError(t, err)
		assert.Equal(t, *singer, fetched)

		err = singerStore.Update(tx, &([]Singer{updatedSinger1, updatedSinger2}))
		assert.NilError(t, err)

		err = singerStore.Reader(ctx, tx).FindAll(keySet, &fetchedSingers)
		assert.NilError(t, err)
		assert.Equal(t, singers[0], fetchedSingers[0])
		assert.Equal(t, singers[1], fetchedSingers[1])
		return nil
	})

	err = singerStore.Reader(ctx, client.Single()).FindOne(spanner.Key{singer.SingerID}, &fetched)
	assert.NilError(t, err)
	assert.Equal(t, updatedSinger, fetched)

	fetchedSingers = nil
	err = singerStore.Reader(ctx, client.Single()).FindAll(keySet, &fetchedSingers)
	assert.NilError(t, err)
	assert.Equal(t, updatedSinger1, fetchedSingers[0])
	assert.Equal(t, updatedSinger2, fetchedSingers[1])

	client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		err := singerStore.Delete(tx, singer)
		assert.NilError(t, err)
		err = singerStore.Reader(ctx, tx).FindOne(spanner.Key{singer.SingerID}, &fetched)
		assert.NilError(t, err)
		return nil
	})
	err = singerStore.Reader(ctx, client.Single()).FindOne(spanner.Key{singer.SingerID}, &fetched)
	assert.Equal(t, spnr.ErrNotFound, err)

	client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		err := singerStore.Delete(tx, &singers)
		assert.NilError(t, err)
		fetchedSingers = nil
		err = singerStore.Reader(ctx, tx).FindAll(keySet, &fetchedSingers)
		assert.NilError(t, err)
		assert.Equal(t, 2, len(fetchedSingers))
		return nil
	})
	fetchedSingers = nil
	err = singerStore.Reader(ctx, client.Single()).FindAll(keySet, &fetchedSingers)
	assert.NilError(t, err)
	assert.Equal(t, 0, len(fetchedSingers))

	assert.NilError(t, deleteAllSingers())
}

func TestDML(t *testing.T) {
	singerStore := spnr.NewDMLWithOptions("Singers", &spnr.Options{LogEnabled: true})
	client.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := singerStore.Insert(ctx, tx, singer)
		assert.NilError(t, err)
		var fetched Singer
		err = singerStore.Reader(ctx, tx).FindOne(spanner.Key{singer.SingerID}, &fetched)
		assert.NilError(t, err)
		assert.Equal(t, singer.SingerID, fetched.SingerID)
		assert.Equal(t, singer.Name, fetched.Name)

		_, err = singerStore.Insert(ctx, tx, &singers)
		assert.NilError(t, err)
		err = singerStore.Reader(ctx, tx).FindOne(spanner.Key{singers[0].SingerID}, &fetched)
		assert.NilError(t, err)
		assert.Equal(t, singers[0].SingerID, fetched.SingerID)
		assert.Equal(t, singers[0].Name, fetched.Name)
		err = singerStore.Reader(ctx, tx).FindOne(spanner.Key{singers[1].SingerID}, &fetched)
		assert.NilError(t, err)
		assert.Equal(t, singers[1].SingerID, fetched.SingerID)
		assert.Equal(t, singers[1].Name, fetched.Name)

		updatedSinger := *singer
		updatedSinger.Name = "Mallory"
		_, err = singerStore.Update(ctx, tx, &updatedSinger)
		assert.NilError(t, err)
		err = singerStore.Reader(ctx, tx).FindOne(spanner.Key{updatedSinger.SingerID}, &fetched)
		assert.NilError(t, err)
		assert.Equal(t, updatedSinger.Name, fetched.Name)

		updatedSinger1 := singers[0]
		updatedSinger1.Name = "Marvin"
		updatedSinger2 := singers[1]
		updatedSinger2.Name = "Mallet"

		_, err = singerStore.Update(ctx, tx, &([]Singer{updatedSinger1, updatedSinger2}))
		assert.NilError(t, err)
		var fetchedSingers []Singer
		keySet := spanner.KeySetFromKeys(spanner.Key{"b"}, spanner.Key{"c"})
		err = singerStore.Reader(ctx, tx).FindAll(keySet, &fetchedSingers)
		assert.NilError(t, err)
		assert.Equal(t, updatedSinger1.Name, fetchedSingers[0].Name)
		assert.Equal(t, updatedSinger2.Name, fetchedSingers[1].Name)

		_, err = singerStore.Delete(ctx, tx, singer)
		assert.NilError(t, err)
		err = singerStore.Reader(ctx, tx).FindOne(spanner.Key{updatedSinger.SingerID}, &fetched)
		assert.Equal(t, spnr.ErrNotFound, err)

		_, err = singerStore.Delete(ctx, tx, &singers)
		assert.NilError(t, err)
		fetchedSingers = nil
		err = singerStore.Reader(ctx, tx).FindAll(keySet, &fetchedSingers)
		assert.Equal(t, 0, len(fetchedSingers))

		return nil
	})

	assert.NilError(t, deleteAllSingers())
}

func TestSingerStore(t *testing.T) {
	singerStore := NewSingerStore()
	client.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{SQL: "delete from Singers where true"})
		assert.NilError(t, err)

		_, err = singerStore.Insert(ctx, tx, singer)
		assert.NilError(t, err)
		var cnt int64
		err = singerStore.GetCount(ctx, tx, &cnt)
		assert.NilError(t, err)
		assert.Equal(t, int64(1), cnt)

		_, err = singerStore.Delete(ctx, tx, singer)
		assert.NilError(t, err)

		return nil
	})

}

func TestMain(m *testing.M) {
	ctx := context.Background()
	c, err := initSpannerContainer(ctx)
	if c != nil {
		defer c.Terminate(ctx)
	}
	if err != nil {
		panic(err)
	}
	if err = initClients(ctx, databaseID); err != nil {
		panic(err)
	}
	if err = initDatabase(ctx); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func initSpannerContainer(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        "gcr.io/cloud-spanner-emulator/emulator:1.3.0",
		ExposedPorts: []string{"9010/tcp"},
		WaitingFor:   wait.ForLog("gateway.go:142: gRPC server listening at 0.0.0.0:9010"),
	}
	spannerC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}
	h, err := spannerC.Host(ctx)
	if err != nil {
		return nil, err
	}
	p, err := spannerC.MappedPort(ctx, "9010")
	if err != nil {
		return nil, err
	}
	return spannerC, os.Setenv("SPANNER_EMULATOR_HOST", fmt.Sprintf("%s:%s", h, p.Port()))
}

func initClients(ctx context.Context, databaseId string) (err error) {
	insAdminClient, err = instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	adminClient, err = database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	client, err = spanner.NewClient(ctx, databaseId)
	return err
}

func initDatabase(ctx context.Context) (err error) {
	createInstanceReq := &instancepb.CreateInstanceRequest{
		Parent: projectID,
		Instance: &instancepb.Instance{
			Name:        instanceID,
			Config:      projectID + "/instanceConfigs/test",
			DisplayName: instanceName,
			NodeCount:   1,
		},
		InstanceId: instanceName,
	}
	ciOp, err := insAdminClient.CreateInstance(ctx, createInstanceReq)
	if err != nil {
		return err
	}
	if _, err = ciOp.Wait(ctx); err != nil {
		return err
	}

	createDatabaseReq := &databasepb.CreateDatabaseRequest{
		Parent:          instanceID,
		CreateStatement: "CREATE DATABASE " + databaseName,
		ExtraStatements: []string{ddlSingers, ddlAlbums},
	}
	cdOp, err := adminClient.CreateDatabase(ctx, createDatabaseReq)
	if err != nil {
		return err
	}
	_, err = cdOp.Wait(ctx)
	if err != nil {
		return err
	}

	return err
}

func deleteAllSingers() error {
	_, err := client.ReadWriteTransaction(context.Background(), func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		_, err := tx.Update(ctx, spanner.Statement{SQL: "delete from Singers where true"})
		return err
	})
	return err
}
