package spnr

import (
	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"context"
	"fmt"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"io/ioutil"
	"math/big"
	"os"
	"testing"
	"time"
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
	dataClient     *spanner.Client
	testRecord1    = &Test{
		String:      "testId1",
		Bytes:       []byte{1},
		Int64:       10,
		Float64:     84.217403,
		Numeric:     *big.NewRat(17893, 8473),
		Date:        civil.DateOf(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
		Timestamp:   time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC),
		NullString:  NewNullString("a"),
		NullInt64:   NewNullInt64(100),
		NullNumeric: NewNullNumeric(53, 10384),
		ArrayInt64:  []int64{1, 2, 3},
		ArrayBytes:  [][]byte{{80}, {90}},
	}
	testRecord2 = &Test{
		String:     "testId2",
		Bytes:      []byte{2},
		Int64:      20,
		Date:       civil.DateOf(time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)),
		Timestamp:  time.Date(2999, 1, 1, 0, 0, 0, 0, time.UTC),
		NullString: NewNullString("b"),
		NullInt64:  NewNullInt64(200),
		ArrayInt64: []int64{4},
	}
	testRepository = NewMutation("Test")
)

type Test struct {
	String         string              `spanner:"String" pk:"1"`
	Bytes          []byte              `spanner:"Bytes"`
	Int64          int64               `spanner:"Int64" pk:"2"`
	Float64        float64             `spanner:"Float64"`
	Numeric        big.Rat             `spanner:"Numeric"`
	Bool           bool                `spanner:"Bool"`
	Date           civil.Date          `spanner:"Date"`
	Timestamp      time.Time           `spanner:"Timestamp"`
	NullString     spanner.NullString  `spanner:"NullString"`
	NullInt64      spanner.NullInt64   `spanner:"NullInt64"`
	NullFloat64    spanner.NullFloat64 `spanner:"NullFloat64"`
	NullNumeric    spanner.NullNumeric `spanner:"NullNumeric"`
	NullBool       spanner.NullBool    `spanner:"NullBool"`
	NullDate       spanner.NullDate    `spanner:"NullDate"`
	NullTimestamp  spanner.NullTime    `spanner:"NullTimestamp"`
	ArrayString    []string            `spanner:"ArrayString"`
	ArrayBytes     [][]byte            `spanner:"ArrayBytes"`
	ArrayInt64     []int64             `spanner:"ArrayInt64"`
	ArrayFloat64   []float64           `spanner:"ArrayFloat64"`
	ArrayNumeric   []big.Rat           `spanner:"ArrayNumeric"`
	ArrayBool      []bool              `spanner:"ArrayBool"`
	ArrayDate      []civil.Date        `spanner:"ArrayDate"`
	ArrayTimestamp []time.Time         `spanner:"ArrayTimestamp"`
}

type TestOrderChanged struct {
	ArrayString    []string            `spanner:"ArrayString"`
	ArrayBytes     [][]byte            `spanner:"ArrayBytes"`
	ArrayInt64     []int64             `spanner:"ArrayInt64"`
	ArrayFloat64   []float64           `spanner:"ArrayFloat64"`
	ArrayNumeric   []big.Rat           `spanner:"ArrayNumeric"`
	ArrayBool      []bool              `spanner:"ArrayBool"`
	ArrayDate      []civil.Date        `spanner:"ArrayDate"`
	ArrayTimestamp []time.Time         `spanner:"ArrayTimestamp"`
	String         string              `spanner:"String" pk:"1"`
	Bytes          []byte              `spanner:"Bytes"`
	Int64          int64               `spanner:"Int64"`
	Float64        float64             `spanner:"Float64"`
	Numeric        big.Rat             `spanner:"Numeric"`
	Bool           bool                `spanner:"Bool"`
	Date           civil.Date          `spanner:"Date"`
	Timestamp      time.Time           `spanner:"Timestamp"`
	NullString     spanner.NullString  `spanner:"NullString"`
	NullInt64      spanner.NullInt64   `spanner:"NullInt64"`
	NullFloat64    spanner.NullFloat64 `spanner:"NullFloat64"`
	NullNumeric    spanner.NullNumeric `spanner:"NullNumeric"`
	NullBool       spanner.NullBool    `spanner:"NullBool"`
	NullDate       spanner.NullDate    `spanner:"NullDate"`
	NullTimestamp  spanner.NullTime    `spanner:"NullTimestamp"`
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
	dataClient, err = spanner.NewClient(ctx, databaseId)
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

	b, err := ioutil.ReadFile("testdata/test.sql")
	if err != nil {
		return err
	}
	createDatabaseReq := &databasepb.CreateDatabaseRequest{
		Parent:          instanceID,
		CreateStatement: "CREATE DATABASE " + databaseName,
		ExtraStatements: []string{string(b)},
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
