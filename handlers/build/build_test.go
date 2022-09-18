package build

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
)

const (
	projectName  = "test-project"
	instanceName = "test"
	databaseName = "test"
	projectID    = "projects/" + projectName
	instanceID   = projectID + "/instances/" + instanceName
)

var (
	insAdminClient *instance.InstanceAdminClient
	adminClient    *database.DatabaseAdminClient
)

func TestGenerateCode(t *testing.T) {
	codes, err := generateCode(context.Background(), projectName, instanceName, databaseName, "entity_test")
	assert.Nil(t, err)
	b, err := ioutil.ReadFile("testdata/test1.go")
	assert.Nil(t, err)
	assert.Equal(t, string(b), string(codes["Test1"]))
	b, err = ioutil.ReadFile("testdata/test2.go")
	assert.Nil(t, err)
	assert.Equal(t, string(b), string(codes["Test2"]))
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	c, err := initSpannerContainer(ctx)
	if c != nil {
		defer c.Terminate(ctx) //nolint:errcheck
	}
	if err != nil {
		panic(err)
	}
	if err = initClients(ctx); err != nil {
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

func initClients(ctx context.Context) (err error) {
	insAdminClient, err = instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	adminClient, err = database.NewDatabaseAdminClient(ctx)
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

	b1, err := ioutil.ReadFile("testdata/test1.sql")
	if err != nil {
		return err
	}
	b2, err := ioutil.ReadFile("testdata/test2.sql")
	if err != nil {
		return err
	}

	createDatabaseReq := &databasepb.CreateDatabaseRequest{
		Parent:          instanceID,
		CreateStatement: "CREATE DATABASE " + databaseName,
		ExtraStatements: []string{string(b1), string(b2)},
	}
	cdOp, err := adminClient.CreateDatabase(ctx, createDatabaseReq)
	if err != nil {
		return err
	}
	_, err = cdOp.Wait(ctx)
	return err
}
