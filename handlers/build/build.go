package build

import (
	"context"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"os"
	"strings"
)

const (
	FlagNameProjectId    = "p"
	FlagNameInstanceName = "i"
	FlagNameDatabaseName = "d"
	FlagNameOut          = "o"
	FlagNamePackageName  = "n"
)

func Run(c *cli.Context) error {
	out := c.String(FlagNameOut)
	if _, err := os.Stat(out); errors.Is(err, os.ErrNotExist) {
		return errors.Errorf("%s doesn't exist", out)
	}
	if !strings.HasSuffix(out, "/") {
		out += "/"
	}

	packageName := c.String(FlagNamePackageName)
	if packageName == "" {
		packageName = "entity"
	}

	codes, err := generateCode(
		c.Context,
		c.String(FlagNameProjectId),
		c.String(FlagNameInstanceName),
		c.String(FlagNameDatabaseName),
		packageName,
	)
	if err != nil {
		return err
	}

	for tableName, code := range codes {
		if err := writeFile(out, tableName, code); err != nil {
			return err
		}
	}
	return nil
}

func generateCode(ctx context.Context, projectId, instanceName, dbName, packageName string) (map[string][]byte, error) {
	columns, err := fetchColumns(ctx, projectId, instanceName, dbName)
	if err != nil {
		return nil, err
	}
	return generate(packageName, columns)
}

func writeFile(dirName, tableName string, code []byte) error {
	f, err := os.Create(dirName + strings.ToLower(tableName) + ".go")
	if err != nil {
		return errors.New("Got error during create file")
	}
	defer f.Close()
	_, err = f.Write(code)
	return err
}
