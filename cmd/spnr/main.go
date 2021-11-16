package main

import (
	"fmt"
	"github.com/kanjih/go-spnr/handlers/build"
	"github.com/urfave/cli/v2"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Usage = "Reducing boilerplate code for spanner"
	app.EnableBashCompletion = true
	app.Commands = []*cli.Command{
		{
			Name:  "build",
			Usage: "build structs to map records",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     build.FlagNameProjectId,
					Usage:    "gcp project id",
					Required: true,
				},
				&cli.StringFlag{
					Name:     build.FlagNameInstanceName,
					Usage:    "spanner instance name",
					Required: true,
				},
				&cli.StringFlag{
					Name:     build.FlagNameDatabaseName,
					Usage:    "spanner database name",
					Required: true,
				},
				&cli.StringFlag{
					Name:     build.FlagNameOut,
					Usage:    "output folder",
					Required: true,
				},
				&cli.StringFlag{
					Name:     build.FlagNamePackageName,
					Usage:    "package name",
					Required: false,
				},
			},
			Action: build.Run,
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println("[ERROR] " + err.Error())
		os.Exit(1)
	}
}
