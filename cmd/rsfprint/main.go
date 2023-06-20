// Copyright (C) 2023 by Posit Software, PBC
package main

import (
	"log"
	"os"

	"github.com/rstudio/repository-snapshot-format/cmd/rsfprint/cmd"
)

func main() {
	log.SetOutput(os.Stdout)

	cmd.PrintCmd.SetOut(os.Stdout)
	cmd.PrintCmd.SetErr(os.Stderr)
	err := cmd.PrintCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
