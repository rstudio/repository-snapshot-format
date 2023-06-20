// Copyright (C) 2023 by Posit Software, PBC
package cmd

import (
	"bufio"
	"fmt"
	"os"

	rsf "github.com/rstudio/repository-snapshot-format"
	"github.com/spf13/cobra"
)

var PrintCmd = &cobra.Command{
	Use:   "rspm",
	Short: "Posit Package Manager",
	Long:  "Posit Package Manager administrative toolset.",
	RunE: func(cmd *cobra.Command, args []string) error {
		for _, f := range args {
			_, err := os.Stat(f)
			if err != nil {
				return fmt.Errorf("unable to read %s: %s", f, err)
			}
		}

		for _, f := range args {
			rsfFile, err := os.Open(f)
			if err != nil {
				return fmt.Errorf("unable to open %s for reading: %s", f, err)
			}
			buf := bufio.NewReader(rsfFile)
			err = rsf.Print(cmd.OutOrStdout(), buf)
			if err != nil {
				return fmt.Errorf("error printing RSF data from %s: %s", f, err)
			}
		}

		return nil
	},
}
