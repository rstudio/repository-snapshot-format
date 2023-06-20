// Copyright (C) 2023 by Posit Software, PBC
package cmd

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type RsfPrintCommandSuite struct {
	suite.Suite
}

func TestRsfPrintCommandSuite(t *testing.T) {
	suite.Run(t, &RsfPrintCommandSuite{})
}
