// Copyright (C) 2023 by Posit Software, PBC
package main

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type RsfPrintMainCommandSuite struct {
	suite.Suite
}

func TestRsfPrintMainCommandSuite(t *testing.T) {
	suite.Run(t, &RsfPrintMainCommandSuite{})
}
