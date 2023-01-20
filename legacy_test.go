// Copyright (C) 2022 by Posit Software, PBC
package rsf

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/suite"
)

type LegacySuite struct {
	suite.Suite
}

func TestLegacySuite(t *testing.T) {
	suite.Run(t, &LegacySuite{})
}

func (s *LegacySuite) TestWritePackageCRAN() {

	snapshots := []fullManifestSnapshot{
		{
			Name:     "test-package",
			Snapshot: "20220228",
			JsonPath: "fedbca9876543210fedbca9876543210fedbca9876543210fedbca9876543210",
			Files: []checkpointPackageSnippet{
				{
					Summary:      "file\nsummary\nhere",
					Version:      "3.0.4",
					Sha256Sum:    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					Path:         "path/to/file.tar.gz",
					RDep:         "R >= 4.2",
					FieldSetHash: "fedbca9876543210fedbca9876543210",
					FieldSetData: "this is the fieldset data\nfor this package",
				},
				{
					Summary:      "another\nfile\nsummary\nhere",
					Version:      "3.0.5",
					Sha256Sum:    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					Path:         "another/path/to/file.tar.gz",
					RDep:         "",
					FieldSetHash: "fedbca9876543210fedbca9876543210",
					FieldSetData: "this is the fieldset data\nfor this package",
				},
			},
		},
		{
			Name:     "next-package",
			Snapshot: "20220301",
			JsonPath: "9876543210fedbca9876543210fedbca9876543210fedbca9876543210fedbca",
			Files: []checkpointPackageSnippet{
				{
					Summary:      "another\nfile\nsummary\nhere",
					Version:      "3.0.5",
					Sha256Sum:    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
					Path:         "another/path/to/file.tar.gz",
					RDep:         "",
					FieldSetHash: "fedbca9876543210fedbca9876543210",
					FieldSetData: "this is the fieldset data\nfor this package",
				},
			},
		},
		{
			Name:     "deleted-package",
			Snapshot: "20220302",
			JsonPath: "9876543210fedbca9876543210fedbca9876543210fedbca9876543210fedbca",
			Deleted:  true,
		},
	}

	buf := &bytes.Buffer{}
	w := NewWriter(buf)
	err := w.(*writer).writePackage("test-package", snapshots)
	s.Assert().Nil(err)
	s.Assert().Equal(""+
		"\x88\x03\x00\x00\f\x00\x00\x00test-package\x03\x00\x00\x0020220228"+
		"\xeb\x01\x00\x0020220301\x1c\x01\x00\x0020220302E\x00\x00\x00"+
		"fedbca9876543210fedbca9876543210fedbca9876543210fedbca9876543210"+
		"\x00\x02\x00\x00\x00\x11\x00\x00\x00file\n"+
		"summary\nhere\x05\x00\x00\x003.0.4"+
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"+
		"\b\x00\x00\x00R >= 4.2\x13\x00\x00\x00path/to/file.tar.gz"+
		"fedbca9876543210fedbca9876543210*\x00\x00\x00this is the fieldset data\n"+
		"for this package\x19\x00\x00\x00another\nfile\nsummary\n"+
		"here\x05\x00\x00\x003.0.5"+
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"+
		"\x00\x00\x00\x00\x1b\x00\x00\x00another/path/to/file.tar.gz"+
		"fedbca9876543210fedbca9876543210*\x00\x00\x00this is the fieldset data\n"+
		"for this package9876543210fedbca9876543210fedbca9876543210fedbca9876543210fedbca"+
		"\x00\x01\x00\x00\x00\x19\x00\x00\x00another\nfile\nsummary\n"+
		"here\x05\x00\x00\x003.0.5"+
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"+
		"\x00\x00\x00\x00\x1b\x00\x00\x00another/path/to/file.tar.gz"+
		"fedbca9876543210fedbca9876543210*\x00\x00\x00this is the fieldset data\n"+
		"for this package"+
		"9876543210fedbca9876543210fedbca9876543210fedbca9876543210fedbca\x01\x00\x00\x00\x00",
		buf.String())

	buf2 := &bytes.Buffer{}
	w = NewWriter(buf2)
	err = w.WriteObject(fullPackageRecord{Name: "test-package", Snapshots: snapshots})
	s.Assert().Nil(err)
	s.Assert().Equal(buf.String(), buf2.String())
	s.Assert().EqualValues(buf.Bytes(), buf2.Bytes())
}

func (s *LegacySuite) TestWritePackagePyPI() {
	snapshots := []fullManifestSnapshotPyPI{
		{
			CanonicalName: "next-package",
			ProjectName:   "Next-Package",
			Snapshot:      "20220228",
			Version:       "3.0.3",
			Summary:       "this is a summary",
		},
		{
			CanonicalName: "next-package",
			ProjectName:   "Next-Package",
			Snapshot:      "20220301",
			Version:       "3.0.4",
			Summary:       "this is a summary again",
		},
		{
			CanonicalName: "next-package",
			ProjectName:   "Next-Package",
			Snapshot:      "20220302",
			Deleted:       true,
		},
	}

	buf := &bytes.Buffer{}
	w := NewWriter(buf)
	err := w.(*writer).writePackagePyPI("next-package", "Next-Package", snapshots)
	s.Assert().Nil(err)
	s.Assert().Equal(""+
		"\x99\x00\x00\x00\f\x00\x00\x00next-package\f\x00\x00\x00Next-Package"+
		"\x03\x00\x00\x0020220228\x1f\x00\x00\x0020220301%\x00\x00\x0020220302"+
		"\t\x00\x00\x00\x00\x05\x00\x00\x003.0.3\x11\x00\x00\x00this is a summary"+
		"\x00\x05\x00\x00\x003.0.4\x17\x00\x00\x00this is a summary again"+
		"\x01\x00\x00\x00\x00\x00\x00\x00\x00",
		buf.String())

	buf2 := &bytes.Buffer{}
	w = NewWriter(buf2)
	err = w.WriteObject(fullPackageRecordPyPI{
		CanonicalName: "next-package",
		ProjectName:   "Next-Package",
		Snapshots:     snapshots,
	})
	s.Assert().Nil(err)
	s.Assert().Equal(buf.String(), buf2.String())
	s.Assert().EqualValues(buf.Bytes(), buf2.Bytes())
}
