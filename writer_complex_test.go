// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/suite"
)

type WriterComplexSuite struct {
	suite.Suite
}

func TestWriterComplexSuite(t *testing.T) {
	suite.Run(t, &WriterComplexSuite{})
}

type Classifier struct {
	Name   string   `rsf:"name"`
	Type   int      `rsf:"type"`
	Values []string `rsf:"values"`
}

type FullManifestSnapshotPyPI struct {
	// These two fields are used during the manifest generation process to
	// track information, but they're ignored when writing the manifest.
	CanonicalName string `json:"-" rsf:"-"`
	ProjectName   string `json:"-" rsf:"-"`

	Description string `rsf:"description"`
	Deleted     bool   `json:"d,omitempty" rsf:"deleted"`
	Snapshot    string `json:"s" rsf:"snapshot,skip,fixed:10"`
	Version     string `json:"v,omitempty" rsf:"version"`
	Summary     string `json:"u,omitempty" rsf:"summary"`
	License     string `rsf:"license"`
}

type FullPackageRecordPyPI struct {
	HomePage      string                     `rsf:"homepage"`
	CanonicalName string                     `rsf:"cname"`
	ProjectName   string                     `rsf:"pname"`
	Classifiers   []Classifier               `rsf:"classifiers"`
	Author        string                     `rsf:"author"`
	Snapshots     []FullManifestSnapshotPyPI `rsf:"snapshots,index:snapshot"`
	Popularity    int64                      `rsf:"popularity"`
}

var testComplexData = []FullPackageRecordPyPI{
	{
		HomePage:      "http://homepage.com",
		CanonicalName: "numpy",
		ProjectName:   "Numpy",
		Classifiers: []Classifier{
			{
				Name:   "License",
				Type:   2,
				Values: []string{"one", "two", "three"},
			},
			{
				Name: "Usage",
				Type: 1,
			},
		},
		Author: "an-author",
		Snapshots: []FullManifestSnapshotPyPI{
			{
				CanonicalName: "ignored",
				ProjectName:   "ignored",
				Description:   "The description of numpy",
				Deleted:       false,
				Snapshot:      "2020-10-11",
				Version:       "3.0.3",
				Summary:       "numpy summary",
				License:       "MIT",
			}, {
				CanonicalName: "ignored",
				ProjectName:   "ignored",
				Description:   "Older description of numpy",
				Deleted:       false,
				Snapshot:      "2020-10-10",
				Version:       "3.0.2",
				Summary:       "numpy summary",
				License:       "MIT",
			}, {
				CanonicalName: "ignored",
				ProjectName:   "ignored",
				Deleted:       true,
				Snapshot:      "2020-10-09",
			},
		},
		Popularity: 55,
	},
	{
		HomePage:      "http://django-home.com",
		CanonicalName: "django",
		ProjectName:   "Django",
		Classifiers: []Classifier{
			{
				Name:   "License",
				Type:   2,
				Values: []string{"one", "two"},
			},
			{
				Name: "Usage",
				Type: 1,
			},
		},
		Author: "be-an-author",
		Snapshots: []FullManifestSnapshotPyPI{
			{
				CanonicalName: "ignored",
				ProjectName:   "ignored",
				Description:   "The description of django",
				Deleted:       false,
				Snapshot:      "2020-10-11",
				Version:       "3.0.3",
				Summary:       "django summary",
				License:       "MIT",
			}, {
				CanonicalName: "ignored",
				ProjectName:   "ignored",
				Deleted:       true,
				Snapshot:      "2020-10-09",
			},
		},
		Popularity: 55,
	},
}

// TestWriteComplexObject tests writing `testComplexData` using the current Version2
// index format.
func (s *WriterComplexSuite) TestWriteComplexObject() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

	var totalSz int
	for _, obj := range testComplexData {
		sz, err := w.WriteObject(obj)
		s.Assert().Nil(err)
		totalSz += sz
	}
	// Object should use 888 bytes.
	s.Assert().Equal(888, totalSz)
	s.Assert().Len(buf.Bytes(), 888)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object index header
		//
		// Index version 2
		0x0, 0x8, 0x32,
		//
		// Index size
		0xa, 0x1, 0x0, 0x0,
		//
		// Fields Index
		//
		// "homepage"
		0x8, 0x0, 0x0, 0x0,
		0x68, 0x6f, 0x6d, 0x65, 0x70, 0x61, 0x67, 0x65,
		// type
		0x1, 0x0, 0x0, 0x0,
		//
		// "cname"
		0x5, 0x0, 0x0, 0x0,
		0x63, 0x6e, 0x61, 0x6d, 0x65,
		0x1, 0x0, 0x0, 0x0,
		//
		// "pname"
		0x5, 0x0, 0x0, 0x0,
		0x70, 0x6e, 0x61, 0x6d, 0x65,
		0x1, 0x0, 0x0, 0x0,
		//
		// "classifiers" (array)
		0xb, 0x0, 0x0, 0x0,
		0x63, 0x6c, 0x61, 0x73, 0x73, 0x69, 0x66, 0x69, 0x65, 0x72, 0x73,
		// array type
		0x4, 0x0, 0x0, 0x0,
		// not indexed
		0x0,
		// array subtype
		0x19, 0x0, 0x0, 0x0,
		// 3 subfields
		0x3, 0x0, 0x0, 0x0,
		//
		// "classifiers" - "name"
		0x4, 0x0, 0x0, 0x0,
		0x6e, 0x61, 0x6d, 0x65,
		0x1, 0x0, 0x0, 0x0,
		//
		// "classifiers" - "type" (int)
		0x4, 0x0, 0x0, 0x0,
		0x74, 0x79, 0x70, 0x65,
		0x7, 0x0, 0x0, 0x0,
		//
		// "classifiers" - "values" (array)
		0x6, 0x0, 0x0, 0x0,
		0x76, 0x61, 0x6c, 0x75, 0x65, 0x73,
		0x4, 0x0, 0x0, 0x0,
		// not indexed
		0x0,
		// Subtype
		0x18, 0x0, 0x0, 0x0,
		// string array has zero subfields
		0x0, 0x0, 0x0, 0x0,
		//
		// "author"
		0x6, 0x0, 0x0, 0x0,
		0x61, 0x75, 0x74, 0x68, 0x6f, 0x72,
		0x1, 0x0, 0x0, 0x0,
		//
		// "snapshots" (array)
		0x9, 0x0, 0x0, 0x0,
		0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x73,
		// "array" type
		0x4, 0x0, 0x0, 0x0,
		// "snapshots" array is indexed
		0x1,
		// "snapshots" array index type
		0x18, 0x0, 0x0, 0x0,
		// "snapshots" array field size 10
		0xa, 0x0, 0x0, 0x0,
		// "snapshots" array field type (struct)
		0x19, 0x0, 0x0, 0x0,
		// 5 subfields for snapshots array
		0x5, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "description"
		0xb, 0x0, 0x0, 0x0,
		0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e,
		0x1, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "deleted" (bool)
		0x7, 0x0, 0x0, 0x0,
		0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64,
		0x3, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "version"
		0x7, 0x0, 0x0, 0x0,
		0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
		0x1, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "summary"
		0x7, 0x0, 0x0, 0x0,
		0x73, 0x75, 0x6d, 0x6d, 0x61, 0x72, 0x79,
		0x1, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "license"
		0x7, 0x0, 0x0, 0x0,
		0x6c, 0x69, 0x63, 0x65, 0x6e, 0x73, 0x65,
		0x1, 0x0, 0x0, 0x0,
		//
		// "popularity" (int)
		0xa, 0x0, 0x0, 0x0,
		0x70, 0x6f, 0x70, 0x75, 0x6c, 0x61, 0x72, 0x69, 0x74, 0x79,
		0x7, 0x0, 0x0, 0x0,
		//
		// -- end index --
		// -- start object --
		//
		// Object size
		0x5c, 0x1, 0x0, 0x0,
		//
		// "http://homepage.com"
		0x13, 0x0, 0x0, 0x0,
		0x68, 0x74, 0x74, 0x70, 0x3a, 0x2f, 0x2f, 0x68, 0x6f, 0x6d,
		0x65, 0x70, 0x61, 0x67, 0x65, 0x2e, 0x63, 0x6f, 0x6d,
		//
		// "numpy"
		0x5, 0x0, 0x0, 0x0,
		0x6e, 0x75, 0x6d, 0x70, 0x79,
		//
		// "Numpy"
		0x5, 0x0, 0x0, 0x0,
		0x4e, 0x75, 0x6d, 0x70, 0x79,
		//
		// -- start array --
		//
		// "classifiers" array size
		0x57, 0x0, 0x0, 0x0,
		// array length
		0x2, 0x0, 0x0, 0x0,
		// "License"
		0x7, 0x0, 0x0, 0x0,
		0x4c, 0x69, 0x63, 0x65, 0x6e, 0x73, 0x65,
		// 2
		0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// "values" array size
		0x1f, 0x0, 0x0, 0x0,
		// array length
		0x3, 0x0, 0x0, 0x0,
		// "one"
		0x3, 0x0, 0x0, 0x0,
		0x6f, 0x6e, 0x65,
		// "two"
		0x3, 0x0, 0x0, 0x0,
		0x74, 0x77, 0x6f,
		// "three"
		0x5, 0x0, 0x0, 0x0,
		0x74, 0x68, 0x72, 0x65, 0x65,
		//
		// "Usage"
		0x5, 0x0, 0x0, 0x0,
		0x55, 0x73, 0x61, 0x67, 0x65,
		// 1
		0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// "values" array size
		0x8, 0x0, 0x0, 0x0,
		// zero len array
		0x0, 0x0, 0x0, 0x0,
		//
		// -- end array --
		//
		// "an-author"
		0x9, 0x0, 0x0, 0x0,
		0x61, 0x6e, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72,
		//
		// -- start "snapshots" array
		//
		// Array size
		0xc1, 0x0, 0x0, 0x0,
		// Array length
		0x3, 0x0, 0x0, 0x0,
		//
		// -- Array index --
		//
		// "2020-10-11"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x31, 0x31,
		0x3e, 0x0, 0x0, 0x0,
		// "2020-10-10"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x31, 0x30,
		0x40, 0x0, 0x0, 0x0,
		// "2020-10-09"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x30, 0x39,
		0x11, 0x0, 0x0, 0x0,
		//
		// -- end array index --
		// -- array data --
		//
		// "The description of numpy"
		0x18, 0x0, 0x0, 0x0,
		0x54, 0x68, 0x65, 0x20, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69,
		0x70, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x6f, 0x66, 0x20, 0x6e,
		0x75, 0x6d, 0x70, 0x79,
		//
		// false
		0x0,
		//
		// "3.0.3"
		0x5, 0x0, 0x0, 0x0,
		0x33, 0x2e, 0x30, 0x2e, 0x33,
		//
		// "numpy summary"
		0xd, 0x0, 0x0, 0x0,
		0x6e, 0x75, 0x6d, 0x70, 0x79, 0x20, 0x73, 0x75, 0x6d, 0x6d, 0x61, 0x72, 0x79,
		//
		// "MIT"
		0x3, 0x0, 0x0, 0x0,
		0x4d, 0x49, 0x54,
		//
		// ------
		//
		// "Older description of numpy"
		0x1a, 0x0, 0x0, 0x0,
		0x4f, 0x6c, 0x64, 0x65, 0x72, 0x20, 0x64, 0x65, 0x73, 0x63,
		0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x6f, 0x66,
		0x20, 0x6e, 0x75, 0x6d, 0x70, 0x79,
		// false
		0x0,
		// "3.0.2"
		0x5, 0x0, 0x0, 0x0,
		0x33, 0x2e, 0x30, 0x2e, 0x32,
		// "numpy summary"
		0xd, 0x0, 0x0, 0x0,
		0x6e, 0x75, 0x6d, 0x70, 0x79, 0x20, 0x73, 0x75, 0x6d, 0x6d, 0x61, 0x72, 0x79,
		// "MIT"
		0x3, 0x0, 0x0, 0x0,
		0x4d, 0x49, 0x54,
		//
		// ------
		//
		// no description
		0x0, 0x0, 0x0, 0x0,
		// true
		0x1,
		// no version
		0x0, 0x0, 0x0, 0x0,
		// no summary
		0x0, 0x0, 0x0, 0x0,
		// no license
		0x0, 0x0, 0x0, 0x0,
		//
		// -- end array --
		//
		// 55 (popularity)
		0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		//
		// -- end object --
		//
		// Next object size
		0xf, 0x1, 0x0, 0x0,
		//
		// "http://django-home.com"
		0x16, 0x0, 0x0, 0x0,
		0x68, 0x74, 0x74, 0x70, 0x3a, 0x2f, 0x2f, 0x64, 0x6a, 0x61,
		0x6e, 0x67, 0x6f, 0x2d, 0x68, 0x6f, 0x6d, 0x65, 0x2e, 0x63,
		0x6f, 0x6d,
		//
		// "django",
		0x6, 0x0, 0x0, 0x0,
		0x64, 0x6a, 0x61, 0x6e, 0x67, 0x6f,
		//
		// "Django"
		0x6, 0x0, 0x0, 0x0,
		0x44, 0x6a, 0x61, 0x6e, 0x67, 0x6f,
		//
		// -- start array --
		//
		// "classifiers" array size
		0x4e, 0x0, 0x0, 0x0,
		// array length
		0x2, 0x0, 0x0, 0x0,
		//
		// "License"
		0x7, 0x0, 0x0, 0x0,
		0x4c, 0x69, 0x63, 0x65, 0x6e, 0x73, 0x65,
		//
		// 2
		0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		//
		// "values" array size
		0x16, 0x0, 0x0, 0x0,
		//
		// "values" array length
		0x2, 0x0, 0x0, 0x0,
		//
		// "one"
		0x3, 0x0, 0x0, 0x0,
		0x6f, 0x6e, 0x65,
		//
		// "two"
		0x3, 0x0, 0x0, 0x0,
		0x74, 0x77, 0x6f,
		//
		// "Usage"
		0x5, 0x0, 0x0, 0x0,
		0x55, 0x73, 0x61, 0x67, 0x65,
		//
		// 1
		0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// "values" array size"
		0x8, 0x0, 0x0, 0x0,
		//
		// zero length array
		0x0, 0x0, 0x0, 0x0,
		//
		// -- end array --
		//
		// "be-an-author"
		0xc, 0x0, 0x0, 0x0,
		0x62, 0x65, 0x2d, 0x61, 0x6e, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72,
		//
		// -- array start --
		//
		// "snapshots" array size
		0x75, 0x0, 0x0, 0x0,
		// "snapshots" array length
		0x2, 0x0, 0x0, 0x0,
		//
		// -- Array index --
		//
		// "2020-10-11"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x31, 0x31,
		0x40, 0x0, 0x0, 0x0,
		// "2020-10-09"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x30, 0x39,
		0x11, 0x0, 0x0, 0x0,
		//
		// -- end array index --
		// -- array data --
		//
		//
		// "The description of django"
		0x19, 0x0, 0x0, 0x0,
		0x54, 0x68, 0x65, 0x20, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69,
		0x70, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x6f, 0x66, 0x20, 0x64,
		0x6a, 0x61, 0x6e, 0x67, 0x6f,
		//
		// false
		0x0,
		//
		// "3.0.3
		0x5, 0x0, 0x0, 0x0,
		0x33, 0x2e, 0x30, 0x2e, 0x33,
		//
		// "django summary"
		0xe, 0x0, 0x0, 0x0,
		0x64, 0x6a, 0x61, 0x6e, 0x67, 0x6f, 0x20, 0x73, 0x75, 0x6d,
		0x6d, 0x61, 0x72, 0x79,
		//
		// "MIT"
		0x3, 0x0, 0x0, 0x0,
		0x4d, 0x49, 0x54,
		//
		// no description
		0x0, 0x0, 0x0, 0x0,
		//
		// true
		0x1,
		//
		// no version
		0x0, 0x0, 0x0, 0x0,
		//
		// no summary
		0x0, 0x0, 0x0, 0x0,
		// no license
		0x0, 0x0, 0x0, 0x0,
		//
		// -- end array --
		//
		// 55
		0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
	}, buf.Bytes())

	pbuf := &bytes.Buffer{}
	err := Print(pbuf, bufio.NewReader(buf))
	s.Require().Nil(err)
	s.Require().Equal(`
-----------------------------------------
                Object[1]                
-----------------------------------------
homepage (string): http://homepage.com
cname (string): numpy
pname (string): Numpy
classifiers (array(2)):
    -
    name (string): License
    type (int): 2
    values (array(3)):
        -one
        -two
        -three
    -
    name (string): Usage
    type (int): 1
    values (array(0)):
author (string): an-author
snapshots (indexed array(3)):
    - 2020-10-11
    description (string): The description of numpy
    deleted (bool): false
    version (string): 3.0.3
    summary (string): numpy summary
    license (string): MIT
    - 2020-10-10
    description (string): Older description of numpy
    deleted (bool): false
    version (string): 3.0.2
    summary (string): numpy summary
    license (string): MIT
    - 2020-10-09
    description (string): 
    deleted (bool): true
    version (string): 
    summary (string): 
    license (string): 
popularity (int): 55

-----------------------------------------
                Object[2]                
-----------------------------------------
homepage (string): http://django-home.com
cname (string): django
pname (string): Django
classifiers (array(2)):
    -
    name (string): License
    type (int): 2
    values (array(2)):
        -one
        -two
    -
    name (string): Usage
    type (int): 1
    values (array(0)):
author (string): be-an-author
snapshots (indexed array(2)):
    - 2020-10-11
    description (string): The description of django
    deleted (bool): false
    version (string): 3.0.3
    summary (string): django summary
    license (string): MIT
    - 2020-10-09
    description (string): 
    deleted (bool): true
    version (string): 
    summary (string): 
    license (string): 
popularity (int): 55
`, "\n"+pbuf.String())
}

// TestWriteV1ComplexObject tests writing `testComplexData` using the legacy Version1
// index format. It also validates that we can still read the Version1 index
// format with no errors.
func (s *WriterComplexSuite) TestWriteV1ComplexObject() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	var totalSz int
	for _, obj := range testComplexData {
		sz, err := w.WriteObject(obj)
		s.Assert().Nil(err)
		totalSz += sz
	}
	// Object should use 862 bytes.
	s.Assert().Equal(862, totalSz)
	s.Assert().Len(buf.Bytes(), 862)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object index header
		//
		// Index size
		0xf3, 0x0, 0x0, 0x0,
		//
		// Fields Index
		//
		// "homepage"
		0x8, 0x0, 0x0, 0x0,
		0x68, 0x6f, 0x6d, 0x65, 0x70, 0x61, 0x67, 0x65,
		// type
		0x1, 0x0, 0x0, 0x0,
		//
		// "cname"
		0x5, 0x0, 0x0, 0x0,
		0x63, 0x6e, 0x61, 0x6d, 0x65,
		0x1, 0x0, 0x0, 0x0,
		//
		// "pname"
		0x5, 0x0, 0x0, 0x0,
		0x70, 0x6e, 0x61, 0x6d, 0x65,
		0x1, 0x0, 0x0, 0x0,
		//
		// "classifiers" (array)
		0xb, 0x0, 0x0, 0x0,
		0x63, 0x6c, 0x61, 0x73, 0x73, 0x69, 0x66, 0x69, 0x65, 0x72, 0x73,
		// array type
		0x4, 0x0, 0x0, 0x0,
		// 3 subfields
		0x3, 0x0, 0x0, 0x0,
		//
		// "classifiers" - "name"
		0x4, 0x0, 0x0, 0x0,
		0x6e, 0x61, 0x6d, 0x65,
		0x1, 0x0, 0x0, 0x0,
		//
		// "classifiers" - "type" (int)
		0x4, 0x0, 0x0, 0x0,
		0x74, 0x79, 0x70, 0x65,
		0x7, 0x0, 0x0, 0x0,
		//
		// "classifiers" - "values" (array)
		0x6, 0x0, 0x0, 0x0,
		0x76, 0x61, 0x6c, 0x75, 0x65, 0x73,
		0x4, 0x0, 0x0, 0x0,
		// string array has zero subfields
		0x0, 0x0, 0x0, 0x0,
		//
		// "author"
		0x6, 0x0, 0x0, 0x0,
		0x61, 0x75, 0x74, 0x68, 0x6f, 0x72,
		0x1, 0x0, 0x0, 0x0,
		//
		// "snapshots" (array)
		0x9, 0x0, 0x0, 0x0,
		0x73, 0x6e, 0x61, 0x70, 0x73, 0x68, 0x6f, 0x74, 0x73,
		0x4, 0x0, 0x0, 0x0,
		// 5 subfields
		0x5, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "description"
		0xb, 0x0, 0x0, 0x0,
		0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e,
		0x1, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "deleted" (bool)
		0x7, 0x0, 0x0, 0x0,
		0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64,
		0x3, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "version"
		0x7, 0x0, 0x0, 0x0,
		0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
		0x1, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "summary"
		0x7, 0x0, 0x0, 0x0,
		0x73, 0x75, 0x6d, 0x6d, 0x61, 0x72, 0x79,
		0x1, 0x0, 0x0, 0x0,
		//
		// "snapshots" - "license"
		0x7, 0x0, 0x0, 0x0,
		0x6c, 0x69, 0x63, 0x65, 0x6e, 0x73, 0x65,
		0x1, 0x0, 0x0, 0x0,
		//
		// "popularity" (int)
		0xa, 0x0, 0x0, 0x0,
		0x70, 0x6f, 0x70, 0x75, 0x6c, 0x61, 0x72, 0x69, 0x74, 0x79,
		0x7, 0x0, 0x0, 0x0,
		//
		// -- end index --
		// -- start object --
		//
		// Object size
		0x5c, 0x1, 0x0, 0x0,
		//
		// "http://homepage.com"
		0x13, 0x0, 0x0, 0x0,
		0x68, 0x74, 0x74, 0x70, 0x3a, 0x2f, 0x2f, 0x68, 0x6f, 0x6d,
		0x65, 0x70, 0x61, 0x67, 0x65, 0x2e, 0x63, 0x6f, 0x6d,
		//
		// "numpy"
		0x5, 0x0, 0x0, 0x0,
		0x6e, 0x75, 0x6d, 0x70, 0x79,
		//
		// "Numpy"
		0x5, 0x0, 0x0, 0x0,
		0x4e, 0x75, 0x6d, 0x70, 0x79,
		//
		// -- start array --
		//
		// "classifiers" array size
		0x57, 0x0, 0x0, 0x0,
		// array length
		0x2, 0x0, 0x0, 0x0,
		// "License"
		0x7, 0x0, 0x0, 0x0,
		0x4c, 0x69, 0x63, 0x65, 0x6e, 0x73, 0x65,
		// 2
		0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// "values" array size
		0x1f, 0x0, 0x0, 0x0,
		// array length
		0x3, 0x0, 0x0, 0x0,
		// "one"
		0x3, 0x0, 0x0, 0x0,
		0x6f, 0x6e, 0x65,
		// "two"
		0x3, 0x0, 0x0, 0x0,
		0x74, 0x77, 0x6f,
		// "three"
		0x5, 0x0, 0x0, 0x0,
		0x74, 0x68, 0x72, 0x65, 0x65,
		//
		// "Usage"
		0x5, 0x0, 0x0, 0x0,
		0x55, 0x73, 0x61, 0x67, 0x65,
		// 1
		0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// "values" array size
		0x8, 0x0, 0x0, 0x0,
		// zero len array
		0x0, 0x0, 0x0, 0x0,
		//
		// -- end array --
		//
		// "an-author"
		0x9, 0x0, 0x0, 0x0,
		0x61, 0x6e, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72,
		//
		// -- start "snapshots" array
		//
		// Array size
		0xc1, 0x0, 0x0, 0x0,
		// Array length
		0x3, 0x0, 0x0, 0x0,
		//
		// -- Array index --
		//
		// "2020-10-11"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x31, 0x31,
		0x3e, 0x0, 0x0, 0x0,
		// "2020-10-10"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x31, 0x30,
		0x40, 0x0, 0x0, 0x0,
		// "2020-10-09"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x30, 0x39,
		0x11, 0x0, 0x0, 0x0,
		//
		// -- end array index --
		// -- array data --
		//
		// "The description of numpy"
		0x18, 0x0, 0x0, 0x0,
		0x54, 0x68, 0x65, 0x20, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69,
		0x70, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x6f, 0x66, 0x20, 0x6e,
		0x75, 0x6d, 0x70, 0x79,
		//
		// false
		0x0,
		//
		// "3.0.3"
		0x5, 0x0, 0x0, 0x0,
		0x33, 0x2e, 0x30, 0x2e, 0x33,
		//
		// "numpy summary"
		0xd, 0x0, 0x0, 0x0,
		0x6e, 0x75, 0x6d, 0x70, 0x79, 0x20, 0x73, 0x75, 0x6d, 0x6d, 0x61, 0x72, 0x79,
		//
		// "MIT"
		0x3, 0x0, 0x0, 0x0,
		0x4d, 0x49, 0x54,
		//
		// ------
		//
		// "Older description of numpy"
		0x1a, 0x0, 0x0, 0x0,
		0x4f, 0x6c, 0x64, 0x65, 0x72, 0x20, 0x64, 0x65, 0x73, 0x63,
		0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x6f, 0x66,
		0x20, 0x6e, 0x75, 0x6d, 0x70, 0x79,
		// false
		0x0,
		// "3.0.2"
		0x5, 0x0, 0x0, 0x0,
		0x33, 0x2e, 0x30, 0x2e, 0x32,
		// "numpy summary"
		0xd, 0x0, 0x0, 0x0,
		0x6e, 0x75, 0x6d, 0x70, 0x79, 0x20, 0x73, 0x75, 0x6d, 0x6d, 0x61, 0x72, 0x79,
		// "MIT"
		0x3, 0x0, 0x0, 0x0,
		0x4d, 0x49, 0x54,
		//
		// ------
		//
		// no description
		0x0, 0x0, 0x0, 0x0,
		// true
		0x1,
		// no version
		0x0, 0x0, 0x0, 0x0,
		// no summary
		0x0, 0x0, 0x0, 0x0,
		// no license
		0x0, 0x0, 0x0, 0x0,
		//
		// -- end array --
		//
		// 55 (popularity)
		0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		//
		// -- end object --
		//
		// Next object size
		0xf, 0x1, 0x0, 0x0,
		//
		// "http://django-home.com"
		0x16, 0x0, 0x0, 0x0,
		0x68, 0x74, 0x74, 0x70, 0x3a, 0x2f, 0x2f, 0x64, 0x6a, 0x61,
		0x6e, 0x67, 0x6f, 0x2d, 0x68, 0x6f, 0x6d, 0x65, 0x2e, 0x63,
		0x6f, 0x6d,
		//
		// "django",
		0x6, 0x0, 0x0, 0x0,
		0x64, 0x6a, 0x61, 0x6e, 0x67, 0x6f,
		//
		// "Django"
		0x6, 0x0, 0x0, 0x0,
		0x44, 0x6a, 0x61, 0x6e, 0x67, 0x6f,
		//
		// -- start array --
		//
		// "classifiers" array size
		0x4e, 0x0, 0x0, 0x0,
		// array length
		0x2, 0x0, 0x0, 0x0,
		//
		// "License"
		0x7, 0x0, 0x0, 0x0,
		0x4c, 0x69, 0x63, 0x65, 0x6e, 0x73, 0x65,
		//
		// 2
		0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		//
		// "values" array size
		0x16, 0x0, 0x0, 0x0,
		//
		// "values" array length
		0x2, 0x0, 0x0, 0x0,
		//
		// "one"
		0x3, 0x0, 0x0, 0x0,
		0x6f, 0x6e, 0x65,
		//
		// "two"
		0x3, 0x0, 0x0, 0x0,
		0x74, 0x77, 0x6f,
		//
		// "Usage"
		0x5, 0x0, 0x0, 0x0,
		0x55, 0x73, 0x61, 0x67, 0x65,
		//
		// 1
		0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// "values" array size"
		0x8, 0x0, 0x0, 0x0,
		//
		// zero length array
		0x0, 0x0, 0x0, 0x0,
		//
		// -- end array --
		//
		// "be-an-author"
		0xc, 0x0, 0x0, 0x0,
		0x62, 0x65, 0x2d, 0x61, 0x6e, 0x2d, 0x61, 0x75, 0x74, 0x68, 0x6f, 0x72,
		//
		// -- array start --
		//
		// "snapshots" array size
		0x75, 0x0, 0x0, 0x0,
		// "snapshots" array length
		0x2, 0x0, 0x0, 0x0,
		//
		// -- Array index --
		//
		// "2020-10-11"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x31, 0x31,
		0x40, 0x0, 0x0, 0x0,
		// "2020-10-09"
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x30, 0x39,
		0x11, 0x0, 0x0, 0x0,
		//
		// -- end array index --
		// -- array data --
		//
		//
		// "The description of django"
		0x19, 0x0, 0x0, 0x0,
		0x54, 0x68, 0x65, 0x20, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69,
		0x70, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x6f, 0x66, 0x20, 0x64,
		0x6a, 0x61, 0x6e, 0x67, 0x6f,
		//
		// false
		0x0,
		//
		// "3.0.3
		0x5, 0x0, 0x0, 0x0,
		0x33, 0x2e, 0x30, 0x2e, 0x33,
		//
		// "django summary"
		0xe, 0x0, 0x0, 0x0,
		0x64, 0x6a, 0x61, 0x6e, 0x67, 0x6f, 0x20, 0x73, 0x75, 0x6d,
		0x6d, 0x61, 0x72, 0x79,
		//
		// "MIT"
		0x3, 0x0, 0x0, 0x0,
		0x4d, 0x49, 0x54,
		//
		// no description
		0x0, 0x0, 0x0, 0x0,
		//
		// true
		0x1,
		//
		// no version
		0x0, 0x0, 0x0, 0x0,
		//
		// no summary
		0x0, 0x0, 0x0, 0x0,
		// no license
		0x0, 0x0, 0x0, 0x0,
		//
		// -- end array --
		//
		// 55
		0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
	}, buf.Bytes())

	// Now, ensure we can still read the v1 index.
	r := NewReader()
	idx, err := r.ReadIndex(buf)
	s.Require().Nil(err)
	s.Require().Equal(Index{
		IndexEntry{
			FieldName: "homepage",
			FieldType: 1,
		},
		IndexEntry{
			FieldName: "cname",
			FieldType: 1,
		},
		IndexEntry{
			FieldName: "pname",
			FieldType: 1,
		},
		IndexEntry{
			FieldName: "classifiers",
			FieldType: 4,
			Subfields: Index{
				IndexEntry{
					FieldName: "name",
					FieldType: 1,
				},
				IndexEntry{
					FieldName: "type",
					FieldType: 7,
				},
				IndexEntry{
					FieldName: "values",
					FieldType: 4,
				},
			},
		},
		IndexEntry{
			FieldName: "author",
			FieldType: 1,
		},
		IndexEntry{
			FieldName: "snapshots",
			FieldType: 4,
			Subfields: Index{
				IndexEntry{
					FieldName: "description",
					FieldType: 1,
				},
				IndexEntry{
					FieldName: "deleted",
					FieldType: 3,
				},
				IndexEntry{
					FieldName: "version",
					FieldType: 1,
				},
				IndexEntry{
					FieldName: "summary",
					FieldType: 1,
				},
				IndexEntry{
					FieldName: "license",
					FieldType: 1,
				},
			},
		},
		IndexEntry{
			FieldName: "popularity",
			FieldType: 7,
		},
	}, idx)

	// Read the object size
	objectSz, err := r.ReadSizeField(buf)
	s.Require().Nil(err)
	s.Require().Equal(348, objectSz)

	// Read the next value
	homePage, err := r.ReadStringField(buf)
	s.Require().Nil(err)
	s.Require().Equal("http://homepage.com", homePage)
}
