// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"
)

type WriterSuite struct {
	suite.Suite
}

func TestWriterSuite(t *testing.T) {
	suite.Run(t, &WriterSuite{})
}

func (s *WriterSuite) TestNewWriter() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)
	s.Assert().Equal(&rsfWriter{writer: buf}, w)
}

func (s *WriterSuite) TestDiscreteWrites() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	sz, err := w.WriteSizeField(0, 4567, buf)
	s.Assert().Nil(err)
	s.Assert().Equal(4, sz)
	// Hex 11d7 equals 4567.
	s.Assert().Equal([]byte{0xd7, 0x11, 0x0, 0x0}, buf.Bytes())

	buf.Reset()
	sz, err = w.WriteFixedStringField(0, 15, "package-manager-too-long", buf)
	s.Assert().ErrorContains(err, "size 24 does not match expected size 15")
	sz, err = w.WriteFixedStringField(0, 15, "package-manager", buf)
	s.Assert().Nil(err)
	s.Assert().Equal(15, sz)
	s.Assert().Equal([]byte{0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x2d, 0x6d, 0x61, 0x6e, 0x61, 0x67, 0x65, 0x72}, buf.Bytes())

	buf.Reset()
	sz, err = w.WriteStringField(0, "package-manager", buf)
	s.Assert().Nil(err)
	s.Assert().Equal(19, sz)
	// The leading 4 bytes (f000) indicate the size of 15 for the string.
	// The following 15 bytes are the same as the fixed string bytes for "package-manager", above.
	s.Assert().Equal([]byte{0xf, 0x0, 0x0, 0x0, 0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x2d, 0x6d, 0x61, 0x6e, 0x61, 0x67, 0x65, 0x72}, buf.Bytes())

	buf.Reset()
	// Write `true`.
	sz, err = w.WriteBoolField(0, true, buf)
	s.Assert().Nil(err)
	s.Assert().Equal(1, sz)
	s.Assert().Equal([]byte{0x1}, buf.Bytes())
	// Append `false`.
	sz, err = w.WriteBoolField(sz, false, buf)
	s.Assert().Nil(err)
	s.Assert().Equal(2, sz)
	s.Assert().Equal([]byte{0x1, 0x0}, buf.Bytes())
}

func (s *WriterSuite) TestWriteString() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	// Write a variable-length string
	t := &tag{}
	sz, err := w.(*rsfWriter).writeString("test", t, buf)
	s.Assert().Nil(err)
	s.Assert().Equal(8, sz)

	// Switch to fixed-length string
	t.fixed = 8
	_, err = w.(*rsfWriter).writeString("test", t, buf)
	s.Assert().ErrorContains(err, "size 4 does not match expected size 8")
	sz, err = w.(*rsfWriter).writeString("test-now", t, buf)
	s.Assert().Nil(err)
	s.Assert().Equal(8, sz)
	s.Assert().Equal([]byte{
		// 4 bytes in length (header for variable-length string)
		0x4, 0x0, 0x0, 0x0,
		// "test"
		0x74, 0x65, 0x73, 0x74,
		// "test-again" (8 byte fixed-length string)
		0x74, 0x65, 0x73, 0x74, 0x2d, 0x6e, 0x6f, 0x77,
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteArray() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	a := []struct {
		Date     string `rsf:"date,fixed:10"`
		Name     string `rsf:"name"`
		Verified bool   `rsf:"verified"`
	}{
		{
			Date:     "2020-10-01-mistake",
			Name:     "From 2020",
			Verified: false,
		},
		{
			Date:     "2021-03-21",
			Name:     "From 2021",
			Verified: true,
		},
		{
			Date:     "2022-12-15",
			Name:     "this is from 2022",
			Verified: true,
		},
	}

	t := &tag{
		index:   "date",
		indexSz: 10,
	}

	// Error
	_, err := w.(*rsfWriter).writeArray(reflect.ValueOf(a), t, buf)
	s.Assert().ErrorContains(err, "size 18 does not match expected size 10")

	// Fix error
	a[0].Date = "2020-10-01"
	buf.Reset()
	sz, err := w.(*rsfWriter).writeArray(reflect.ValueOf(a), t, buf)
	s.Assert().Nil(err)
	s.Assert().Equal(130, sz)
	s.Assert().Equal(130, buf.Len())
	s.Assert().Equal([]byte{
		//
		// Array Header
		//
		// Array is 130 bytes in size
		0x82, 0x0, 0x0, 0x0,
		//
		// Array has 3 elements
		0x3, 0x0, 0x0, 0x0,
		//
		// Array Index
		//
		// 2020-10-01 index entry
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x30, 0x31,
		// Record is 24 bytes in size
		0x18, 0x0, 0x0, 0x0,
		// 2021-03-21 index entry
		0x32, 0x30, 0x32, 0x31, 0x2d, 0x30, 0x33, 0x2d, 0x32, 0x31,
		// Record is 24 bytes in size
		0x18, 0x0, 0x0, 0x0,
		// 2022-12-15 index entry
		0x32, 0x30, 0x32, 0x32, 0x2d, 0x31, 0x32, 0x2d, 0x31, 0x35,
		// Record is 32 bytes in size
		0x20, 0x0, 0x0, 0x0,
		//
		// Array data
		//
		// 2020-10-01 (fixed-length string)
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x30, 0x31,
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2020"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x30,
		// false
		0x0,
		// 2021-03-21 (fixed-length string)
		0x32, 0x30, 0x32, 0x31, 0x2d, 0x30, 0x33, 0x2d, 0x32, 0x31,
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2021"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x31,
		// true
		0x1,
		// 2022-12-15 (fixed-length string)
		0x32, 0x30, 0x32, 0x32, 0x2d, 0x31, 0x32, 0x2d, 0x31, 0x35,
		// 17 byte variable-length string
		0x11, 0x0, 0x0, 0x0,
		// "this is from 2022"
		0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x32,
		// true
		0x1,
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectWithArrayIndex() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	type snap struct {
		// Skip this field since we can determine it from the array index:
		//
		// Since the `List []snap` struct tag includes `index:date`, the
		// array will be indexed using the value of this field. Since the
		// date is written in the index, there's no need to write it again
		// when serializing each array element, so we include `skip` here.
		Date     string `rsf:"date,skip,fixed:10"`
		Name     string `rsf:"name"`
		Verified bool   `rsf:"verified"`
		Skip     string `rsf:"-"`
	}

	a := struct {
		Skip    string `rsf:"-"`
		Company string `rsf:"company"`
		Ready   bool   `rsf:"ready"`
		List    []snap `rsf:"list,index:date"`
	}{
		Company: "posit",
		Ready:   true,
		List: []snap{
			{
				Date:     "2020-10-01",
				Name:     "From 2020",
				Verified: false,
			},
			{
				Date:     "2021-03-21",
				Name:     "From 2021",
				Verified: true,
			},
			{
				Date:     "2022-12-15",
				Name:     "this is from 2022",
				Verified: true,
			},
		},
	}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 186 bytes.
	s.Assert().Equal(186, sz)
	s.Assert().Len(buf.Bytes(), 186)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object index header
		//
		// Full size of index header is 72 bytes
		0x48, 0x0, 0x0, 0x0,
		//
		// Fields Index
		//
		// "company" field is 7 bytes in length
		0x7, 0x0, 0x0, 0x0,
		// "company" field name
		0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79,
		// "company" field type 1 indicates variable-length
		0x1, 0x0, 0x0, 0x0,
		//
		// "ready" field is 5 bytes in length
		0x5, 0x0, 0x0, 0x0,
		// "ready" field name
		0x72, 0x65, 0x61, 0x64, 0x79,
		// "ready" field type 2 indicates fixed-length
		0x2, 0x0, 0x0, 0x0,
		//
		// "list" field is 4 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "list field name
		0x6c, 0x69, 0x73, 0x74,
		// "list" field type 3 indicates array
		0x3, 0x0, 0x0, 0x0,
		//
		// "name" field is 4 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "name" field name
		0x6e, 0x61, 0x6d, 0x65,
		// "name" field type 1 indicates variable length
		0x1, 0x0, 0x0, 0x0,
		//
		// "verified" field is 8 bytes in length
		0x8, 0x0, 0x0, 0x0,
		// "verified" field name
		0x76, 0x65, 0x72, 0x69, 0x66, 0x69, 0x65, 0x64,
		// "verified" field type 2 indicates fixed-length
		0x2, 0x0, 0x0, 0x0,
		//
		// -- End of 72-byte object index header ---
		//
		// Object header
		//
		// Object size is 114 bytes
		0x72, 0x0, 0x0, 0x0,
		//
		// 5 byte variable-length string
		0x5, 0x0, 0x0, 0x0,
		// "posit"
		0x70, 0x6f, 0x73, 0x69, 0x74,
		// ready:true
		0x1,
		//
		// Array Header
		//
		// Array is 100 bytes in size
		0x64, 0x0, 0x0, 0x0,
		//
		// Array has 3 elements
		0x3, 0x0, 0x0, 0x0,
		//
		// Array Index
		//
		// 2020-10-01 index entry
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x30, 0x31,
		// Record is 14 bytes in size
		0xe, 0x0, 0x0, 0x0,
		// 2021-03-21 index entry
		0x32, 0x30, 0x32, 0x31, 0x2d, 0x30, 0x33, 0x2d, 0x32, 0x31,
		// Record is 14 bytes in size
		0xe, 0x0, 0x0, 0x0,
		// 2022-12-15 index entry
		0x32, 0x30, 0x32, 0x32, 0x2d, 0x31, 0x32, 0x2d, 0x31, 0x35,
		// Record is 22 bytes in size
		0x16, 0x0, 0x0, 0x0,
		//
		// Array data
		//
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2020"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x30,
		// verified:false
		0x0,
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2021"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x31,
		// verified:true
		0x1,
		// 17 byte variable-length string
		0x11, 0x0, 0x0, 0x0,
		// "this is from 2022"
		0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x32,
		// verified:true
		0x1,
		//
		// -- End of 114-byte object --
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectNoArrayIndex() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	type snap struct {
		Skip string `rsf:"-"`
		// In the `TestWriteObjectWithArrayIndex` test, we included `skip` in the
		// `Date` struct tag. Here, since we don't use an array index, we don't skip
		// this field since it must be serialized with each array element.
		Date     string `rsf:"date,fixed:10"`
		Name     string `rsf:"name"`
		Verified bool   `rsf:"verified"`
	}

	a := struct {
		Skip    string `rsf:"-"`
		Company string `rsf:"company"`
		Ready   bool   `rsf:"ready"`
		List    []snap `rsf:"list"`
	}{
		Company: "posit",
		Ready:   true,
		List: []snap{
			{
				Date:     "2020-10-01",
				Name:     "From 2020",
				Verified: false,
			},
			{
				Date:     "2021-03-21",
				Name:     "From 2021",
				Verified: true,
			},
			{
				Date:     "2022-12-15",
				Name:     "this is from 2022",
				Verified: true,
			},
		},
	}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 186 bytes since there is no array index
	s.Assert().Equal(186, sz)
	s.Assert().Len(buf.Bytes(), 186)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object index header
		//
		// Full size of index header is 84 bytes
		0x54, 0x0, 0x0, 0x0,
		//
		// Fields Index
		//
		// "company" field is 7 bytes in length
		0x7, 0x0, 0x0, 0x0,
		// "company" field name
		0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79,
		// "company" field type 1 indicates variable-length
		0x1, 0x0, 0x0, 0x0,
		//
		// "ready" field is 5 bytes in length
		0x5, 0x0, 0x0, 0x0,
		// "ready" field name
		0x72, 0x65, 0x61, 0x64, 0x79,
		// "ready" field type 2 indicates fixed-length
		0x2, 0x0, 0x0, 0x0,
		//
		// "list" field is 7 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "list field name
		0x6c, 0x69, 0x73, 0x74,
		// "list" field type 3 indicates array
		0x3, 0x0, 0x0, 0x0,
		//
		// "date" field is 4 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "date" field name
		0x64, 0x61, 0x74, 0x65,
		// "date" field type 2 indicates variable length
		0x2, 0x0, 0x0, 0x0,
		//
		// "name" field is 4 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "name" field name
		0x6e, 0x61, 0x6d, 0x65,
		// "name" field type 1 indicates variable length
		0x1, 0x0, 0x0, 0x0,
		//
		// "verified" field is 8 bytes in length
		0x8, 0x0, 0x0, 0x0,
		// "verified" field name
		0x76, 0x65, 0x72, 0x69, 0x66, 0x69, 0x65, 0x64,
		// "verified" field type 2 indicates fixed-length
		0x2, 0x0, 0x0, 0x0,
		//
		// -- End of 84-byte object index header ---
		//
		// Object header
		//
		// Object size is 102 bytes
		0x66, 0x0, 0x0, 0x0,
		// 5 byte variable-length string
		0x5, 0x0, 0x0, 0x0,
		// "posit"
		0x70, 0x6f, 0x73, 0x69, 0x74,
		// ready:true
		0x1,
		//
		// Array Header
		//
		// Array is 88 bytes in size
		0x58, 0x0, 0x0, 0x0,
		//
		// Array has 3 elements
		0x3, 0x0, 0x0, 0x0,
		//
		// Array data
		//
		// 2020-10-01 (fixed-length string)
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x30, 0x31,
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2020"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x30,
		// verified:false
		0x0,
		// 2021-03-21 (fixed-length string)
		0x32, 0x30, 0x32, 0x31, 0x2d, 0x30, 0x33, 0x2d, 0x32, 0x31,
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2021"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x31,
		// verified:true
		0x1,
		// 2022-12-15 (fixed-length string)
		0x32, 0x30, 0x32, 0x32, 0x2d, 0x31, 0x32, 0x2d, 0x31, 0x35,
		// 17 byte variable-length string
		0x11, 0x0, 0x0, 0x0,
		// "this is from 2022"
		0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x32,
		// verified:true
		0x1,
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectArray() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	a := []string{
		"one",
		"two",
		"three",
	}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 35 bytes
	s.Assert().Equal(35, sz)
	s.Assert().Len(buf.Bytes(), 35)
	// Verify bytes.
	s.Assert().Equal([]byte{
		// Full object size of 35
		0x23, 0x0, 0x0, 0x0,
		//
		// Full array size of 31
		0x1f, 0x0, 0x0, 0x0,
		//
		// Array length of three
		0x3, 0x0, 0x0, 0x0,
		//
		// "one"
		0x3, 0x0, 0x0, 0x0,
		0x6f, 0x6e, 0x65,
		//
		// "two"
		0x3, 0x0, 0x0, 0x0,
		0x74, 0x77, 0x6f,
		//
		// "three"
		0x5, 0x0, 0x0, 0x0,
		0x74, 0x68, 0x72, 0x65, 0x65,
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectArrayOfStructs() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	type my struct {
		Name      string
		Certified bool
	}
	a := []my{
		{Name: "one", Certified: true},
		{Name: "two"},
		{Name: "three", Certified: true},
	}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 35 bytes
	s.Assert().Equal(38, sz)
	s.Assert().Len(buf.Bytes(), 38)
	// Verify bytes.
	s.Assert().Equal([]byte{
		// Full object size of 38
		0x26, 0x0, 0x0, 0x0,
		//
		// Full array size of 34
		0x22, 0x0, 0x0, 0x0,
		//
		// Array length of three
		0x3, 0x0, 0x0, 0x0,
		//
		// "one"
		0x3, 0x0, 0x0, 0x0,
		0x6f, 0x6e, 0x65,
		//
		// true
		0x1,
		//
		// "two"
		0x3, 0x0, 0x0, 0x0,
		0x74, 0x77, 0x6f,
		//
		// false
		0x0,
		//
		// "three"
		0x5, 0x0, 0x0, 0x0,
		0x74, 0x68, 0x72, 0x65, 0x65,
		//
		// true
		0x1,
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectString() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	a := "this is a test"
	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 22 bytes
	s.Assert().Equal(22, sz)
	s.Assert().Len(buf.Bytes(), 22)
	// Verify bytes.
	s.Assert().Equal([]byte{
		// Full object size of 22
		0x16, 0x0, 0x0, 0x0,
		//
		// String size of 14
		0xe, 0x0, 0x0, 0x0,
		//
		// "this is a test"
		0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x61, 0x20, 0x74, 0x65, 0x73, 0x74,
	}, buf.Bytes())
}
