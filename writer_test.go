// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
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
	w := NewWriterWithVersion(buf, Version2)
	s.Assert().Equal(&rsfWriter{writer: buf, version: Version2}, w)
}

func (s *WriterSuite) TestDiscreteWrites() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

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

	// Test max int64
	buf.Reset()
	sz, err = w.WriteInt64Field(0, math.MaxInt64, buf)
	s.Assert().Nil(err)
	s.Assert().Equal(10, sz)
	s.Assert().Equal([]byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1}, buf.Bytes())
	intVal, _ := binary.Varint(buf.Bytes())
	s.Assert().Equal(int64(math.MaxInt64), intVal)

	// Test float
	buf.Reset()
	sz, err = w.WriteFloatField(0, 697828.28977, buf)
	s.Assert().Nil(err)
	s.Assert().Equal(8, sz)
	s.Assert().Equal([]byte{0xc3, 0xbb, 0x5c, 0x94, 0xc8, 0x4b, 0x25, 0x41}, buf.Bytes())
	s.Assert().Equal(697828.28977, math.Float64frombits(binary.LittleEndian.Uint64(buf.Bytes())))

	// Test max float
	buf.Reset()
	sz, err = w.WriteFloatField(0, math.MaxFloat64, buf)
	s.Assert().Nil(err)
	s.Assert().Equal(8, sz)
	s.Assert().Equal([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f}, buf.Bytes())
	s.Assert().Equal(math.MaxFloat64, math.Float64frombits(binary.LittleEndian.Uint64(buf.Bytes())))
}

func (s *WriterSuite) TestInternalWriteString() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

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

func (s *WriterSuite) TestInternalWriteArray() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

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
	w := NewWriterWithVersion(buf, Version2)

	type snap struct {
		// Skip this field since we can determine it from the array index:
		//
		// Since the `List []snap` struct tag includes `index:date`, the
		// array will be indexed using the value of this field. Since the
		// date is written in the index, there's no need to write it again
		// when serializing each array element, so we include `skip` here.
		Date     string   `rsf:"date,skip,fixed:10"`
		Name     string   `rsf:"name"`
		Verified bool     `rsf:"verified"`
		Skip     string   `rsf:"-"`
		Aliases  []string `rsf:"aliases"`
	}

	a := struct {
		Skip    string  `rsf:"-"`
		Company string  `rsf:"company"`
		Ready   bool    `rsf:"ready"`
		List    []snap  `rsf:"list,index:date"`
		Age     int     `rsf:"age"`
		Rating  float64 `rsf:"rating"`
	}{
		Company: "posit",
		Ready:   true,
		Age:     55,
		Rating:  92.689,
		List: []snap{
			{
				Date:     "2020-10-01",
				Name:     "From 2020",
				Aliases:  []string{"from 2020", "before 2021"},
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
				Aliases:  []string{"from 2022"},
				Verified: true,
			},
		},
	}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 338 bytes.
	s.Assert().Equal(338, sz)
	s.Assert().Len(buf.Bytes(), 338)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object index header
		//
		// Index version
		0x0, 0x8, 0x32,
		//
		// Full size of index header
		0x8a, 0x0, 0x0, 0x0,
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
		// "ready" field type 3 indicates boolean
		0x3, 0x0, 0x0, 0x0,
		//
		// "list" field is 4 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "list field name
		0x6c, 0x69, 0x73, 0x74,
		// "list" field type 4 indicates array
		0x4, 0x0, 0x0, 0x0,
		// is indexed
		0x1,
		// index type is string
		0x18, 0x0, 0x0, 0x0,
		// index size is 10
		0xa, 0x0, 0x0, 0x0,
		// Array type is struct
		0x19, 0x0, 0x0, 0x0,
		// "list" field has 3 subfields
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
		// "verified" field type 3 indicates boolean
		0x3, 0x0, 0x0, 0x0,
		//
		// "aliases" field is 7 bytes in length
		0x7, 0x0, 0x0, 0x0,
		// "aliases" field name
		0x61, 0x6c, 0x69, 0x61, 0x73, 0x65, 0x73,
		// "verified" field type 4 indicates array
		0x4, 0x0, 0x0, 0x0,
		// not indexed
		0x0,
		// Array is string type.
		0x18, 0x0, 0x0, 0x0,
		// "verified" has zero subfields
		0x0, 0x0, 0x0, 0x0,
		//
		// "age" field is 3 bytes in length
		0x3, 0x0, 0x0, 0x0,
		// "age" field name
		0x61, 0x67, 0x65,
		// "age field type 7 indicates int64
		0x7, 0x0, 0x0, 0x0,
		//
		// "rating" field is 6 bytes in length
		0x6, 0x0, 0x0, 0x0,
		// "rating" field name
		0x72, 0x61, 0x74, 0x69, 0x6e, 0x67,
		// "rating field type 6 indicates float
		0x6, 0x0, 0x0, 0x0,
		//
		// -- End of 72-byte object index header ---
		//
		// Object header
		//
		// Object size
		0xc5, 0x0, 0x0, 0x0,
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
		// Array size
		0xa5, 0x0, 0x0, 0x0,
		//
		// Array has 3 elements
		0x3, 0x0, 0x0, 0x0,
		//
		// Array Index
		//
		// 2020-10-01 index entry
		0x32, 0x30, 0x32, 0x30, 0x2d, 0x31, 0x30, 0x2d, 0x30, 0x31,
		// Record size
		0x32, 0x0, 0x0, 0x0,
		// 2021-03-21 index entry
		0x32, 0x30, 0x32, 0x31, 0x2d, 0x30, 0x33, 0x2d, 0x32, 0x31,
		// Record size
		0x16, 0x0, 0x0, 0x0,
		// 2022-12-15 index entry
		0x32, 0x30, 0x32, 0x32, 0x2d, 0x31, 0x32, 0x2d, 0x31, 0x35,
		// Record size
		0x2b, 0x0, 0x0, 0x0,
		//
		// Array data
		//
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2020"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x30,
		// verified:false
		0x0,
		//
		// "aliases" array size
		0x24, 0x0, 0x0, 0x0,
		//
		// "aliases" array length
		0x2, 0x0, 0x0, 0x0,
		//
		// "aliases" array data
		//
		// element 1 size
		0x9, 0x0, 0x0, 0x0,
		// "from 2020"
		0x66, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x30,
		//
		// element 2 size
		0xb, 0x0, 0x0, 0x0,
		// "before 2021"
		0x62, 0x65, 0x66, 0x6f, 0x72, 0x65, 0x20, 0x32, 0x30, 0x32, 0x31,
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2021"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x31,
		// verified:true
		0x1,
		//
		// "aliases" array size
		0x8, 0x0, 0x0, 0x0,
		//
		// "aliases" array length (this is a zero length array)
		0x0, 0x0, 0x0, 0x0,
		//
		// 17 byte variable-length string
		0x11, 0x0, 0x0, 0x0,
		// "this is from 2022"
		0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x32,
		// verified:true
		0x1,
		//
		// "aliases" array size
		0x15, 0x0, 0x0, 0x0,
		//
		// "aliases" array length
		0x1, 0x0, 0x0, 0x0,
		//
		// "aliases" array data
		//
		// element 1 size
		0x9, 0x0, 0x0, 0x0,
		// "from 2022"
		0x66, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x32,
		//
		// Age: 55
		0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// Rating:  92.689
		0x6a, 0xbc, 0x74, 0x93, 0x18, 0x2c, 0x57, 0x40,
		//
		// -- End of object --
	}, buf.Bytes())
}

// This is the same as `TestWriteObjectWithArrayIndex` except that
// the array index is an integer value.
func (s *WriterSuite) TestWriteObjectWithArrayIntIndex() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

	type snap struct {
		// Skip this field since we can determine it from the array index:
		//
		// Since the `List []snap` struct tag includes `index:date`, the
		// array will be indexed using the value of this field. Since the
		// date is written in the index, there's no need to write it again
		// when serializing each array element, so we include `skip` here.
		Date     int64    `rsf:"date,skip"`
		Name     string   `rsf:"name"`
		Verified bool     `rsf:"verified"`
		Skip     string   `rsf:"-"`
		Aliases  []string `rsf:"aliases"`
	}

	a := struct {
		Skip    string  `rsf:"-"`
		Company string  `rsf:"company"`
		Ready   bool    `rsf:"ready"`
		List    []snap  `rsf:"list,index:date"`
		Age     int     `rsf:"age"`
		Rating  float64 `rsf:"rating"`
	}{
		Company: "posit",
		Ready:   true,
		Age:     55,
		Rating:  92.689,
		List: []snap{
			{
				Date:     20201001,
				Name:     "From 2020",
				Aliases:  []string{"from 2020", "before 2021"},
				Verified: false,
			},
			{
				Date:     20210321,
				Name:     "From 2021",
				Verified: true,
			},
			{
				Date:     20221215,
				Name:     "this is from 2022",
				Aliases:  []string{"from 2022"},
				Verified: true,
			},
		},
	}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 338 bytes.
	s.Assert().Equal(338, sz)
	s.Assert().Len(buf.Bytes(), 338)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object index header
		//
		// Index version
		0x0, 0x8, 0x32,
		//
		// Full size of index header
		0x8a, 0x0, 0x0, 0x0,
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
		// "ready" field type 3 indicates boolean
		0x3, 0x0, 0x0, 0x0,
		//
		// "list" field is 4 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "list field name
		0x6c, 0x69, 0x73, 0x74,
		// "list" field type 4 indicates array
		0x4, 0x0, 0x0, 0x0,
		// indexed
		0x1,
		// index type int64
		0x6, 0x0, 0x0, 0x0,
		// index size 10
		0xa, 0x0, 0x0, 0x0,
		// array type struct
		0x19, 0x0, 0x0, 0x0,
		// "list" field has 3 subfields
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
		// "verified" field type 3 indicates boolean
		0x3, 0x0, 0x0, 0x0,
		//
		// "aliases" field is 7 bytes in length
		0x7, 0x0, 0x0, 0x0,
		// "aliases" field name
		0x61, 0x6c, 0x69, 0x61, 0x73, 0x65, 0x73,
		// "verified" field type 4 indicates array
		0x4, 0x0, 0x0, 0x0,
		// not indexed
		0x0,
		// string array type
		0x18, 0x0, 0x0, 0x0,
		// "verified" has zero subfields
		0x0, 0x0, 0x0, 0x0,
		//
		// "age" field is 3 bytes in length
		0x3, 0x0, 0x0, 0x0,
		// "age" field name
		0x61, 0x67, 0x65,
		// "age field type 7 indicates int64
		0x7, 0x0, 0x0, 0x0,
		//
		// "rating" field is 6 bytes in length
		0x6, 0x0, 0x0, 0x0,
		// "rating" field name
		0x72, 0x61, 0x74, 0x69, 0x6e, 0x67,
		// "rating field type 6 indicates float
		0x6, 0x0, 0x0, 0x0,
		//
		// -- End of 72-byte object index header ---
		//
		// Object header
		//
		// Object size
		0xc5, 0x0, 0x0, 0x0,
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
		// Array size
		0xa5, 0x0, 0x0, 0x0,
		//
		// Array has 3 elements
		0x3, 0x0, 0x0, 0x0,
		//
		// Array Index
		//
		// 20201001 index entry
		0xd2, 0xf8, 0xa1, 0x13, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// Record size
		0x32, 0x0, 0x0, 0x0,
		// 20210321 index entry
		0xa2, 0x8a, 0xa3, 0x13, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// Record size
		0x16, 0x0, 0x0, 0x0,
		// 20221215 index entry
		0xbe, 0xb4, 0xa4, 0x13, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// Record size
		0x2b, 0x0, 0x0, 0x0,
		//
		// Array data
		//
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2020"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x30,
		// verified:false
		0x0,
		//
		// "aliases" array size
		0x24, 0x0, 0x0, 0x0,
		//
		// "aliases" array length
		0x2, 0x0, 0x0, 0x0,
		//
		// "aliases" array data
		//
		// element 1 size
		0x9, 0x0, 0x0, 0x0,
		// "from 2020"
		0x66, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x30,
		//
		// element 2 size
		0xb, 0x0, 0x0, 0x0,
		// "before 2021"
		0x62, 0x65, 0x66, 0x6f, 0x72, 0x65, 0x20, 0x32, 0x30, 0x32, 0x31,
		// 9 byte variable-length string
		0x9, 0x0, 0x0, 0x0,
		// "From 2021"
		0x46, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x31,
		// verified:true
		0x1,
		//
		// "aliases" array size
		0x8, 0x0, 0x0, 0x0,
		//
		// "aliases" array length (this is a zero length array)
		0x0, 0x0, 0x0, 0x0,
		//
		// 17 byte variable-length string
		0x11, 0x0, 0x0, 0x0,
		// "this is from 2022"
		0x74, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x32,
		// verified:true
		0x1,
		//
		// "aliases" array size
		0x15, 0x0, 0x0, 0x0,
		//
		// "aliases" array length
		0x1, 0x0, 0x0, 0x0,
		//
		// "aliases" array data
		//
		// element 1 size
		0x9, 0x0, 0x0, 0x0,
		// "from 2022"
		0x66, 0x72, 0x6f, 0x6d, 0x20, 0x32, 0x30, 0x32, 0x32,
		//
		// Age: 55
		0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// Rating:  92.689
		0x6a, 0xbc, 0x74, 0x93, 0x18, 0x2c, 0x57, 0x40,
		//
		// -- End of object --
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectWithArrayIndexNilSubArray() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

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
		Skip    string  `rsf:"-"`
		Company string  `rsf:"company"`
		Ready   bool    `rsf:"ready"`
		List    []snap  `rsf:"list,index:date"`
		Age     int     `rsf:"age"`
		Rating  float64 `rsf:"rating"`
	}{
		Company: "posit",
		Ready:   true,
		Age:     55,
		Rating:  92.689,
	}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 157 bytes.
	s.Assert().Equal(157, sz)
	s.Assert().Len(buf.Bytes(), 157)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object index header
		//
		// Index version
		0x0, 0x8, 0x32,
		//
		// Full size of index header
		0x72, 0x0, 0x0, 0x0,
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
		// "ready" field type 2 indicates boolean
		0x3, 0x0, 0x0, 0x0,
		//
		// "list" field is 4 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "list field name
		0x6c, 0x69, 0x73, 0x74,
		// "list" field type 4 indicates array
		0x4, 0x0, 0x0, 0x0,
		// indexed
		0x1,
		// index field type is string
		0x18, 0x0, 0x0, 0x0,
		// index size 10
		0xa, 0x0, 0x0, 0x0,
		// array type is struct
		0x19, 0x0, 0x0, 0x0,
		// "list" field has 2 subfields
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
		// "verified" field type 3 indicates boolean
		0x3, 0x0, 0x0, 0x0,
		//
		// "age" field is 3 bytes in length
		0x3, 0x0, 0x0, 0x0,
		// "age" field name
		0x61, 0x67, 0x65,
		// "age field type 7 indicates int64
		0x7, 0x0, 0x0, 0x0,
		//
		// "rating" field is 6 bytes in length
		0x6, 0x0, 0x0, 0x0,
		// "rating" field name
		0x72, 0x61, 0x74, 0x69, 0x6e, 0x67,
		// "rating field type 6 indicates float
		0x6, 0x0, 0x0, 0x0,
		//
		// -- End of index header ---
		//
		// Object header
		//
		// Object size
		0x28, 0x0, 0x0, 0x0,
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
		// Array is 8 bytes in size
		0x8, 0x0, 0x0, 0x0,
		//
		// Array has zero elements
		0x0, 0x0, 0x0, 0x0,
		//
		// Age: 55
		0x6e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		// Rating:  92.689
		0x6a, 0xbc, 0x74, 0x93, 0x18, 0x2c, 0x57, 0x40,
		//
		// -- End of object --
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectNoArrayIndex() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

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
	// Object should use 202 bytes since there is no array index
	s.Assert().Equal(202, sz)
	s.Assert().Len(buf.Bytes(), 202)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object index header
		//
		// Index version
		0x0, 0x8, 0x32,
		//
		// Full size of index header
		0x61, 0x0, 0x0, 0x0,
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
		// "ready" field type 3 indicates boolean
		0x3, 0x0, 0x0, 0x0,
		//
		// "list" field is 4 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "list field name
		0x6c, 0x69, 0x73, 0x74,
		// "list" field type 4 indicates array
		0x4, 0x0, 0x0, 0x0,
		// not indexed
		0x0,
		// Field type is struct.
		0x19, 0x0, 0x0, 0x0,
		// "list" array has 3 subfields
		0x3, 0x0, 0x0, 0x0,
		//
		// "date" field is 4 bytes in length
		0x4, 0x0, 0x0, 0x0,
		// "date" field name
		0x64, 0x61, 0x74, 0x65,
		// "date" field type 2 indicates fixed-length string
		0x2, 0x0, 0x0, 0x0,
		// "date" field is of fixed size 10
		0xa, 0x0, 0x0, 0x0,
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
		// "verified" field type 3 indicates boolean
		0x3, 0x0, 0x0, 0x0,
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
	w := NewWriterWithVersion(buf, Version2)

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

func (s *WriterSuite) TestWriteObjectNilArray() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

	var a []string

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 12 bytes
	s.Assert().Equal(12, sz)
	s.Assert().Len(buf.Bytes(), 12)
	// Verify bytes.
	s.Assert().Equal([]byte{
		// Full object size of 12
		0xc, 0x0, 0x0, 0x0,
		//
		// Full array size of 8
		0x8, 0x0, 0x0, 0x0,
		//
		// Array length of 0
		0x0, 0x0, 0x0, 0x0,
		//
		// no elements
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectInt() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

	a := []int{3, 6, 9, 12, 15}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 62 bytes
	s.Assert().Equal(62, sz)
	s.Assert().Len(buf.Bytes(), 62)
	// Verify bytes.
	s.Assert().Equal([]byte{
		// Full object size of 62
		0x3e, 0x0, 0x0, 0x0,
		//
		// Full array size of 58
		0x3a, 0x0, 0x0, 0x0,
		//
		// Array length of 5
		0x5, 0x0, 0x0, 0x0,
		//
		// 3, 6, 9, 12, 15
		0x6, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0xc, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x12, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x18, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
		0x1e, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectFloat() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

	a := []float64{3.33, 6.928, 9.1, 12.0, 15.78967}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 52 bytes
	s.Assert().Equal(52, sz)
	s.Assert().Len(buf.Bytes(), 52)
	// Verify bytes.
	s.Assert().Equal([]byte{
		// Full object size of 52
		0x34, 0x0, 0x0, 0x0,
		//
		// Full array size of 48
		0x30, 0x0, 0x0, 0x0,
		//
		// Array length of 5
		0x5, 0x0, 0x0, 0x0,
		//
		// 3.33
		0xa4, 0x70, 0x3d, 0xa, 0xd7, 0xa3, 0xa, 0x40,
		// 6.928
		0x83, 0xc0, 0xca, 0xa1, 0x45, 0xb6, 0x1b, 0x40,
		// 9.1
		0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x22, 0x40,
		// 12.0
		0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x28, 0x40, 0xbf,
		// 15.78967
		0x43, 0x51, 0xa0, 0x4f, 0x94, 0x2f, 0x40,
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectArrayOfStructs() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

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
	w := NewWriterWithVersion(buf, Version2)

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

// TestWriteObjectArrayOfArrays tests writing a struct that contains an array
// or arrays. This is supported by RSF, but is not well-supported by printing.
func (s *WriterSuite) TestWriteObjectArrayOfArrays() {
	buf := &bytes.Buffer{}
	w := NewWriterWithVersion(buf, Version2)

	type TestObject struct {
		Arrays [][]string `rsf:"arrays"`
	}
	a := TestObject{
		Arrays: [][]string{
			{
				"a1", "a2", "a3",
			},
			{
				"b1", "b2",
			},
		},
	}

	sz, err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 88 bytes
	s.Assert().Equal(88, sz)
	s.Assert().Len(buf.Bytes(), 88)
	// Verify bytes.
	s.Assert().Equal([]byte{
		// Index version 2
		0x0, 0x8, 0x32,
		// Index size
		0x1b, 0x0, 0x0, 0x0,
		// "arrays" index field
		0x6, 0x0, 0x0, 0x0,
		0x61, 0x72, 0x72, 0x61, 0x79, 0x73,
		// array field type
		0x4, 0x0, 0x0, 0x0,
		// Not indexed
		0x0,
		// Array type
		0x17, 0x0, 0x0, 0x0,
		// zero subfields since not a struct
		0x0, 0x0, 0x0, 0x0,

		// Full object size
		0x3a, 0x0, 0x0, 0x0,
		//
		// Full array size of 34
		0x36, 0x0, 0x0, 0x0,
		//
		// Array length
		0x2, 0x0, 0x0, 0x0,
		//
		// Sub-array(1) size
		0x1a, 0x0, 0x0, 0x0,
		// Sub-array(1) length
		0x3, 0x0, 0x0, 0x0,
		// "a1"
		0x2, 0x0, 0x0, 0x0,
		0x61, 0x31,
		// "a2"
		0x2, 0x0, 0x0, 0x0,
		0x61, 0x32,
		// "a3"
		0x2, 0x0, 0x0, 0x0,
		0x61, 0x33,
		// Sub-array(1) size
		0x14, 0x0, 0x0, 0x0,
		// Sub-array(2) length
		0x2, 0x0, 0x0, 0x0,
		// "b1"
		0x2, 0x0, 0x0, 0x0,
		0x62, 0x31,
		// "b2"
		0x2, 0x0, 0x0, 0x0,
		0x62, 0x32,
	}, buf.Bytes())

	// Cannot yet print arrays of arrays, but falls back gracefully to indicate that
	// an array of arrays was encountered.
	pbuf := &bytes.Buffer{}
	err = Print(pbuf, bufio.NewReader(buf))
	fmt.Printf(pbuf.String())
	s.Require().Nil(err)
	s.Require().Equal(`
-----------------------------------------
                Object[1]                
-----------------------------------------
arrays (array(2)):
    - cannot print data for arrays of arrays
`, "\n"+pbuf.String())
}
