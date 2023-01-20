// Copyright (C) 2022 by Posit Software, PBC
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
	s.Assert().Equal(&writer{f: buf}, w)
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

func (s *WriterSuite) TestWriteObjectString() {
	buf := &bytes.Buffer{}
	w := NewWriter(buf)

	// Write a variable-length string
	t := &tag{}
	err := w.(*writer).writeString("test", t, buf)
	s.Assert().Nil(err)

	// Switch to fixed-length string
	t.fixed = 8
	err = w.(*writer).writeString("test", t, buf)
	s.Assert().ErrorContains(err, "size 4 does not match expected size 8")
	err = w.(*writer).writeString("test-now", t, buf)
	s.Assert().Nil(err)
	s.Assert().Equal([]byte{
		// 4 bytes in length (header for variable-length string)
		0x4, 0x0, 0x0, 0x0,
		// "test"
		0x74, 0x65, 0x73, 0x74,
		// "test-again" (8 byte fixed-length string)
		0x74, 0x65, 0x73, 0x74, 0x2d, 0x6e, 0x6f, 0x77,
	}, buf.Bytes())
}

func (s *WriterSuite) TestWriteObjectArray() {
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
	err := w.(*writer).writeArray(reflect.ValueOf(a), t, buf)
	s.Assert().ErrorContains(err, "size 18 does not match expected size 10")

	// Fix error
	a[0].Date = "2020-10-01"
	buf.Reset()
	err = w.(*writer).writeArray(reflect.ValueOf(a), t, buf)
	s.Assert().Nil(err)
	s.Assert().Equal([]byte{
		//
		// Array Header
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

	err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 110 bytes.
	s.Assert().Len(buf.Bytes(), 110)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object header
		//
		// Object size is 110 bytes
		0x6e, 0x0, 0x0, 0x0,
		// 5 byte variable-length string
		0x5, 0x0, 0x0, 0x0,
		// "posit"
		0x70, 0x6f, 0x73, 0x69, 0x74,
		// ready:true
		0x1,
		//
		// Array Header
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

	err := w.WriteObject(a)
	s.Assert().Nil(err)
	// Object should use 98 bytes since there is no array index
	s.Assert().Len(buf.Bytes(), 98)
	// Verify bytes.
	s.Assert().Equal([]byte{
		//
		// Object header
		//
		// Object size is 98 bytes
		0x62, 0x0, 0x0, 0x0,
		// 5 byte variable-length string
		0x5, 0x0, 0x0, 0x0,
		// "posit"
		0x70, 0x6f, 0x73, 0x69, 0x74,
		// ready:true
		0x1,
		//
		// Array Header
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
