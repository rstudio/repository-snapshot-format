// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReaderSuite struct {
	suite.Suite
}

func TestReaderSuite(t *testing.T) {
	suite.Run(t, &ReaderSuite{})
}

func (s *ReaderSuite) TestNewReader() {
	s.Assert().Equal(&rsfReader{}, NewReader())
}

// This method returns the same data used by `TestWriteObjectWithArrayIndex`
// in `writer_test.go`.
func getData(s suite.Suite) *bytes.Buffer {
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

	// Write the test data to the buffer
	_, err := w.WriteObject(a)
	s.Assert().Nil(err)
	return buf
}

func (s *ReaderSuite) TestRead() {
	buf := bufio.NewReader(getData(s.Suite))
	r := NewReader()

	// Read the index
	index, err := r.ReadIndex(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(117, r.Pos())

	// Check the index
	s.Assert().Equal(Index{
		IndexEntry{
			FieldName: "company",
			FieldType: FieldTypeVarStr,
		},
		IndexEntry{
			FieldName: "ready",
			FieldType: FieldTypeBool,
		},
		IndexEntry{
			FieldName:    "list",
			FieldType:    FieldTypeArray,
			Indexed:      true,
			IndexSize:    10,
			IndexType:    int(reflect.String),
			SubfieldType: int(reflect.Struct),
			Subfields: []IndexEntry{
				{
					FieldName: "name",
					FieldType: FieldTypeVarStr,
				},
				{
					FieldName: "verified",
					FieldType: FieldTypeBool,
				},
			},
		},
		IndexEntry{
			FieldName: "age",
			FieldType: FieldTypeInt64,
		},
		IndexEntry{
			FieldName: "rating",
			FieldType: FieldTypeFloat,
		},
	}, r.(*rsfReader).index)
	s.Assert().Equal(index, r.(*rsfReader).index)

	// Test updating index
	newIndex := Index{}
	r.SetIndex(newIndex)
	s.Assert().Equal(newIndex, r.(*rsfReader).index)

	// Set back to original index
	r.SetIndex(index)
	s.Assert().Equal(index, r.(*rsfReader).index)

	// Record should be 132 bytes in length
	recordSz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(132, recordSz)
	// Position increased by 4 (size field is 4 bytes)
	s.Assert().Equal(121, r.Pos())

	// Company
	err = r.AdvanceTo(buf, "company")
	s.Assert().Nil(err)
	company, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("posit", company)
	// Position increased by 9. Size field is 4 bytes + data is 5 bytes.
	s.Assert().Equal(130, r.Pos())

	// Ready
	err = r.AdvanceTo(buf, "ready")
	s.Assert().Nil(err)
	ready, err := r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().True(ready)
	// Position increased by 1
	s.Assert().Equal(131, r.Pos())

	// Array should be 100 bytes in size
	err = r.AdvanceTo(buf, "list")
	s.Assert().Nil(err)
	arraySz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(100, arraySz)
	// Position increased by 4
	s.Assert().Equal(135, r.Pos())

	// Array should be 3 elements in length
	arrayLen, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(3, arrayLen)
	// Position increased by 4
	s.Assert().Equal(139, r.Pos())

	// Array index. Read all three index entries
	// Entry 1
	date, err := r.ReadFixedStringField(10, buf)
	s.Assert().Nil(err)
	s.Assert().Equal("2020-10-01", date)
	elSz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(14, elSz)
	//
	// Entry 2
	date, err = r.ReadFixedStringField(10, buf)
	s.Assert().Nil(err)
	s.Assert().Equal("2021-03-21", date)
	elSz, err = r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(14, elSz)
	//
	// Entry 3
	date, err = r.ReadFixedStringField(10, buf)
	s.Assert().Nil(err)
	s.Assert().Equal("2022-12-15", date)
	elSz, err = r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(22, elSz)
	// Position increased by 3(10+4) since each index entry uses
	// a 10-byte fixed-length string and a 4-byte size field.
	// 3*14=42
	// 136+42=170
	s.Assert().Equal(181, r.Pos())

	// Discard 28 bytes (14+14) to move to the last array element.
	err = r.Discard(28, buf)
	s.Assert().Nil(err)
	// Position increased by 28 to 178+28=206.
	s.Assert().Equal(209, r.Pos())

	// Read last array element's "Name" field.
	err = r.AdvanceTo(buf, "list", "name")
	s.Assert().Nil(err)
	name, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("this is from 2022", name)
	// Position increased by 4+17. String size uses 4 bytes and
	// string value uses 17 bytes.
	// 206+21=227
	s.Assert().Equal(230, r.Pos())

	// Read last array element's "Verified" field.
	err = r.AdvanceTo(buf, "list", "verified")
	s.Assert().Nil(err)
	verified, err := r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().True(verified)
	// Position increased by 1.
	s.Assert().Equal(231, r.Pos())

	// Read age field
	err = r.AdvanceTo(buf, "age")
	s.Assert().Nil(err)
	age, err := r.ReadIntField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(int64(55), age)

	// Read rating field
	err = r.AdvanceTo(buf, "rating")
	s.Assert().Nil(err)
	rating, err := r.ReadFloatField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(92.689, rating)

	// Verify at EOF.
	_, err = r.ReadSizeField(buf)
	s.Assert().ErrorIs(err, io.EOF)

	// Dump buffer to temp file to test `Seek`
	tmp, err := os.CreateTemp("", "")
	s.Assert().Nil(err)
	defer os.Remove(tmp.Name())
	buf = bufio.NewReader(getData(s.Suite))
	_, err = io.Copy(tmp, buf)

	// Seek back to the last array element.
	err = r.Seek(209, tmp)
	s.Assert().Nil(err)
	// Position set to 209
	s.Assert().Equal(209, r.Pos())

	// Read last array element's "Name" field again from the temp file.
	name, err = r.ReadStringField(tmp)
	s.Assert().Nil(err)
	s.Assert().Equal("this is from 2022", name)
	// Position increased by 4+17. String size uses 4 bytes and
	// string value uses 17 bytes.
	// 209+21=230
	s.Assert().Equal(230, r.Pos())
}
