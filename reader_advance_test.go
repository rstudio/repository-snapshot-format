// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReaderMigrationSuite struct {
	suite.Suite
}

func TestReaderMigrationSuite(t *testing.T) {
	suite.Run(t, &ReaderMigrationSuite{})
}

func (s *ReaderMigrationSuite) TestAdvanceFields() {
	buf := bufio.NewReader(getData(s.Suite))
	r := NewReader()

	// Read the index
	_, err := r.ReadIndex(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(101, r.Pos())

	// Record should be 132 bytes in length
	recordSz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(132, recordSz)
	// Position increased by 4 (size field is 4 bytes)
	s.Assert().Equal(105, r.Pos())

	// Company
	err = r.AdvanceTo(buf, "company")
	s.Assert().Nil(err)
	company, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("posit", company)
	// Position increased by 9. Size field is 4 bytes + data is 5 bytes.
	s.Assert().Equal(114, r.Pos())

	// Skip the ready field and advance to "list"
	// Array should be 100 bytes in size
	err = r.AdvanceTo(buf, "list")
	s.Assert().Nil(err)
	arrayPos := r.Pos()
	arraySz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(100, arraySz)
	// Position increased by 4
	s.Assert().Equal(119, r.Pos())
	// Get expect array end position
	arrayEndPos := arrayPos + arraySz
	s.Assert().Equal(215, arrayEndPos)

	// Array should be 3 elements in length
	arrayLen, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(3, arrayLen)
	// Position increased by 4
	s.Assert().Equal(123, r.Pos())

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
	// 123+42=165
	s.Assert().Equal(165, r.Pos())

	// Get the first array element's "Name" field
	err = r.AdvanceTo(buf, "list", "name")
	s.Assert().Nil(err)
	name, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("From 2020", name)
	// Position increased by 4+9. String size uses 4 bytes and
	// string value uses 9 bytes.
	// 165+13=178
	s.Assert().Equal(178, r.Pos())

	// Skip the "Verified" field and advance to second array element's "Name" field
	err = r.AdvanceToNextElement(buf)
	s.Assert().Nil(err)
	err = r.AdvanceTo(buf, "list", "name")
	s.Assert().Nil(err)
	name, err = r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("From 2021", name)
	// Position increased by 4+9+1. String size uses 4 bytes and
	// string value uses 9 bytes. Also, the skipped field "verified"
	// uses 1 byte
	// 178+13+1=192
	s.Assert().Equal(192, r.Pos())

	// Read the second array element's "Verified" field
	err = r.AdvanceTo(buf, "list", "verified")
	s.Assert().Nil(err)
	verified, err := r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().True(verified)
	s.Assert().Equal(193, r.Pos())

	// Skip the last array element's "Name" field and advance to "Verified".
	// This tests skipping an array sub-element.
	err = r.AdvanceToNextElement(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(193, r.Pos())
	err = r.AdvanceTo(buf, "list", "verified")
	s.Assert().Nil(err)
	// Last name field (skipped) read "this is from 2022"
	// 17 bytes + 4 bytes (size) = 21
	// 193 + 21 = 214
	s.Assert().Equal(214, r.Pos())
	verified, err = r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().True(verified)
	// 214 + 1 = 215 (arrayEndPos)
	s.Assert().Equal(arrayEndPos, r.Pos())

	// Skip age field and advance to "rating"
	err = r.AdvanceTo(buf, "rating")
	s.Assert().Nil(err)
	rating, err := r.ReadFloatField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(92.689, rating)

	// Verify at EOF.
	_, err = r.ReadSizeField(buf)
	s.Assert().ErrorIs(err, io.EOF)
}

func (s *ReaderMigrationSuite) TestAdvanceArray() {
	buf := bufio.NewReader(getData(s.Suite))
	r := NewReader()

	// Read the index
	_, err := r.ReadIndex(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(101, r.Pos())

	// Record should be 132 bytes in length
	recordSz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(132, recordSz)
	// Position increased by 4 (size field is 4 bytes)
	s.Assert().Equal(105, r.Pos())

	// Company
	err = r.AdvanceTo(buf, "company")
	s.Assert().Nil(err)
	company, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("posit", company)
	// Position increased by 9. Size field is 4 bytes + data is 5 bytes.
	s.Assert().Equal(114, r.Pos())

	// Skip the "ready" field and the "list" array and advance
	// to "age". This tests skipping both a regular field and an entire array.
	err = r.AdvanceTo(buf, "age")
	s.Assert().Nil(err)
	age, err := r.ReadIntField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(int64(55), age)
	s.Assert().Equal(225, r.Pos())

	// Read the "rating" field.
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
	err = r.Seek(193, tmp)
	s.Assert().Nil(err)
	// Position set to 193
	s.Assert().Equal(193, r.Pos())

	// Read last array element's "Name" field again from the temp file.
	name, err := r.ReadStringField(tmp)
	s.Assert().Nil(err)
	s.Assert().Equal("this is from 2022", name)
	s.Assert().Equal(214, r.Pos())
}

func (s *ReaderMigrationSuite) TestAdvanceErrors() {
	buf := bufio.NewReader(getData(s.Suite))
	r := NewReader()

	// Read the index
	_, err := r.ReadIndex(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(101, r.Pos())

	// Record should be 132 bytes in length
	recordSz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(132, recordSz)
	// Position increased by 4 (size field is 4 bytes)
	s.Assert().Equal(105, r.Pos())

	// Company
	err = r.AdvanceTo(buf, "company")
	s.Assert().Nil(err)
	company, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("posit", company)
	// Position increased by 9. Size field is 4 bytes + data is 5 bytes.
	s.Assert().Equal(114, r.Pos())

	// Attempt to advance to field that doesn't exist
	err = r.AdvanceTo(buf, "nothere")
	s.Assert().ErrorIs(err, ErrNoSuchField)
}
