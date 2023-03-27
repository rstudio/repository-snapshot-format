// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"bytes"
	"io"
	"os"
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
func (s *ReaderSuite) getData() *bytes.Buffer {
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

	// Write the test data to the buffer
	_, err := w.WriteObject(a)
	s.Assert().Nil(err)
	return buf
}

func (s *ReaderSuite) TestRead() {
	buf := bufio.NewReader(s.getData())
	r := NewReader()

	// Object index size
	indexSz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(72, indexSz)
	// Position increased by 4 (size field is 4 bytes)
	s.Assert().Equal(4, r.Pos())

	// TODO don't skip header bytes
	err = r.Discard(indexSz-4, buf)
	s.Assert().Nil(err)

	// Record should be 110 bytes in length
	recordSz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(114, recordSz)
	// Position increased by 4 (size field is 4 bytes)
	s.Assert().Equal(76, r.Pos())

	// Company
	company, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("posit", company)
	// Position increased by 9. Size field is 4 bytes + data is 5 bytes.
	s.Assert().Equal(85, r.Pos())

	// Read
	ready, err := r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().True(ready)
	// Position increased by 1
	s.Assert().Equal(86, r.Pos())

	// Array should be 100 bytes in size
	arraySz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(100, arraySz)
	// Position increased by 4
	s.Assert().Equal(90, r.Pos())

	// Array should be 3 elements in length
	arrayLen, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(3, arrayLen)
	// Position increased by 4
	s.Assert().Equal(94, r.Pos())

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
	// 94+42=136
	s.Assert().Equal(136, r.Pos())

	// Discard 28 bytes (14+14) to move to the last array element.
	err = r.Discard(28, buf)
	s.Assert().Nil(err)
	// Position increased by 28 to 136+28=136.
	s.Assert().Equal(164, r.Pos())

	// Read last array element's "Name" field.
	name, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("this is from 2022", name)
	// Position increased by 4+17. String size uses 4 bytes and
	// string value uses 17 bytes.
	// 164+21=185
	s.Assert().Equal(185, r.Pos())

	// Read last array element's "Verified" field.
	verified, err := r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().True(verified)
	// Position increased by 1.
	s.Assert().Equal(186, r.Pos())

	// Verify at EOF.
	_, err = r.ReadSizeField(buf)
	s.Assert().ErrorIs(err, io.EOF)

	// Dump buffer to temp file to test `Seek`
	tmp, err := os.CreateTemp("", "")
	s.Assert().Nil(err)
	defer os.Remove(tmp.Name())
	buf = bufio.NewReader(s.getData())
	_, err = io.Copy(tmp, buf)

	// Seek back to the last array element.
	err = r.Seek(164, tmp)
	s.Assert().Nil(err)
	// Position set to 164
	s.Assert().Equal(164, r.Pos())

	// Read last array element's "Name" field again from the temp file.
	name, err = r.ReadStringField(tmp)
	s.Assert().Nil(err)
	s.Assert().Equal("this is from 2022", name)
	// Position increased by 4+17. String size uses 4 bytes and
	// string value uses 17 bytes.
	// 164+21=185
	s.Assert().Equal(185, r.Pos())

}
