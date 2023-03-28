// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/suite"
)

type WriterUpgradeSuite struct {
	suite.Suite
}

func TestWriterUpgradeSuite(t *testing.T) {
	suite.Run(t, &WriterUpgradeSuite{})
}

func (s *WriterUpgradeSuite) TestWriteObjectAndUpgrade() {
	// Create data with a legacy struct.
	type legacySnap struct {
		Date     string `rsf:"date,skip,fixed:10"`
		Name     string `rsf:"name"`
		Verified bool   `rsf:"verified"`
		Skip     string `rsf:"-"`
	}
	a := struct {
		Skip    string       `rsf:"-"`
		Company string       `rsf:"company"`
		Ready   bool         `rsf:"ready"`
		List    []legacySnap `rsf:"list,index:date"`
		Age     int          `rsf:"age"`
		Rating  float64      `rsf:"rating"`
	}{
		Company: "posit",
		Ready:   true,
		Age:     55,
		Rating:  92.689,
		List: []legacySnap{
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
	buf1 := &bytes.Buffer{}
	w1 := NewWriter(buf1)
	sz, err := w1.WriteObject(a)
	s.Assert().Nil(err)
	s.Assert().Equal(233, sz)

	// Create some "upgraded" objects that include new fields not present in the
	// legacy structs that follow. The structs maintain the original fields and
	// values, but add many new fields.
	type snap struct {
		Guid     string `rsf:"guid,fixed:36"` // Not in original struct
		Date     string `rsf:"date,skip,fixed:10"`
		Name     string `rsf:"name"`
		Project  string `rsf:"project"` // Not in original struct
		Verified bool   `rsf:"verified"`
		Skip     string `rsf:"-"`
		SkipAlso string `rsf:"-"`     // Not in original struct
		Trust    bool   `rsf:"trust"` // Not in original struct
	}
	// Add a new type that was also not included in the original data.
	type product struct {
		Barcode string  `rsf:"barcode,skip,fixed:10"`
		Name    string  `rsf:"name"`
		Price   float32 `rsf:"price"`
	}
	b := struct {
		Location string    `rsf:"location"` // Not in original struct
		Skip     string    `rsf:"-"`
		Company  string    `rsf:"company"`
		Products []product `rsf:"products,index:barcode"` // Not in original struct
		Ready    bool      `rsf:"ready"`
		Portable bool      `rsf:"portable"` // Not in original struct
		List     []snap    `rsf:"list,index:date"`
		Income   float64   `rsf:"income"` // Not in original struct
		Age      int       `rsf:"age"`
		Rating   float64   `rsf:"rating"`
		Zip      int       `rsf:"zip"` // Not in original struct
	}{
		Location: "Albuquerque",
		Company:  "posit",
		Ready:    true,
		Portable: true,
		Income:   56999.98,
		Age:      55,
		Rating:   92.689,
		Zip:      75043,
		List: []snap{
			{
				Guid:     "199d22ca-719f-40e6-a108-1f2147564168",
				Date:     "2020-10-01",
				Name:     "From 2020",
				Project:  "albatross",
				Verified: false,
				SkipAlso: "test",
				Trust:    true,
			},
			{
				Guid:     "eba30155-b31c-4287-a7a1-1018010859c1",
				Date:     "2021-03-21",
				Name:     "From 2021",
				Project:  "bluebird",
				Verified: true,
				Trust:    false,
			},
			{
				Guid:     "c7f67f5f-7891-42b0-bdbc-82a0e5cd5572",
				Date:     "2022-12-15",
				Name:     "this is from 2022",
				Project:  "none",
				Verified: true,
				Trust:    true,
			},
		},
		Products: []product{
			{
				Barcode: "0123456789",
				Name:    "shovel",
				Price:   32.99,
			},
			{
				Barcode: "9876543210",
				Name:    "rake",
				Price:   15.44,
			},
		},
	}
	buf2 := &bytes.Buffer{}
	w2 := NewWriter(buf2)
	sz, err = w2.WriteObject(b)
	s.Assert().Nil(err)
	s.Assert().Equal(627, sz)

	// Read the legacy struct with the expected set of fields.
	s.validateRead(buf1)

	// Read the new struct with all the new fields. The results should
	// be identical.
	s.validateRead(buf2)
}

func (s *WriterUpgradeSuite) validateRead(b *bytes.Buffer) {

	// Read index
	r := NewReader()
	err := r.ReadIndex(b)
	s.Assert().Nil(err)

	// Read object size.
	sz, err := r.ReadSizeField(b)
	s.Assert().Nil(err)
	// Since we've already read the index, the object size should be the
	// remaining buffer bytes, plus 4 for the size field we just read.
	s.Assert().Equal(b.Len()+4, sz)

	buf := bufio.NewReader(b)

	// Advance to company
	err = r.AdvanceTo(buf, "company")
	s.Assert().Nil(err)
	company, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("posit", company)

	// Advance to ready
	err = r.AdvanceTo(buf, "ready")
	s.Assert().Nil(err)
	ready, err := r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().True(ready)

	// Advance to list
	err = r.AdvanceTo(buf, "list")
	s.Assert().Nil(err)

	// Save start position for array
	objectStart := r.Pos()

	// Full array size
	arraySz, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)

	// Array should be 3 elements in length
	arrayLen, err := r.ReadSizeField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(3, arrayLen)

	// Array index. Read all three index entries
	// Entry 1
	date, err := r.ReadFixedStringField(10, buf)
	s.Assert().Nil(err)
	s.Assert().Equal("2020-10-01", date)
	_, err = r.ReadSizeField(buf)
	s.Assert().Nil(err)
	//
	// Entry 2
	date, err = r.ReadFixedStringField(10, buf)
	s.Assert().Nil(err)
	s.Assert().Equal("2021-03-21", date)
	_, err = r.ReadSizeField(buf)
	s.Assert().Nil(err)
	//
	// Entry 3
	date, err = r.ReadFixedStringField(10, buf)
	s.Assert().Nil(err)
	s.Assert().Equal("2022-12-15", date)
	_, err = r.ReadSizeField(buf)
	s.Assert().Nil(err)

	// Get the first array element's "Name" field
	err = r.AdvanceTo(buf, "list", "name")
	s.Assert().Nil(err)
	name, err := r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("From 2020", name)

	// Read the first array element's "Verified" field
	err = r.AdvanceTo(buf, "list", "verified")
	s.Assert().Nil(err)
	verified, err := r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().False(verified)

	// Advance to the second array element
	err = r.AdvanceToNextElement(buf)
	s.Assert().Nil(err)

	// Get the second array element's "Name" field
	err = r.AdvanceTo(buf, "list", "name")
	s.Assert().Nil(err)
	name, err = r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("From 2021", name)

	// Read the second array element's "Verified" field
	err = r.AdvanceTo(buf, "list", "verified")
	s.Assert().Nil(err)
	verified, err = r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().True(verified)

	// Advance to the third array element
	err = r.AdvanceToNextElement(buf)
	s.Assert().Nil(err)

	// Get the third array element's "Name" field
	err = r.AdvanceTo(buf, "list", "name")
	s.Assert().Nil(err)
	name, err = r.ReadStringField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal("this is from 2022", name)

	// Read the third array element's "Verified" field
	err = r.AdvanceTo(buf, "list", "verified")
	s.Assert().Nil(err)
	verified, err = r.ReadBoolField(buf)
	s.Assert().Nil(err)
	s.Assert().True(verified)

	// Advance to the array end
	err = r.AdvanceToNextElement(buf)
	s.Assert().Nil(err)

	// Verify at end of array
	s.Assert().Equal(arraySz, r.Pos()-objectStart)

	// Advance to age
	err = r.AdvanceTo(buf, "age")
	s.Assert().Nil(err)
	age, err := r.ReadInt64Field(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(int64(55), age)

	// Advance to rating
	err = r.AdvanceTo(buf, "rating")
	s.Assert().Nil(err)
	rating, err := r.ReadFloatField(buf)
	s.Assert().Nil(err)
	s.Assert().Equal(92.689, rating)

	// Advance to end of struct
	err = r.AdvanceToNextElement(buf)
	s.Assert().Nil(err)

	// Verify at EOF.
	_, err = r.ReadSizeField(buf)
	s.Assert().ErrorIs(err, io.EOF)
}
