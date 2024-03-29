// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type Index []IndexEntry

const Top = ""

type IndexEntry struct {
	FieldName    string
	FieldType    int
	FieldSize    int
	Indexed      bool
	IndexSize    int
	IndexType    int
	SubfieldType int
	Subfields    Index
}

func (f *rsfReader) SetIndex(newIndex Index) {
	f.index = newIndex
}

func (f *rsfReader) ReadIndex(r io.Reader) (Index, error) {
	var err error

	// Peek at the first three bytes to see if an index version is included
	header := make([]byte, 3)
	n, err := r.Read(header)
	if err != nil {
		return nil, err
	}
	if n != 3 {
		return nil, fmt.Errorf("unexpected index header read length %d", n)
	}

	// If the first three bytes equal an index version, then record the
	// index version.
	if bytes.Equal(header, IndexVersion2) {
		f.indexVersion = 2
		f.pos += 3
	} else {
		f.indexVersion = 1
	}

	var sz int
	if f.indexVersion > 1 {
		// If an index version was found, simply read the full size field.
		sz, err = f.ReadSizeField(r)
		if err != nil {
			return nil, err
		}
	} else {
		// If an index version was not found, we need to read one more byte to get
		// the size, since we've already read the first three bytes.
		// used `Peek` to determine the first three byte values.
		lastByte := make([]byte, 1)
		n, err = r.Read(lastByte)
		if err != nil {
			return nil, err
		}
		if n != 1 {
			return nil, fmt.Errorf("unexpected index size supplemental read length %d", n)
		}

		// Manually increment pos
		f.pos += 4
		size := append(header, lastByte[0])
		sz = int(binary.LittleEndian.Uint32(size))
	}

	// Position when done reading index will be the current reader position +
	// the index size, minus the size field length, since we've already read it.
	f.index, err = f.readIndexEntries(r, f.pos+sz-sizeFieldLen, 0)
	return f.index, err
}

func (f *rsfReader) readIndexEntries(r io.Reader, finalPos, limit int) (Index, error) {
	var err error

	entries := make([]IndexEntry, 0)
	var pass int
	for {
		// We call this method recursively to read array subfields. In those cases, we know
		// how many fields to read (limit).
		if limit != 0 && pass == limit {
			break
		}
		pass++

		// When we've completed reading the index, the file position is at the index size (sz).
		if f.pos == finalPos {
			break
		}

		// Read the field name.
		var fieldName string
		fieldName, err = f.ReadStringField(r)
		if err != nil {
			return nil, err
		}

		// Read the field type.
		var fieldType int
		fieldType, err = f.ReadSizeField(r)
		if err != nil {
			return nil, err
		}

		// For arrays, read the count of the number of subfields.
		var subfieldCount int
		var indexed bool
		var arrayFieldType int
		var indexSize, indexType int
		if fieldType == FieldTypeArray {

			// Older indexes didn't include the following two fields
			if f.indexVersion >= 2 {
				indexed, err = f.ReadBoolField(r)
				if err != nil {
					return nil, err
				}

				if indexed {
					indexType, err = f.ReadSizeField(r)
					if err != nil {
						return nil, err
					}

					indexSize, err = f.ReadSizeField(r)
					if err != nil {
						return nil, err
					}
				}

				arrayFieldType, err = f.ReadSizeField(r)
				if err != nil {
					return nil, err
				}

			}

			subfieldCount, err = f.ReadSizeField(r)
			if err != nil {
				return nil, err
			}
		}

		// For fixed-length strings, read the string size.
		var fieldSize int
		if fieldType == FieldTypeFixedStr {
			fieldSize, err = f.ReadSizeField(r)
			if err != nil {
				return nil, err
			}
		}

		// If there's a bad index, we may read past the expected size. This is a serious error.
		if f.pos > finalPos {
			return nil, fmt.Errorf("unexpected index position %d; index max pos reported is %d", f.pos, finalPos)
		}

		// For arrays, recursively read the array subfields into a new array of entries.
		var subfields []IndexEntry
		if subfieldCount > 0 {
			// Enumerate the subfields
			subfields, err = f.readIndexEntries(r, finalPos, subfieldCount)
			if err != nil {
				return nil, err
			}
		}

		// Append the index entry, including any subfields.
		entries = append(entries, IndexEntry{
			FieldName:    fieldName,
			FieldType:    fieldType,
			FieldSize:    fieldSize,
			SubfieldType: arrayFieldType,
			Subfields:    subfields,
			Indexed:      indexed,
			IndexSize:    indexSize,
			IndexType:    indexType,
		})
	}

	return entries, nil
}

func (f *rsfReader) advance(advField IndexEntry, buf *bufio.Reader) error {
	var err error
	switch advField.FieldType {
	case FieldTypeFixedStr:
		err = f.Discard(advField.FieldSize, buf)
	case FieldTypeArray:
		var sz int
		sz, err = f.ReadSizeField(buf)
		if err != nil {
			return err
		}
		err = f.Discard(sz-sizeFieldLen, buf)
	case FieldTypeVarStr:
		var sz int
		sz, err = f.ReadSizeField(buf)
		if err != nil {
			return err
		}
		err = f.Discard(sz, buf)
	case FieldTypeBool:
		err = f.Discard(1, buf)
	case FieldTypeInt64:
		err = f.Discard(sizeInt64, buf)
	case FieldTypeFloat:
		err = f.Discard(sizeFloat64, buf)
	default:
		return fmt.Errorf("unexpected index field type %d", advField.FieldType)
	}

	return err
}

var ErrNoSuchField = errors.New("field not found")

func (f *rsfReader) AdvanceTo(buf *bufio.Reader, fieldNames ...string) error {
	at := f.at
	if len(fieldNames) < len(at) {
		at = f.at[:len(fieldNames)]
	} else if len(at) < len(fieldNames) {
		at = append(at, Top)
	}

	from, fromPos, err := entrySet(f.index, at...)
	if err != nil {
		return err
	}

	_, toPos, err := entrySet(f.index, fieldNames...)
	if err != nil {
		return err
	}

	for i := fromPos + 1; i < toPos; i++ {
		err = f.advance(from[i], buf)
		if err != nil {
			return err
		}
	}

	f.at = fieldNames

	return nil

}

func (f *rsfReader) AdvanceToNextElement(buf *bufio.Reader, fieldNames ...string) error {
	from, fromPos, err := entrySet(f.index, f.at...)
	if err != nil {
		return err
	}

	for i := fromPos + 1; i < len(from); i++ {
		err = f.advance(from[i], buf)
		if err != nil {
			return err
		}
	}

	if len(fieldNames) > 0 {
		f.at = fieldNames
	} else {
		at := f.at[:len(f.at)-1]
		at = append(at, Top)
		f.at = at
	}

	return nil

}

func entrySet(index Index, fieldNames ...string) (Index, int, error) {
	var atPos int

	if fieldNames == nil {
		fieldNames = []string{Top}
	}

	// Look up fields in path
	at := index
	next := index
	for _, field := range fieldNames {
		var found bool
		for pos, entry := range next {
			if entry.FieldName == field || field == Top {
				found = true
				at = next
				if field == Top {
					atPos = -1
				} else {
					atPos = pos
				}
				next = entry.Subfields
				break
			}
		}
		if !found {
			return nil, 0, ErrNoSuchField
		}
	}
	return at, atPos, nil
}
