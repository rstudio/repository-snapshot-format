// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"io"
)

type Writer interface {
	// WriteObject uses reflection and `rsf` struct tag annotations to write an object.
	WriteObject(v any) (int, error)

	// WriteSizeField writes a 4-byte field that indicates a size (usually the
	// size in bytes of an object or value, or an array length).
	WriteSizeField(pos int, val int, r io.Writer) (int, error)

	// WriteFixedStringField writes a string of a fixed length. An error is returned
	// if the string size does not match the provided `sz` parameter.
	WriteFixedStringField(pos, sz int, val string, r io.Writer) (int, error)

	// WriteStringField writes a variable length string. The string value will be
	// prepended with a 4-byte size field that indicates the string length.
	WriteStringField(pos int, val string, r io.Writer) (int, error)

	// WriteBoolField writes a 1-byte (0 or 1) boolean value.
	WriteBoolField(pos int, val bool, r io.Writer) (int, error)

	// WriteInt64Field write a 10-byte signed int64 value.
	WriteInt64Field(pos int, val int64, r io.Writer) (int, error)

	// WriteFloatField write an 8-byte float64 value
	WriteFloatField(pos int, val float64, r io.Writer) (int, error)
}

// Reader - The Reader interface provides Read* methods analogous to the Write*
// methods in the Writer interface. No `ReadObject` method is provided since
// reading is likely to be customized per use case.
type Reader interface {
	ReadSizeField(r io.Reader) (int, error)
	ReadFixedStringField(sz int, r io.Reader) (string, error)
	ReadStringField(r io.Reader) (string, error)
	ReadBoolField(r io.Reader) (bool, error)
	ReadIntField(r io.Reader) (int64, error)
	ReadFloatField(r io.Reader) (float64, error)

	// AdvanceTo advances the reader to the field indicated by `fieldNames`.
	AdvanceTo(buf *bufio.Reader, fieldNames ...string) error

	// AdvanceToNextElement advances the reader to the end of the current
	// struct.
	AdvanceToNextElement(buf *bufio.Reader, fieldNames ...string) error

	// ReadIndex reads the object index at the top of an RSF file
	ReadIndex(r io.Reader) (Index, error)
	SetIndex(i Index)

	// Seek is used to seek a file position.
	Seek(pos int, r io.Seeker, fieldNames ...string) error

	// Discard discards `sz` bytes.
	Discard(sz int, r *bufio.Reader, fieldNames ...string) error

	// Pos returns the current position in the read buffer.
	Pos() int
}

// General constants
const (
	sizeFieldLen = 4
	sizeFloat64  = 8
	sizeInt64    = 10
)

// Constants used by `rsf` struct tags
const (
	//
	// Tags:
	//
	// The struct tag used to control serialization
	tagName = "rsf"

	//
	// Delimiters:
	//
	// Separates multiple struct tag parameters.
	rsfDelim = ","
	// Separates a struct tag parameter that uses the name:value format.
	rsfSep = ":"

	//
	// Parameters:
	//
	// When used as the only parameter (e.g., `rsf:"-"`), the field will be completely ignored.
	rsfIgnore = "-"
	// Instructs that field logic will run, but the field will not be serialized.
	rsfSkip = "skip"
	// Denotes a fixed-size field that does not require a size header.
	rsfFixed = "fixed"
	// Denotes that a field is used to index an array.
	rsfIndex = "index"
)

// A struct used to record and pass information about `rsf` struct tags
type tag struct {
	name     string
	fixed    int
	index    string
	indexSz  int
	indexVal string
}
