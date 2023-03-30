// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type rsfReader struct {
	pos int

	// When reading an RSF file based on a struct, the first entry
	// is an index. See `ReadIndex`.
	index Index

	// Saves the current position for advancing the reader.
	at []string
}

func NewReader() Reader {
	return &rsfReader{}
}

func (f *rsfReader) Pos() int {
	return f.pos
}

func (f *rsfReader) Seek(pos int, r io.Seeker) error {
	i, err := r.Seek(int64(pos), 0)
	f.pos = int(i)
	return err
}

func (f *rsfReader) Discard(sz int, r *bufio.Reader) error {
	i, err := r.Discard(sz)
	if err != nil {
		return err
	} else if i != sz {
		return fmt.Errorf("unexpected discard size %d; expected %d", i, sz)
	}
	f.pos += i
	return nil
}

func (f *rsfReader) ReadSizeField(r io.Reader) (int, error) {
	bs := make([]byte, sizeFieldLen)
	i, err := io.ReadFull(r, bs)
	if err != nil {
		return 0, err
	} else if i != sizeFieldLen {
		return 0, fmt.Errorf("unexpected read size %d; expected %d", i, sizeFieldLen)
	}
	f.pos += i
	sz := binary.LittleEndian.Uint32(bs)
	return int(sz), nil
}

func (f *rsfReader) ReadIntField(r io.Reader) (int64, error) {
	bs := make([]byte, sizeInt64)
	i, err := io.ReadFull(r, bs)
	if err != nil {
		return 0, err
	} else if i != sizeInt64 {
		return 0, fmt.Errorf("unexpected read size %d; expected %d", i, sizeInt64)
	}
	f.pos += i
	intVal, _ := binary.Varint(bs)
	return intVal, nil
}

func (f *rsfReader) ReadFloatField(r io.Reader) (float64, error) {
	bs := make([]byte, sizeFloat64)
	i, err := io.ReadFull(r, bs)
	if err != nil {
		return 0, err
	} else if i != sizeFloat64 {
		return 0, fmt.Errorf("unexpected read size %d; expected %d", i, sizeFloat64)
	}
	f.pos += i
	return math.Float64frombits(binary.LittleEndian.Uint64(bs)), nil
}

func (f *rsfReader) ReadFixedStringField(sz int, r io.Reader) (string, error) {
	// Read string field
	bs := make([]byte, sz)
	i, err := io.ReadFull(r, bs)
	if err != nil {
		return "", err
	} else if i != sz {
		return "", fmt.Errorf("unexpected read size %d; expected %d", i, sz)
	}
	f.pos += i

	return string(bs), nil
}

func (f *rsfReader) ReadStringField(r io.Reader) (string, error) {
	// read size
	bs := make([]byte, sizeFieldLen)
	i, err := io.ReadFull(r, bs)
	if err != nil {
		return "", err
	} else if i != sizeFieldLen {
		return "", fmt.Errorf("unexpected read size %d; expected %d", i, sizeFieldLen)
	}
	f.pos += i

	sz := binary.LittleEndian.Uint32(bs)
	// Read string field
	bs = make([]byte, sz)
	i, err = io.ReadFull(r, bs)
	if err != nil {
		return "", err
	} else if i != int(sz) {
		return "", fmt.Errorf("unexpected read size %d; expected %d", i, sz)
	}
	f.pos += i

	return string(bs), nil
}

func (f *rsfReader) ReadBoolField(r io.Reader) (bool, error) {
	// Read bool field
	bs := make([]byte, 1)
	i, err := io.ReadFull(r, bs)
	if err != nil {
		return false, err
	} else if i != 1 {
		return false, fmt.Errorf("unexpected read size %d; expected %d", i, 1)
	}
	f.pos += i

	return bs[0] == 1, nil
}
