// Copyright (C) 2022 by Posit Software, PBC
package rsf

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type reader struct {
	pos int
}

func NewReader() Reader {
	return &reader{}
}

func (f *reader) Pos() int {
	return f.pos
}

func (f *reader) Discard(sz int, r *bufio.Reader) error {
	i, err := r.Discard(sz)
	if err != nil {
		return err
	} else if i != sz {
		return fmt.Errorf("unexpected discard size %d; expected %d", i, sz)
	}
	f.pos += i
	return nil
}

func (f *reader) ReadSizeField(r io.Reader) (int, error) {
	var i int
	var sz uint32
	var err error

	bs := make([]byte, 4)
	i, err = io.ReadFull(r, bs)
	if err != nil {
		return 0, err
	} else if i != 4 {
		return 0, fmt.Errorf("unexpected read size %d; expected %d", i, 4)
	}
	f.pos += i
	sz = binary.LittleEndian.Uint32(bs)
	return int(sz), nil
}

func (f *reader) ReadFixedStringField(sz int, r io.Reader) (string, error) {
	var i int
	var err error

	// Read string field
	bs := make([]byte, sz)
	i, err = io.ReadFull(r, bs)
	if err != nil {
		return "", err
	} else if i != sz {
		return "", fmt.Errorf("unexpected read size %d; expected %d", i, sz)
	}
	f.pos += i

	return string(bs), nil
}

func (f *reader) ReadStringField(r io.Reader) (string, error) {
	var i int
	var sz uint32
	var err error

	// read size
	bs := make([]byte, 4)
	i, err = io.ReadFull(r, bs)
	if err != nil {
		return "", err
	} else if i != 4 {
		return "", fmt.Errorf("unexpected read size %d; expected %d", i, 4)
	}
	f.pos += i

	sz = binary.LittleEndian.Uint32(bs)
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

func (f *reader) ReadBoolField(r io.Reader) (bool, error) {
	var i int
	var err error

	// Read bool field
	bs := make([]byte, 1)
	i, err = io.ReadFull(r, bs)
	if err != nil {
		return false, err
	} else if i != 1 {
		return false, fmt.Errorf("unexpected read size %d; expected %d", i, 1)
	}
	f.pos += i

	return bs[0] == 1, nil
}
