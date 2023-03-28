// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

type rsfWriter struct {
	writer io.Writer
	pos    int
}

func NewWriter(f io.Writer) Writer {
	return &rsfWriter{writer: f}
}

func (f *rsfWriter) WriteSizeField(pos int, val int, r io.Writer) (int, error) {
	// Write size
	bs := make([]byte, sizeFieldLen)
	binary.LittleEndian.PutUint32(bs, uint32(val))
	sz, err := r.Write(bs)
	if err != nil {
		return 0, err
	}

	return pos + sz, nil
}

func (f *rsfWriter) WriteInt64Field(pos int, val int64, r io.Writer) (int, error) {
	// Write int
	bs := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(bs, val)
	sz, err := r.Write(bs)
	if err != nil {
		return 0, err
	}

	return pos + sz, nil
}

func (f *rsfWriter) WriteFloatField(pos int, val float64, r io.Writer) (int, error) {
	// Write float
	bs := make([]byte, size64)
	binary.LittleEndian.PutUint64(bs, math.Float64bits(val))
	sz, err := r.Write(bs)
	if err != nil {
		return 0, err
	}

	return pos + sz, nil
}

func (f *rsfWriter) WriteFixedStringField(pos, sz int, val string, r io.Writer) (int, error) {
	if sz != len(val) {
		return 0, fmt.Errorf("size %d does not match expected size %d", len(val), sz)
	}

	// Write value
	i, err := r.Write([]byte(val))
	if err != nil {
		return 0, err
	}
	if i != sz {
		return 0, fmt.Errorf("expected write size %d; wrote %d", sz, i)
	}

	return pos + sz, nil
}

func (f *rsfWriter) WriteStringField(pos int, val string, r io.Writer) (int, error) {
	// Write size
	bs := make([]byte, sizeFieldLen)
	binary.LittleEndian.PutUint32(bs, uint32(len(val)))
	sz, err := r.Write(bs)
	if err != nil {
		return 0, err
	}

	// Write value
	i, err := r.Write([]byte(val))
	if err != nil {
		return 0, err
	}
	sz += i

	return pos + sz, nil
}

func (f *rsfWriter) WriteBoolField(pos int, val bool, r io.Writer) (int, error) {
	// Write value
	var b []byte
	if val {
		b = []byte{1}
	} else {
		b = []byte{0}
	}
	sz, err := r.Write(b)
	if err != nil {
		return 0, err
	}

	return pos + sz, nil
}
