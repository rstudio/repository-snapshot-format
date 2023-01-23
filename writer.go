// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
)

type rsfWriter struct {
	writer io.Writer
}

func NewWriter(f io.Writer) Writer {
	return &rsfWriter{writer: f}
}

func (f *rsfWriter) WriteObject(v any) (int, error) {
	var buf = &bytes.Buffer{}
	var totalSz int
	totalSz, err := f.writeObject(reflect.ValueOf(v), &tag{}, buf)
	if err != nil {
		return 0, err
	}

	// Write size of full record
	bs := make([]byte, sizeFieldLen)
	recordSize := buf.Len() + sizeFieldLen
	binary.LittleEndian.PutUint32(bs, uint32(recordSize))
	sz, err := f.writer.Write(bs)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	// Write initial buffer. This includes the name and the number
	// of snapshots.
	_, err = io.Copy(f.writer, buf)
	if err != nil {
		return 0, err
	}

	return totalSz, nil
}

func (f *rsfWriter) writeObject(v reflect.Value, t *tag, buf *bytes.Buffer) (int, error) {
	switch v.Type().Kind() {
	case reflect.Array, reflect.Slice:
		return f.writeArray(v, t, buf)
	case reflect.Struct:
		return f.writeStruct(v, t, buf)
	case reflect.String:
		return f.writeString(v.String(), t, buf)
	case reflect.Bool:
		return f.WriteBoolField(0, v.Bool(), buf)
	default:
		return 0, fmt.Errorf("unknown field type %#v: %#v", v.Type().Kind(), v)
	}
}

func (f *rsfWriter) writeStruct(v reflect.Value, tParent *tag, buf *bytes.Buffer) (int, error) {
	var totalSz int
	for i := 0; i < v.NumField(); i++ {
		// Get the field tag value
		rawTag := v.Type().Field(i).Tag.Get(tagName)
		if rawTag == rsfIgnore {
			continue
		}

		t := &tag{}
		var skip bool

		fieldVal := v.Field(i)
		if rawTag != "" {
			tagParts := strings.Split(rawTag, rsfDelim)
			t.name = tagParts[0]
			for j := 1; j < len(tagParts); j++ {
				part := strings.TrimSpace(strings.ToLower(tagParts[j]))
				if part == rsfSkip {
					skip = true
				}
				if strings.HasPrefix(part, rsfIndex+rsfSep) && len(part) > 6 {
					indexParts := strings.Split(part, rsfSep)
					t.index = indexParts[1]
				}
				if strings.HasPrefix(part, rsfFixed+rsfSep) && len(part) > 6 {
					fixedParts := strings.Split(part, rsfSep)
					var err error
					t.fixed, err = strconv.Atoi(fixedParts[1])
					if err != nil {
						return 0, err
					}
				}
			}
			if tParent.index == t.name {
				tParent.indexVal = fieldVal.String()
				tParent.indexSz = t.fixed
			}
		}
		if !skip {
			sz, err := f.writeObject(fieldVal, t, buf)
			if err != nil {
				return 0, err
			}
			totalSz += sz
		}
	}
	return totalSz, nil
}

func (f *rsfWriter) writeArray(v reflect.Value, t *tag, buf *bytes.Buffer) (int, error) {
	var snapIndexBuf *bytes.Buffer
	var snapBuf *bytes.Buffer
	if t.index != "" {
		snapIndexBuf = &bytes.Buffer{}
		snapBuf = &bytes.Buffer{}
	} else {
		snapBuf = buf
	}
	var totalSz int
	totalSz, err := f.WriteSizeField(0, v.Len(), buf)
	if err != nil {
		return 0, err
	}
	var lastLen int
	var sz int
	for i := 0; i < v.Len(); i++ {
		el := v.Index(i)
		sz, err = f.writeObject(el, t, snapBuf)
		if err != nil {
			return 0, err
		}
		totalSz += sz
		bufLen := snapBuf.Len()

		if t.index != "" {
			sz, err = f.WriteFixedStringField(0, t.indexSz, t.indexVal, snapIndexBuf)
			if err != nil {
				return 0, err
			}
			totalSz += sz
			sz, err = f.WriteSizeField(0, bufLen-lastLen, snapIndexBuf)
			if err != nil {
				return 0, err
			}
			totalSz += sz
			lastLen = bufLen
		}
	}
	if t.index != "" {
		_, err = io.Copy(buf, snapIndexBuf)
		if err != nil {
			return 0, err
		}
		_, err = io.Copy(buf, snapBuf)
		if err != nil {
			return 0, err
		}
	}
	return totalSz, nil
}

func (f *rsfWriter) writeString(s string, t *tag, buf *bytes.Buffer) (int, error) {
	var err error
	var sz int
	if t.fixed > 0 {
		sz, err = f.WriteFixedStringField(0, t.fixed, s, buf)
	} else {
		sz, err = f.WriteStringField(0, s, buf)
	}
	return sz, err
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
