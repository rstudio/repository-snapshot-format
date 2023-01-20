// Copyright (C) 2022 by Posit Software, PBC
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

type writer struct {
	f io.Writer
}

func NewWriter(f io.Writer) Writer {
	return &writer{f: f}
}

func (f *writer) WriteObject(v any) error {
	var buf = &bytes.Buffer{}
	var err error
	err = f.writeObject(reflect.ValueOf(v), &tag{}, buf)
	if err != nil {
		return err
	}

	// Write size of full record
	bs := make([]byte, 4)
	recordSize := buf.Len() + 4
	binary.LittleEndian.PutUint32(bs, uint32(recordSize))
	_, err = f.f.Write(bs)
	if err != nil {
		return err
	}

	// Write initial buffer. This includes the name and the number
	// of snapshots.
	_, err = io.Copy(f.f, buf)
	if err != nil {
		return err
	}

	return nil
}

func (f *writer) writeObject(v reflect.Value, t *tag, buf *bytes.Buffer) error {
	switch v.Type().Kind() {
	case reflect.Array, reflect.Slice:
		return f.writeArray(v, t, buf)
	case reflect.Struct:
		return f.writeStruct(v, t, buf)
	case reflect.String:
		return f.writeString(v.String(), t, buf)
	case reflect.Bool:
		_, err := f.WriteBoolField(0, v.Bool(), buf)
		return err
	default:
		return fmt.Errorf("unknown field type %#v: %#v", v.Type().Kind(), v)
	}
}

func (f *writer) writeStruct(v reflect.Value, tParent *tag, buf *bytes.Buffer) error {
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
						return err
					}
				}
			}
			if tParent.index == t.name {
				tParent.indexVal = fieldVal.String()
				tParent.indexSz = t.fixed
			}
		}
		if !skip {
			err := f.writeObject(fieldVal, t, buf)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *writer) writeArray(v reflect.Value, t *tag, buf *bytes.Buffer) error {
	var snapIndexBuf *bytes.Buffer
	var snapBuf *bytes.Buffer
	if t.index != "" {
		snapIndexBuf = &bytes.Buffer{}
		snapBuf = &bytes.Buffer{}
	} else {
		snapBuf = buf
	}
	_, err := f.WriteSizeField(0, v.Len(), buf)
	if err != nil {
		return err
	}
	var lastLen int
	for i := 0; i < v.Len(); i++ {
		el := v.Index(i)
		err = f.writeObject(el, t, snapBuf)
		if err != nil {
			return err
		}
		bufLen := snapBuf.Len()
		if t.index != "" {
			_, err = f.WriteFixedStringField(0, t.indexSz, t.indexVal, snapIndexBuf)
			if err != nil {
				return err
			}
			_, err = f.WriteSizeField(0, bufLen-lastLen, snapIndexBuf)
			if err != nil {
				return err
			}
			lastLen = bufLen
		}
	}
	if t.index != "" {
		_, err = io.Copy(buf, snapIndexBuf)
		if err != nil {
			return err
		}
		_, err = io.Copy(buf, snapBuf)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *writer) writeString(s string, t *tag, buf *bytes.Buffer) error {
	var err error
	if t.fixed > 0 {
		_, err = f.WriteFixedStringField(0, t.fixed, s, buf)
	} else {
		_, err = f.WriteStringField(0, s, buf)
	}
	return err
}

func (f *writer) WriteSizeField(pos int, val int, r io.Writer) (int, error) {
	var i, sz int
	var err error

	// Write size
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(val))
	i, err = r.Write(bs)
	if err != nil {
		return 0, err
	}
	sz += i

	return pos + sz, nil
}

func (f *writer) WriteFixedStringField(pos, sz int, val string, r io.Writer) (int, error) {
	var i int
	var err error

	if sz != len(val) {
		return 0, fmt.Errorf("size %d does not match expected size %d", len(val), sz)
	}

	// Write value
	i, err = r.Write([]byte(val))
	if err != nil {
		return 0, err
	}
	if i != sz {
		return 0, fmt.Errorf("expected write size %d; wrote %d", sz, i)
	}

	return pos + sz, nil
}

func (f *writer) WriteStringField(pos int, val string, r io.Writer) (int, error) {
	var i, sz int
	var err error

	// Write size
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(len(val)))
	i, err = r.Write(bs)
	if err != nil {
		return 0, err
	}
	sz += i

	// Write value
	i, err = r.Write([]byte(val))
	if err != nil {
		return 0, err
	}
	sz += i

	return pos + sz, nil
}

func (f *writer) WriteBoolField(pos int, val bool, r io.Writer) (int, error) {
	var i, sz int
	var err error

	// Write value
	var b []byte
	if val {
		b = []byte{1}
	} else {
		b = []byte{0}
	}
	i, err = r.Write(b)
	if err != nil {
		return 0, err
	}
	sz += i

	return pos + sz, nil
}
