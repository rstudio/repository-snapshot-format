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

func (f *rsfWriter) WriteObject(v any) (int, error) {
	var indexBuf = &bytes.Buffer{}
	var indexSz int
	var totalSz int
	var err error
	var sz int
	if f.pos == 0 {
		indexSz, err = f.writeIndexObject(reflect.TypeOf(v), &tag{}, indexBuf)
		if err != nil {
			return 0, err
		}
		totalSz += indexSz

		// Write index size
		bs := make([]byte, sizeFieldLen)
		indexRecordSize := indexBuf.Len() + sizeFieldLen
		binary.LittleEndian.PutUint32(bs, uint32(indexRecordSize))
		sz, err = f.writer.Write(bs)
		if err != nil {
			return 0, err
		}
		totalSz += sz

		// Write index
		_, err = io.Copy(f.writer, indexBuf)
		if err != nil {
			return 0, err
		}
	}

	var buf = &bytes.Buffer{}
	var objectSz int
	objectSz, err = f.writeObject(reflect.ValueOf(v), &tag{}, buf)
	if err != nil {
		return 0, err
	}
	totalSz += objectSz

	// Write size of full record
	bs := make([]byte, sizeFieldLen)
	recordSize := buf.Len() + sizeFieldLen
	binary.LittleEndian.PutUint32(bs, uint32(recordSize))
	sz, err = f.writer.Write(bs)
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

	// Increment once per object
	f.pos++

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
		t := &tag{}
		skip, err := getTagInfo(v.Type(), i, t, tParent, v.Field(i).String())
		if err != nil {
			return 0, err
		}

		if !skip {
			var sz int
			sz, err = f.writeObject(v.Field(i), t, buf)
			if err != nil {
				return 0, err
			}
			totalSz += sz
		}
	}
	return totalSz, nil
}

func getTagInfo(v reflect.Type, index int, t, tParent *tag, fieldVal string) (bool, error) {
	// Get the field tag value
	rawTag := v.Field(index).Tag.Get(tagName)
	if rawTag == rsfIgnore {
		return true, nil
	}

	var skip bool
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
					return false, err
				}
			}
		}
		if tParent.index == t.name {
			tParent.indexVal = fieldVal
			tParent.indexSz = t.fixed
		}
	}
	return skip, nil
}

func (f *rsfWriter) writeArray(v reflect.Value, t *tag, buf *bytes.Buffer) (int, error) {
	snapBuf := &bytes.Buffer{}
	var snapIndexBuf *bytes.Buffer
	if t.index != "" {
		snapIndexBuf = &bytes.Buffer{}
	}

	var totalSz int
	var lastLen int
	var err error
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

	// Write the size of the entire array, including the size, length, index, and elements.
	totalSz += sizeFieldLen + sizeFieldLen
	_, err = f.WriteSizeField(0, totalSz, buf)
	if err != nil {
		return 0, err
	}

	// Write the array length.
	_, err = f.WriteSizeField(0, v.Len(), buf)
	if err != nil {
		return 0, err
	}

	// Write the index, if included.
	if t.index != "" {
		_, err = io.Copy(buf, snapIndexBuf)
		if err != nil {
			return 0, err
		}
	}

	// Write the array elements
	_, err = io.Copy(buf, snapBuf)
	if err != nil {
		return 0, err
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
