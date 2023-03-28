// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bytes"
	"fmt"
	"reflect"
)

/*

When writing a struct at position zero, we first write an index that
describes the struct fields in the object.

Format:

  [header size]
  [field 1]
  [field 1 name size]
  [field 1 name]
  [field 1 type]
  [field n name size]
  [field n name]
  [field n type]

Example:

  0x48, 0x0, 0x0, 0x0,                            // 72 bytes full header size

  0x7, 0x0, 0x0, 0x0,                             // 7 bytes
  0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79,       // "company"
  0x1, 0x0, 0x0, 0x0,                             // FieldTypeVariable

  0x5, 0x0, 0x0, 0x0,                             // 5 bytes
  0x72, 0x65, 0x61, 0x64, 0x79,                   // "ready"
  0x2, 0x0, 0x0, 0x0,                             // FieldTypeFixed

  0x4, 0x0, 0x0, 0x0,                             // 4 bytes
  0x6c, 0x69, 0x73, 0x74,                         // "list"
  0x3, 0x0, 0x0, 0x0,                             // FieldTypeArray

  0x4, 0x0, 0x0, 0x0,                             // 4 bytes
  0x6e, 0x61, 0x6d, 0x65,                         // "name"
  0x1, 0x0, 0x0, 0x0,                             // FieldTypeVariable

  0x8, 0x0, 0x0, 0x0,                             // 8 bytes
  0x76, 0x65, 0x72, 0x69, 0x66, 0x69, 0x65, 0x64, // "verified"
  0x2, 0x0, 0x0, 0x0,                             // FieldTypeFixed

*/

const (
	FieldTypeVariable = 1
	FieldTypeFixed    = 2
	FieldTypeArray    = 3
)

func (f *rsfWriter) writeIndexObject(v reflect.Type, t *tag, buf *bytes.Buffer) (int, error) {
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		return f.writeIndexArray(v, t, buf)
	case reflect.Struct:
		return f.writeIndexStruct(v, t, buf)
	case reflect.String:
		return f.writeIndexString(t, buf)
	case reflect.Bool:
		return f.writeIndexFixed(t, buf)
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return f.writeIndexFixed(t, buf)
	case reflect.Float32, reflect.Float64:
		return f.writeIndexFixed(t, buf)
	default:
		return 0, fmt.Errorf("unknown field type %#v: %#v", v.Kind(), v)
	}
}

func (f *rsfWriter) writeIndexStruct(v reflect.Type, tParent *tag, buf *bytes.Buffer) (int, error) {
	var totalSz int
	for i := 0; i < v.NumField(); i++ {
		t := &tag{}
		skip, err := getTagInfo(v, i, t, tParent, "")
		if err != nil {
			return 0, err
		}

		if !skip {
			var sz int
			sz, err = f.writeIndexObject(v.Field(i).Type, t, buf)
			if err != nil {
				return 0, err
			}
			totalSz += sz
		}
	}

	return totalSz, nil
}

func (f *rsfWriter) writeIndexArray(v reflect.Type, t *tag, buf *bytes.Buffer) (int, error) {
	var totalSz int
	sz, err := f.WriteStringField(0, t.name, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	sz, err = f.WriteSizeField(0, FieldTypeArray, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	// For struct arrays, write additional info about the struct
	el := v.Elem()
	if el.Kind() == reflect.Struct {
		sz, err = f.writeIndexStruct(el, t, buf)
		totalSz += sz
	}

	return totalSz, err
}

func (f *rsfWriter) writeIndexString(t *tag, buf *bytes.Buffer) (int, error) {
	if t.fixed > 0 {
		return f.writeIndexFixed(t, buf)
	}

	var totalSz int
	sz, err := f.WriteStringField(0, t.name, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	sz, err = f.WriteSizeField(0, FieldTypeVariable, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	return totalSz, err
}

func (f *rsfWriter) writeIndexFixed(t *tag, buf *bytes.Buffer) (int, error) {
	var totalSz int
	sz, err := f.WriteStringField(0, t.name, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	sz, err = f.WriteSizeField(0, FieldTypeFixed, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	return totalSz, err
}
