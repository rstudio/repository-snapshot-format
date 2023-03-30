// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
)

/*

When writing a struct at position zero, we first write an index that
describes the struct fields in the object.

Format:

  [header size]
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
  0x1, 0x0, 0x0, 0x0,                             // FieldTypeVarStr

  0x7, 0x0, 0x0, 0x0,                             // 7 bytes
  0x63, 0x6f, 0x6d, 0x70, 0x61, 0x6e, 0x79,       // "myfloat"
  0x6, 0x0, 0x0, 0x0,                             // FieldTypeFloat

  0x5, 0x0, 0x0, 0x0,                             // 5 bytes
  0x72, 0x65, 0x61, 0x64, 0x79,                   // "ready"
  0x2, 0x0, 0x0, 0x0,                             // FieldTypeFixedStr
  0x8, 0x0, 0x0, 0x0                              // 8 in size

  0x4, 0x0, 0x0, 0x0,                             // 4 bytes
  0x6c, 0x69, 0x73, 0x74,                         // "list"
  0x3, 0x0, 0x0, 0x0,                             // FieldTypeArray
  0x2, 0x0, 0x0, 0x0,                             // Array length is 2

  // ... write all array struct fields ....

  0x4, 0x0, 0x0, 0x0,                             // 4 bytes
  0x6e, 0x61, 0x6d, 0x65,                         // "name"
  0x1, 0x0, 0x0, 0x0,                             // FieldTypeVarStr

  0x8, 0x0, 0x0, 0x0,                             // 8 bytes
  0x76, 0x65, 0x72, 0x69, 0x66, 0x69, 0x65, 0x64, // "verified"
  0x2, 0x0, 0x0, 0x0,                             // FieldTypeFixedStr
  0x8, 0x0, 0x0, 0x0                              // 8 in size

*/

const (
	FieldTypeVarStr   = 1
	FieldTypeFixedStr = 2
	FieldTypeBool     = 3
	FieldTypeArray    = 4
	FieldTypeFloat    = 6
	FieldTypeInt64    = 7
)

func (f *rsfWriter) writeIndexObject(v reflect.Type, t *tag, buf *bytes.Buffer) (int, error) {
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		return f.writeIndexArray(v, t, buf)
	case reflect.Struct:
		sz, _, err := f.writeIndexStruct(v, t, buf)
		return sz, err
	case reflect.String:
		return f.writeIndexString(t, buf)
	case reflect.Bool:
		return f.writeIndexFixed(t, FieldTypeBool, buf)
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
		return f.writeIndexFixed(t, FieldTypeInt64, buf)
	case reflect.Float32, reflect.Float64:
		return f.writeIndexFixed(t, FieldTypeFloat, buf)
	default:
		return 0, fmt.Errorf("unknown field type %#v: %#v", v.Kind(), v)
	}
}

func (f *rsfWriter) writeIndexStruct(v reflect.Type, tParent *tag, buf *bytes.Buffer) (int, int, error) {
	var totalSz int
	var count int
	for i := 0; i < v.NumField(); i++ {
		t := &tag{}
		skip, err := getTagInfo(v, i, t, tParent, "")
		if err != nil {
			return 0, 0, err
		}

		if !skip {
			var sz int
			sz, err = f.writeIndexObject(v.Field(i).Type, t, buf)
			if err != nil {
				return 0, 0, err
			}
			totalSz += sz
			count++
		}
	}

	return totalSz, count, nil
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

	// For struct arrays, we may need to write additional info about the struct
	el := v.Elem()
	var subfields int
	subfieldsBuf := &bytes.Buffer{}
	if el.Kind() == reflect.Struct {
		// Write the subfields into a buffer and record the number of subfields found.
		_, subfields, err = f.writeIndexStruct(el, t, subfieldsBuf)
		if err != nil {
			return 0, err
		}
	}

	// Record the number of subfields in the array
	sz, err = f.WriteSizeField(0, subfields, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	// For if subfields were found, copy the subfield buffer.
	if subfields > 0 {
		var szCopy int64
		szCopy, err = io.Copy(buf, subfieldsBuf)
		if err != nil {
			return 0, err
		}
		totalSz += int(szCopy)
	}

	return totalSz, err
}

func (f *rsfWriter) writeIndexString(t *tag, buf *bytes.Buffer) (int, error) {
	if t.fixed > 0 {
		sz, err := f.writeIndexFixed(t, FieldTypeFixedStr, buf)
		if err != nil {
			return 0, err
		}

		sizeSz, err := f.WriteSizeField(0, t.fixed, buf)
		return sz + sizeSz, err
	}

	var totalSz int
	sz, err := f.WriteStringField(0, t.name, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	sz, err = f.WriteSizeField(0, FieldTypeVarStr, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	return totalSz, err
}

func (f *rsfWriter) writeIndexFixed(t *tag, fieldType int, buf *bytes.Buffer) (int, error) {
	var totalSz int
	sz, err := f.WriteStringField(0, t.name, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	sz, err = f.WriteSizeField(0, fieldType, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	return totalSz, err
}
