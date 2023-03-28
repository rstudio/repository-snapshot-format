// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bytes"
	"fmt"
	"reflect"
)

/*

When writing a struct at position zero, we first write an index that describes the fields
in the object.

sz       (header size)
company  FieldTypeVariable
ready    FieldTypeFixed
list     FieldTypeArray
name     FieldTypeVariable
verified FieldTypeFixed

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
		return f.writeIndexBool(t, buf)
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
	var totalSz int
	sz, err := f.WriteStringField(0, t.name, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	fType := FieldTypeVariable
	if t.fixed > 0 {
		fType = FieldTypeFixed
	}

	sz, err = f.WriteSizeField(0, fType, buf)
	if err != nil {
		return 0, err
	}
	totalSz += sz

	return totalSz, err
}

func (f *rsfWriter) writeIndexBool(t *tag, buf *bytes.Buffer) (int, error) {
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
