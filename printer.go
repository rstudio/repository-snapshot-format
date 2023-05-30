// Copyright (C) 2023 by Posit Software, PBC
package rsf

import (
	"bufio"
	"fmt"
	"io"
	"reflect"
	"strings"
)

func Print(w io.Writer, r *bufio.Reader, o any) error {
	// Create a new reader since we need to read the RSF data.
	reader := NewReader()

	// Read the RSF index. We'll use this data to help print the information.
	idx, err := reader.ReadIndex(r)
	if err != nil {
		return fmt.Errorf("error reading index: %s", err)
	}

	// Also extract information about array indexes from the object. This
	// information is not available in the index.
	sizes := make(map[string]int)
	kinds := make(map[string]reflect.Kind)
	err = indexArrays("", "", reflect.TypeOf(o), sizes, kinds)
	if err != nil {
		return fmt.Errorf("error indexing arrays: %s", err)
	}

	// Iterate the fields recursively and print the data.
	var i int
	for {
		i++

		// Read full object size
		_, err = reader.ReadSizeField(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		// Add blank newline unless at first object
		if i > 1 {
			_, err = fmt.Fprintln(w, "")
			if err != nil {
				return nil
			}
		}

		// Print object header
		pad := strings.Repeat(" ", 16)
		header := fmt.Sprintf("%s%s[%d]%s", pad, reflect.TypeOf(o).Name(), i, pad)
		line := strings.Repeat("-", len(header))
		_, err = fmt.Fprintf(w, "%s\n%s\n%s\n", line, header, line)
		if err != nil {
			return nil
		}

		// Print data for each field of the object.
		for _, f := range idx {
			err = printField(sizes, kinds, "", f, w, r, reader, 0)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("error printing data: %s", err)
			}
		}
	}
}

func indexArrays(parent, parentIndex string, v reflect.Type, sizes map[string]int, kinds map[string]reflect.Kind) error {
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			t := &tag{}
			_, err := getTagInfo(v, i, t, &tag{}, "")
			if err != nil {
				return err
			}
			key := t.name
			if parent != "" {
				key = strings.Join([]string{parent, t.name}, "...")
			}
			sub := v.Field(i)
			if parentIndex == t.name {
				if t.fixed > 0 {
					sizes[parent] = t.fixed
					kinds[parent] = reflect.String
				}
				// RSF also supports integer indexes, which should not have a `fixed:x` struct tag. Fall
				// through without an `else` to ensure that we correctly handle any odd cases where an
				// integer index has a `fixed` tag.
				switch sub.Type.Kind() {
				case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
					sizes[parent] = 10
					kinds[parent] = reflect.Int64
				}
			}
			if sub.Type.Kind() == reflect.Slice {
				if sub.Type.Elem().Kind() == reflect.Struct {
					err = indexArrays(key, t.index, sub.Type.Elem(), sizes, kinds)
					if err != nil {
						return err
					}
				} else {
					kinds[key] = sub.Type.Elem().Kind()
				}
			}
		}
	}
	return nil
}

func printField(sizes map[string]int, kinds map[string]reflect.Kind, parentKey string, f IndexEntry, w io.Writer, r *bufio.Reader, reader Reader, indent int) error {

	pad := strings.Repeat(" ", indent*4)
	switch f.FieldType {
	case FieldTypeBool:
		b, err := reader.ReadBoolField(r)
		if err != nil {
			return fmt.Errorf("error reading bool: %s", err)
		}
		_, err = fmt.Fprintf(w, "%s%s (bool): %t\n", pad, f.FieldName, b)
		if err != nil {
			return err
		}
	case FieldTypeInt64:
		i, err := reader.ReadIntField(r)
		if err != nil {
			return fmt.Errorf("error reading int: %s", err)
		}
		_, err = fmt.Fprintf(w, "%s%s (int): %d\n", pad, f.FieldName, i)
		if err != nil {
			return err
		}
	case FieldTypeFloat:
		fl, err := reader.ReadFloatField(r)
		if err != nil {
			return fmt.Errorf("error reading float: %s", err)
		}
		_, err = fmt.Fprintf(w, "%s%s (float): %f\n", pad, f.FieldName, fl)
		if err != nil {
			return err
		}
	case FieldTypeFixedStr:
		s, err := reader.ReadFixedStringField(f.FieldSize, r)
		if err != nil {
			return fmt.Errorf("error reading fixed-length string: %s", err)
		}
		_, err = fmt.Fprintf(w, "%s%s (string(%d)): %s\n", pad, f.FieldName, f.FieldSize, s)
		if err != nil {
			return err
		}
	case FieldTypeVarStr:
		s, err := reader.ReadStringField(r)
		if err != nil {
			return fmt.Errorf("error reading variable-length string field %s: %s", f.FieldName, err)
		}
		_, err = fmt.Fprintf(w, "%s%s (string): %s\n", pad, f.FieldName, s)
		if err != nil {
			return err
		}
	case FieldTypeArray:
		sz, err := reader.ReadSizeField(r)
		if err != nil {
			return fmt.Errorf("error reading array size: %s", err)
		}
		arrayLen, err := reader.ReadSizeField(r)
		if err != nil {
			return fmt.Errorf("error reading array length: %s", err)
		}

		key := f.FieldName
		if parentKey != "" {
			key = strings.Join([]string{parentKey, f.FieldName}, "...")
		}

		indexValues := make([]any, 0)

		// Record index values
		if indexSz, ok := sizes[key]; ok {
			for i := 0; i < arrayLen; i++ {
				switch kinds[key] {
				case reflect.String:
					var sIndexVal string
					sIndexVal, err = reader.ReadFixedStringField(indexSz, r)
					if err != nil {
						return fmt.Errorf("error reading index string value: %s", err)
					}
					indexValues = append(indexValues, sIndexVal)
				case reflect.Int64:
					var intIndexVal int64
					intIndexVal, err = reader.ReadIntField(r)
					if err != nil {
						return fmt.Errorf("error reading index int64 value: %s", err)
					}
					indexValues = append(indexValues, intIndexVal)
				}

				// Discard index size
				err = reader.Discard(4, r)
				if err != nil {
					return fmt.Errorf("error discarding index bytes: %s", err)
				}
			}
		}

		if len(indexValues) > 0 {
			_, err = fmt.Fprintf(w, "%s%s (indexed array(%d)):\n", pad, f.FieldName, arrayLen)
			if err != nil {
				return err
			}
		} else {
			_, err = fmt.Fprintf(w, "%s%s (array(%d)):\n", pad, f.FieldName, arrayLen)
			if err != nil {
				return err
			}
		}

	fields:
		for i := 0; i < arrayLen; i++ {
			if f.Subfields != nil {
				var indexVal string
				if len(indexValues) > 0 {
					switch t := indexValues[i].(type) {
					case string:
						indexVal = fmt.Sprintf(" %s", t)
					case int64:
						indexVal = fmt.Sprintf(" %d", t)
					}
				}
				_, err = fmt.Fprintf(w, "%s-%s\n", pad+strings.Repeat(" ", 4), indexVal)
				for _, subfield := range f.Subfields {
					err = printField(sizes, kinds, key, subfield, w, r, reader, indent+1)
					if err != nil {
						if err == io.EOF {
							return nil
						}
						return err
					}
				}
			} else {
				_, err = fmt.Fprintf(w, "%s-", pad+strings.Repeat(" ", 4))
				switch kinds[key] {
				case reflect.String:
					var s string
					s, err = reader.ReadStringField(r)
					if err != nil {
						return fmt.Errorf("error reading array string field: %s", err)
					}
					_, err = fmt.Fprintf(w, "%s\n", s)
					if err != nil {
						return err
					}
				case reflect.Bool:
					var b bool
					b, err = reader.ReadBoolField(r)
					if err != nil {
						return fmt.Errorf("error reading array bool field: %s", err)
					}
					_, err = fmt.Fprintf(w, "%t\n", b)
					if err != nil {
						return err
					}
				case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8:
					var d int64
					d, err = reader.ReadIntField(r)
					if err != nil {
						return fmt.Errorf("error reading array int field: %s", err)
					}
					_, err = fmt.Fprintf(w, "%d\n", d)
					if err != nil {
						return err
					}
				case reflect.Float32, reflect.Float64:
					var fl float64
					fl, err = reader.ReadFloatField(r)
					if err != nil {
						return fmt.Errorf("error reading array float field: %s", err)
					}
					_, err = fmt.Fprintf(w, "%f\n", fl)
					if err != nil {
						return err
					}
				default:
					_, err = fmt.Fprintf(w, " cannot print data for arrays of arrays\n")
					if err != nil {
						return err
					}
					err = reader.Discard(sz-8, r)
					if err != nil {
						return fmt.Errorf("error reading unknown array field data: %s", err)
					}
					break fields
				}
			}
		}
	default:
		return fmt.Errorf("cannot print unknown field %s with type %d", f.FieldName, f.FieldType)
	}
	return nil
}
