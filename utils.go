package dp

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

//ParseName 名字
func toUnderscore(name string) string {
	// name = holdRe.ReplaceAllStringFunc(name, func(s string) string { return s[:1] + strings.ToLower(s[1:]) })
	name = replacers.Replace(name)
	name = upperRe.ReplaceAllString(name, "_$1")
	name = alnumRe.ReplaceAllString(name, "_")
	name = strings.ToLower(strings.Trim(name, "_"))
	return name
}

var (
	alnumRe = regexp.MustCompile(`([^[:alnum:]]+)`)
	upperRe = regexp.MustCompile(`([[:upper:]]+)`)
	// Copied from golint
	commonInitialisms = []string{"API", "ASCII", "ASIN", "CPU", "CSS", "DNS", "EOF", "GUID", "HTML", "HTTP", "HTTPS", "ID", "IP", "ISBN", "JSON", "LHS", "QPS", "RAM", "RHS", "RPC", "SKU", "SLA", "SMTP", "SSH", "TLS", "TTL", "UID", "UI", "UPC", "UUID", "URI", "URL", "UTF8", "VM", "XML", "XSRF", "XSS"}
	replacers         *strings.Replacer
)

func init() {
	var replacer []string
	for _, initialism := range commonInitialisms {
		replacer = append(replacer, initialism, strings.Title(strings.ToLower(initialism)))
	}
	replacers = strings.NewReplacer(replacer...)
}

//GroupValues 分组传入的资源
func groupValues(values ...interface{}) (map[string][]interface{}, error) {
	var objMap = make(map[string][]interface{})
	return objMap, group(objMap, reflect.ValueOf(values))
}

func group(m map[string][]interface{}, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Interface, reflect.Ptr:
		return group(m, v.Elem())
	case reflect.Struct:
		name := fmt.Sprintf("%s/%s", v.Type().PkgPath(), v.Type().Name())
		m[name] = append(m[name], v.Interface())
		return nil
	case reflect.Slice:
		if l := v.Len(); l > 0 {
			for i := 0; i < l; i++ {
				if err := group(m, v.Index(i)); err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("kind: %s not support", v.Kind().String())
	}
}

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
	t = deref(t)
	if t.Kind() != expected {
		return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
	}
	return t, nil
}

// deref is Indirect for reflect.Types
func deref(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func isBlank(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.String:
		return value.Len() == 0
	case reflect.Bool:
		return !value.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return value.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return value.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return value.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return value.IsNil()
	}
	return reflect.DeepEqual(value.Interface(), reflect.Zero(value.Type()).Interface())
}

// func equalAsString(a interface{}, b interface{}) bool {
// 	return toString(a) == toString(b)
// }

// func toString(str interface{}) string {
// 	if values, ok := str.([]interface{}); ok {
// 		var results []string
// 		for _, value := range values {
// 			results = append(results, toString(value))
// 		}
// 		return strings.Join(results, "_")
// 	} else if bytes, ok := str.([]byte); ok {
// 		return string(bytes)
// 	} else if reflectValue := reflect.Indirect(reflect.ValueOf(str)); reflectValue.IsValid() {
// 		return fmt.Sprintf("%v", reflectValue.Interface())
// 	}
// 	return ""
// }

// func makeSlice(elemType reflect.Type) interface{} {
// 	if elemType.Kind() == reflect.Slice {
// 		elemType = elemType.Elem()
// 	}
// 	sliceType := reflect.SliceOf(elemType)
// 	slice := reflect.New(sliceType)
// 	slice.Elem().Set(reflect.MakeSlice(sliceType, 0, 0))
// 	return slice.Interface()
// }

// func strInSlice(a string, list []string) bool {
// 	for _, b := range list {
// 		if b == a {
// 			return true
// 		}
// 	}
// 	return false
// }
