package dp

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

var tabCache = make(map[string]*Table)

var NewID = func() int64 {
	return 0
}

//Table Table
type Table struct {
	Name    string
	Type    *reflect.Type
	Struct  *reflect.StructField
	Columns []*Column

	allColumns     []string //所有字段
	insertColumns  []string //插入的字段
	replaceColumns []string //替换的字段

	SelectScript            string
	InsertScript            string
	BatchInsertScriptFormat string
}

// Column model field definition
type Column struct {
	Name          string
	PrimaryKey    bool
	AutoIncrement bool
	NewID         bool
	Struct        *reflect.StructField
	IsOnUpdate    bool
	TagMap        map[string]string
}

//GetTable 解析struct
func GetTable(obj interface{}) (*Table, error) {
	t := deref(reflect.TypeOf(obj))

	if t.Kind() == reflect.Slice {
		t = deref(t.Elem())
		if t.Kind() == reflect.Interface {
			v := reflect.Indirect(reflect.ValueOf(obj))
			if v.Len() == 0 {
				return nil, errors.New("need struct's pointer or no empty slice's pointer, not " + v.Kind().String())
			}
			return GetTable(v.Index(0).Interface())
		}
	}

	if t.Kind() != reflect.Struct {
		return nil, errors.New("need struct's pointer or slice, not " + t.Kind().String())
	}

	cacheKey := t.PkgPath() + "/" + t.Name()

	tab, find := tabCache[cacheKey]
	if find {
		return tab, nil
	}

	l := t.NumField()
	tab = &Table{Name: ParseName(t.Name()), Type: &t, Columns: make([]*Column, 0, l)}
	for i := 0; i < l; i++ {
		f := t.Field(i)

		tag := f.Tag.Get("db")
		if tag == "" {
			tag = f.Tag.Get("o")
		}
		if tag == "" {
			tag = f.Tag.Get("gorm")
		}
		if tag == "-" {
			continue
		}

		if f.Name == "table" {
			if tag != "" {
				tab.Name = tag
			}
			continue
		}

		column := Column{Struct: &f}
		tags := strings.Split(tag, ";")
		column.TagMap = make(map[string]string, len(tags))

		for _, s := range tags {
			if idx := strings.Index(s, ":"); idx != -1 {
				column.TagMap[strings.TrimSpace(s[:idx])] = s[idx+1:]
			} else {
				column.TagMap[s] = "true"
			}
		}

		_, column.PrimaryKey = column.TagMap["primary_key"]
		_, column.AutoIncrement = column.TagMap["auto_increment"]
		_, column.NewID = column.TagMap["newid"]
		_, column.IsOnUpdate = column.TagMap["on_update"]
		column.Name, _ = column.TagMap["column"]

		if column.Name == "" {
			column.Name = ParseName(f.Name)
		}

		quotedName := "`" + column.Name + "`"
		tab.allColumns = append(tab.allColumns, quotedName)
		if !column.AutoIncrement {
			tab.insertColumns = append(tab.insertColumns, quotedName)
		}
		if column.IsOnUpdate {
			tab.replaceColumns = append(tab.replaceColumns, quotedName+"=VALUES("+quotedName+")")
		}

		tab.Columns = append(tab.Columns, &column)
	}

	sort.Strings(tab.allColumns)
	sort.Strings(tab.insertColumns)
	sort.Strings(tab.replaceColumns)
	sort.Sort(tab)

	tab.SelectScript = "SELECT " + strings.Join(tab.allColumns, ",") + " FROM `" + tab.Name + "`"
	tab.BatchInsertScriptFormat = tab.getInsertQueryFormat()
	tab.InsertScript = fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", tab.Name, strings.Join(tab.insertColumns, ","), strings.TrimSuffix(strings.Repeat("?,", len(tab.insertColumns)), ","))

	return tab, nil
}

//CreateBatchInsertScript CreateBatchInsertScript
func (tab *Table) CreateBatchInsertScript(count int) string {
	w := &strings.Builder{}
	for i := 0; i < count; i++ {
		if i > 0 {
			w.WriteRune(',')
		}
		w.WriteRune('(')
		for j := 0; j < len(tab.insertColumns); j++ {
			if j > 0 {
				w.WriteRune(',')
			}
			w.WriteRune('?')
		}
		w.WriteRune(')')
	}

	return fmt.Sprintf(tab.BatchInsertScriptFormat, w.String())
}

func (tab *Table) getInsertQueryFormat() string {
	w := &strings.Builder{}
	if len(tab.replaceColumns) > 0 {
		w.WriteString("INSERT INTO ")
	} else {
		w.WriteString("INSERT IGNORE INTO ")
	}
	w.WriteRune('`')
	w.WriteString(tab.Name)
	w.WriteRune('`')

	w.WriteString(" (")
	for i, c := range tab.insertColumns {
		if i > 0 {
			w.WriteRune(',')
		}
		w.WriteString(c)
	}
	w.WriteRune(')')
	w.WriteString(" VALUES %s")
	if len(tab.replaceColumns) > 0 {
		w.WriteString("ON DUPLICATE KEY UPDATE ")
		for k, c := range tab.replaceColumns {
			if k > 0 {
				w.WriteRune(',')
			}
			w.WriteString(c)
		}
	}

	return w.String()
}

//Scan Scan
func (tab *Table) Scan(scanner dbScanner) (*reflect.Value, error) {
	item := reflect.New(*tab.Type).Elem()
	dest := make([]interface{}, len(tab.Columns))
	for i := range tab.Columns {
		dest[i] = item.FieldByIndex(tab.Columns[i].Struct.Index).Addr().Interface()
	}
	err := scanner.Scan(dest...)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

func (tab *Table) Len() int           { return len(tab.Columns) }
func (tab *Table) Less(i, j int) bool { return tab.Columns[i].Name < tab.Columns[j].Name }
func (tab *Table) Swap(i, j int)      { tab.Columns[i], tab.Columns[j] = tab.Columns[j], tab.Columns[i] }

//ParseName 名字
var ParseName = toUnderscore
