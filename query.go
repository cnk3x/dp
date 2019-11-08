package dp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type (
	dbExec interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	}

	dbQuery interface {
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	}

	dbScanner interface {
		Scan(dest ...interface{}) error
	}
)

//SelectRow SelectRowContext
func SelectRow(db *sql.DB, dest []interface{}, query string, args ...interface{}) error {
	return SelectRowContext(context.Background(), db, dest, query, args...)
}

//SelectRowContext SelectRowContext
func SelectRowContext(ctx context.Context, db *sql.DB, dest []interface{}, query string, args ...interface{}) error {
	err := db.QueryRowContext(ctx, query, args...).Scan(dest...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}

//InsertContext 插入或者更新
func InsertContext(ctx context.Context, db dbExec, args ...interface{}) (affected int64, err error) {
	defer func() {
		if e := recover(); e != nil {
			if _, ok := e.(error); ok {
				err = fmt.Errorf("%w", e)
			} else {
				err = fmt.Errorf("%#v", e)
			}
		}
	}()

	var (
		itemsMap  map[string][]interface{}
		tab       *Table
		result    sql.Result
		iAffected int64
	)

	itemsMap, err = groupValues(args...)
	if err != nil {
		return
	}

	for _, items := range itemsMap {
		if len(items) == 0 {
			continue
		}

		tab, err = GetTable(items)
		if err != nil {
			return
		}

		query := tab.CreateBatchInsertScript(len(items))
		values := make([]interface{}, 0, len(tab.insertColumns)*len(items))
		for _, item := range items {
			v := reflect.ValueOf(item)
			for _, c := range tab.Columns {
				if !c.AutoIncrement {
					val := v.FieldByIndex(c.Struct.Index)
					if isBlank(val) && c.Struct.Type.Kind() == reflect.Int64 {
						if c.Struct.Name == "CreatedAt" || c.Struct.Name == "UpdatedAt" {
							values = append(values, time.Now().Unix())
						} else if c.NewID || c.PrimaryKey {
							values = append(values, NewID())
						} else {
							values = append(values, 0)
						}
					} else {
						values = append(values, val.Interface())
					}
				}
			}
		}

		result, err = db.ExecContext(ctx, query, values...)
		if err != nil {
			return
		}
		iAffected, err = result.RowsAffected()
		if err != nil {
			return
		}
		affected += iAffected
	}

	return
}

//Insert 插入或者更新
func Insert(db dbExec, items ...interface{}) (affected int64, err error) {
	return InsertContext(context.Background(), db, items...)
}

//SelectContext Select
func SelectContext(ctx context.Context, db dbQuery, out interface{}, where string, args ...interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			if _, ok := e.(error); ok {
				err = fmt.Errorf("%w", e)
			} else {
				err = fmt.Errorf("%#v", e)
			}
		}
	}()

	v := reflect.ValueOf(out)

	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}

	if v.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	v = reflect.Indirect(v)

	tab, err := GetTable(out)
	if err != nil {
		return fmt.Errorf("dp select: %w", err)
	}

	var query string
	if where = strings.TrimSpace(where); where != "" {
		if !strings.HasPrefix(where, "where") {
			where = "where " + where
		}
		query = tab.SelectScript + " " + where
	} else {
		query = tab.SelectScript
	}

	return queryContext(ctx, db, out, tab, query, args...)
}

//Select Select
func Select(db dbQuery, out interface{}, where string, args ...interface{}) error {
	return SelectContext(context.Background(), db, out, where, args...)
}

//DeleteContext Delete
func DeleteContext(ctx context.Context, db dbExec, tableOrModel interface{}, where string, args ...interface{}) (int64, error) {
	table, err := getTableName(tableOrModel)
	if err != nil {
		return 0, fmt.Errorf("dp Delete: %w", err)
	}

	query := &strings.Builder{}
	fmt.Fprintf(query, "DELETE FROM `%s`", table)

	if where = strings.TrimSpace(where); where != "" {
		query.WriteRune(' ')
		if !strings.HasPrefix(strings.ToUpper(where), "WHERE") {
			query.WriteString("WHERE ")
		}
		query.WriteString(where)
	}

	result, err := db.ExecContext(ctx, query.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("dp Delete: %w", err)
	}
	return result.RowsAffected()
}

//Delete Delete
func Delete(db dbExec, table interface{}, where string, args ...interface{}) (int64, error) {
	return DeleteContext(context.Background(), db, table, where, args...)
}

//UpdateContext Update
func UpdateContext(ctx context.Context, db dbExec, tableOrModel interface{}, values map[string]interface{}, where string, args ...interface{}) (int64, error) {
	table, err := getTableName(tableOrModel)
	if err != nil {
		return 0, fmt.Errorf("dp UPDATE: %w", err)
	}
	query := &strings.Builder{}
	params := make([]interface{}, 0, len(values)+len(args))
	fmt.Fprintf(query, "UPDATE `%s` SET", table)
	start := true
	for name, value := range values {
		if start {
			start = false
		} else {
			query.WriteRune(',')
		}

		fmt.Fprintf(query, " `%s`=?", name)
		params = append(params, value)
	}

	if where = strings.TrimSpace(where); where != "" {
		query.WriteRune(' ')
		if !strings.HasPrefix(strings.ToUpper(where), "WHERE") {
			query.WriteString("WHERE ")
		}
		query.WriteString(where)
		params = append(params, args...)
	}

	result, err := db.ExecContext(ctx, query.String(), params...)
	if err != nil {
		return 0, fmt.Errorf("dp Update: %w", err)
	}
	return result.RowsAffected()
}

//Update Update
func Update(db dbExec, tableOrModel interface{}, values map[string]interface{}, where string, args ...interface{}) (int64, error) {
	return UpdateContext(context.Background(), db, tableOrModel, values, where, args...)
}

func getTableName(tableOrModel interface{}) (string, error) {
	if tabString, ok := tableOrModel.(string); ok {
		return tabString, nil
	}
	tab, err := GetTable(tableOrModel)
	if err != nil {
		return "", fmt.Errorf("dp delete: %w", err)
	}
	return tab.Name, nil
}

//Values 创建个Map
func Values(args ...interface{}) map[string]interface{} {
	l := len(args)
	values := make(map[string]interface{}, l/2)
	for i := 0; i < l-1; i += 2 {
		if name, ok := args[i].(string); ok {
			values[name] = args[i+1]
		}
	}
	return values
}

//QueryContext Query
func QueryContext(ctx context.Context, db dbQuery, out interface{}, query string, args ...interface{}) (err error) {
	return queryContext(ctx, db, out, nil, query, args...)
}

//Query Query
func Query(db dbQuery, out interface{}, query string, args ...interface{}) (err error) {
	return QueryContext(context.Background(), db, out, query, args...)
}

//QueryContext Query
func queryContext(ctx context.Context, db dbQuery, out interface{}, tab *Table, query string, args ...interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			if _, ok := e.(error); ok {
				err = fmt.Errorf("%w", e)
			} else {
				err = fmt.Errorf("%#v", e)
			}
		}
	}()

	v := reflect.ValueOf(out)

	if v.Kind() != reflect.Ptr {
		return errors.New("must pass a pointer, not a value, to StructScan destination")
	}

	if v.IsNil() {
		return errors.New("nil pointer passed to StructScan destination")
	}

	v = reflect.Indirect(v)

	if tab == nil {
		tab, err = GetTable(out)
		if err != nil {
			return fmt.Errorf("dp select: %w", err)
		}
	}

	outKind := v.Kind()
	itemKind := outKind
	if outKind == reflect.Slice {
		itemKind = v.Type().Elem().Kind()
	}

	if outKind != reflect.Slice {
		if !strings.Contains(query, "limit") {
			query += " limit 1"
		}
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return
		}
		return err
	}

	defer rows.Close()
	result := v
	for rows.Next() {
		itemV, err := tab.Scan(rows)
		if err != nil {
			return err
		}
		if outKind == reflect.Struct {
			v.Set(*itemV)
			return nil
		}
		if itemKind == reflect.Ptr {
			result = reflect.Append(result, (*itemV).Addr())
		} else {
			result = reflect.Append(result, *itemV)
		}
	}
	v.Set(result)
	return
}
