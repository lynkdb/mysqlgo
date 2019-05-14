// Copyright 2014 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysqlgo

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lynkdb/iomix/rdb"
	"github.com/lynkdb/iomix/rdb/modeler"
)

// "SET NAMES 'utf8'"
// "SET CHARACTER_SET_CLIENT=utf8"
// "SET CHARACTER_SET_RESULTS=utf8"

type DialectModeler struct {
	base rdb.Connector
}

func (dc *DialectModeler) IndexSync(tableName string, index *modeler.Index) error {

	action := ""
	switch index.Type {
	case modeler.IndexTypeIndex:
		action = "INDEX"
	case modeler.IndexTypeUnique:
		action = "UNIQUE"
	default:
		// PRIMARY KEY can be modified, can not be added
		return errors.New("Invalid Index Type")
	}

	sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD %s `%s` (%s)",
		dc.base.DBName(), tableName, action, index.Name,
		dc.QuoteStr(strings.Join(index.Cols, dc.QuoteStr(","))))

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) IndexDel(tableName string, index *modeler.Index) error {

	// PRIMARY KEY can be modified, can not be deleted
	if index.Type == modeler.IndexTypePrimaryKey {
		return nil
	}

	sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`",
		dc.base.DBName(), tableName, index.Name)

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) IndexSet(tableName string, index *modeler.Index) error {

	dropAction, addAction := "", ""

	switch index.Type {
	case modeler.IndexTypePrimaryKey:
		dropAction, addAction = "PRIMARY KEY", "PRIMARY KEY"
	case modeler.IndexTypeIndex:
		dropAction, addAction = "INDEX `"+index.Name+"`", "INDEX `"+index.Name+"`"
	case modeler.IndexTypeUnique:
		dropAction, addAction = "INDEX `"+index.Name+"`", "UNIQUE `"+index.Name+"`"
	default:
		return errors.New("Invalid Index Type")
	}

	sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP %s, ADD %s (%s)",
		dc.base.DBName(), tableName, dropAction, addAction,
		dc.QuoteStr(strings.Join(index.Cols, dc.QuoteStr(","))))
	//fmt.Println(sql)
	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) IndexDump(tableName string) ([]*modeler.Index, error) {

	indexes := []*modeler.Index{}

	s := "SELECT `INDEX_NAME`, `NON_UNIQUE`, `COLUMN_NAME` "
	s += "FROM `INFORMATION_SCHEMA`.`STATISTICS` "
	s += "WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	rows, err := dc.base.DB().Query(s, dc.base.DBName(), tableName)
	if err != nil {
		return indexes, err
	}
	defer rows.Close()

	for rows.Next() {

		var indexType int
		var indexName, colName, nonUnique string

		if err = rows.Scan(&indexName, &nonUnique, &colName); err != nil {
			return indexes, err
		}

		if indexName == "PRIMARY" {
			indexType = modeler.IndexTypePrimaryKey
		} else if "YES" == nonUnique || nonUnique == "1" {
			indexType = modeler.IndexTypeIndex
		} else {
			indexType = modeler.IndexTypeUnique
		}

		exist := false
		for i, v := range indexes {

			if v.Name == indexName {
				indexes[i].AddColumn(colName)
				exist = true
			}
		}

		if !exist {
			indexes = append(indexes, modeler.NewIndex(indexName, indexType).AddColumn(colName))
		}
	}

	return indexes, nil
}

func (dc *DialectModeler) ColumnTypeSql(table_name string, col *modeler.Column) string {

	sql, ok := dialectColumnTypes[col.Type]
	if !ok {
		return dc.QuoteStr(col.Name) + col.Type
		//, errors.New("Unsupported column type `" + col.Type + "`")
	}

	switch col.Type {
	case "string":
		sql = fmt.Sprintf(sql, col.Length)
	case "float64-decimal":
		lens := strings.Split(col.Length, ",")
		if lens[0] == "" {
			lens[0] = "10"
		}
		if len(lens) < 2 {
			lens = append(lens, "2")
		}
		sql = fmt.Sprintf(sql, lens[0], lens[1])
	}

	if col.NullAble {
		sql += " NULL"
	} else {
		sql += " NOT NULL"
	}

	if col.IncrAble {
		sql += " AUTO_INCREMENT"
	}

	if col.Default != "" {
		if col.Default == "null" {
			sql += " DEFAULT NULL"
		} else {
			sql += " DEFAULT '" + col.Default + "'"
		}
	}

	return dc.QuoteStr(col.Name) + " " + sql
}

func (dc *DialectModeler) ColumnSync(tableName string, col *modeler.Column) error {

	sql := fmt.Sprintf("ALTER TABLE `%v`.`%v` ADD %v",
		dc.base.DBName(), tableName, dc.ColumnTypeSql(tableName, col))

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) ColumnDel(tableName string, col *modeler.Column) error {

	sql := fmt.Sprintf("ALTER TABLE `%v`.`%v` DROP `%v`", dc.base.DBName(), tableName, col.Name)

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) ColumnSet(tableName string, col *modeler.Column) error {

	sql := fmt.Sprintf("ALTER TABLE `%v`.`%v` CHANGE `%v` %v",
		dc.base.DBName(), tableName, col.Name, dc.ColumnTypeSql(tableName, col))

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) ColumnDump(tableName string) ([]*modeler.Column, error) {

	cols := []*modeler.Column{}

	q := "SELECT `COLUMN_NAME`, `IS_NULLABLE`, `COLUMN_DEFAULT`, `COLUMN_TYPE`, `EXTRA` "
	q += "FROM `INFORMATION_SCHEMA`.`COLUMNS` "
	q += "WHERE `TABLE_SCHEMA` = ? AND `TABLE_NAME` = ?"

	rs, err := dc.base.QueryRaw(q, dc.base.DBName(), tableName)
	if err != nil {
		return cols, err
	}

	for _, entry := range rs {

		col := &modeler.Column{}

		for name, v := range entry.Fields {
			content := v.String()
			switch name {
			case "COLUMN_NAME":
				col.Name = strings.Trim(content, "` ")
			case "IS_NULLABLE":
				if "YES" == content {
					col.NullAble = true
				}
			case "COLUMN_DEFAULT":
				col.Default = content
			case "COLUMN_TYPE":

				cts := strings.Split(content, "(")
				var len1, len2 int

				if len(cts) == 2 {
					idx := strings.Index(cts[1], ")")
					lens := strings.Split(cts[1][0:idx], ",")
					len1, err = strconv.Atoi(strings.TrimSpace(lens[0]))
					if err != nil {
						//return nil, nil, err
						continue
					}
					if len(lens) == 2 {
						len2, err = strconv.Atoi(lens[1])
						if err != nil {
							//return nil, nil, err
							continue
						}
					}
				}

				col.Type = strings.ToLower(cts[0])
				if len1 > 0 {
					col.Length = fmt.Sprintf("%v", len1)
					if len2 > 0 {
						col.Length += fmt.Sprintf(",%v", len2)
					}
				}

				typepre := ""
				if strings.Contains(content, "unsigned") {
					typepre = "u"
				}
				switch col.Type {
				case "bigint":
					col.Type = typepre + "int64"
				case "int":
					col.Type = typepre + "int32"
				case "smallint":
					col.Type = typepre + "int16"
				case "tinyint":
					col.Type = typepre + "int8"
				case "varchar":
					col.Type = "string"
				case "longtext":
					col.Type = "string-text"
				}

			case "EXTRA":
				if content == "auto_increment" {
					col.IncrAble = true
				}
			}
		}

		cols = append(cols, col)
	}

	return cols, nil
}

func (dc *DialectModeler) TableSync(table *modeler.Table) error {

	if len(table.Columns) == 0 {
		return errors.New("No Columns Found")
	}

	sql := "CREATE TABLE IF NOT EXISTS " + dc.QuoteStr(table.Name) + " (\n"

	for _, col := range table.Columns {
		sql += " " + dc.ColumnTypeSql(table.Name, col) + ",\n"
	}

	pks := []string{}
	for _, idx := range table.Indexes {

		if len(idx.Cols) == 0 {
			continue
		}

		switch idx.Type {
		case modeler.IndexTypePrimaryKey:
			pks = idx.Cols
			continue
		case modeler.IndexTypeIndex:
			sql += " KEY "
		case modeler.IndexTypeUnique:
			sql += " UNIQUE KEY "
		default:
			continue
		}

		sql += dc.QuoteStr(idx.Name) + " ("
		sql += dc.QuoteStr(strings.Join(idx.Cols, dc.QuoteStr(",")))
		sql += "),\n"
	}

	if len(pks) == 0 {
		return errors.New("No PRIMARY KEY Found")
	}
	sql += " PRIMARY KEY ("
	sql += dc.QuoteStr(strings.Join(pks, dc.QuoteStr(",")))
	sql += ")\n"

	sql += ")"

	if table.Engine != "" {
		sql += " ENGINE=" + table.Engine
	} else if dc.base.Options().Value("engine") != "" {
		sql += " ENGINE=" + dc.base.Options().Value("engine")
	}

	if table.Charset != "" {
		sql += " DEFAULT CHARSET=" + table.Charset
	} else if dc.base.Options().Value("charset") != "" {
		sql += " DEFAULT CHARSET=" + dc.base.Options().Value("charset")
	} else {
		sql += " DEFAULT CHARSET=utf8"
	}

	if table.Comment != "" {
		sql += " COMMENT='" + table.Comment + "'"
	}

	sql += ";"

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) TableDump() ([]*modeler.Table, error) {

	tables := []*modeler.Table{}

	q := "SELECT `TABLE_NAME`, `ENGINE`, `TABLE_COLLATION`, `TABLE_COMMENT` "
	q += "FROM `INFORMATION_SCHEMA`.`TABLES` "
	q += "WHERE `TABLE_SCHEMA` = ?"

	rows, err := dc.base.DB().Query(q, dc.base.DBName())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {

		var name, engine, charset, comment string
		if err = rows.Scan(&name, &engine, &charset, &comment); err != nil {
			return nil, err
		}

		if i := strings.Index(charset, "_"); i > 0 {
			charset = charset[0:i]
		}

		idxs, _ := dc.IndexDump(name)

		cols, _ := dc.ColumnDump(name)

		tables = append(tables, &modeler.Table{
			Name:    name,
			Engine:  engine,
			Charset: charset,
			Columns: cols,
			Indexes: idxs,
			Comment: comment,
		})
	}

	return tables, nil
}

func (dc *DialectModeler) TableExist(tableName string) bool {

	q := "SELECT `TABLE_NAME` from `INFORMATION_SCHEMA`.`TABLES` "
	q += "WHERE `TABLE_SCHEMA` = ? and `TABLE_NAME` = ?"

	rows, err := dc.base.QueryRaw(q, dc.base.DBName(), tableName)
	if err != nil {
		return false
	}

	return len(rows) > 0
}

func (dc *DialectModeler) SchemaSync(newds *modeler.Schema) error {

	curds, err := dc.SchemaDump()
	if err != nil {
		return err
	}

	for _, newTable := range newds.Tables {

		exist := false
		var curTable *modeler.Table

		for _, curTable = range curds.Tables {

			if newTable.Name == curTable.Name {
				exist = true
				break
			}
		}

		if !exist {

			if err := dc.TableSync(newTable); err != nil {
				return err
			}

			continue
		}

		// Column
		for _, newcol := range newTable.Columns {

			colExist := false
			colChange := false

			for _, curcol := range curTable.Columns {

				if newcol.Name != curcol.Name {
					continue
				}

				colExist = true

				if newcol.Type != curcol.Type ||
					newcol.Length != curcol.Length ||
					newcol.NullAble != curcol.NullAble ||
					newcol.IncrAble != curcol.IncrAble ||
					newcol.Default != curcol.Default {
					colChange = true
					break
				}
			}

			if !colExist {

				if err := dc.ColumnSync(newTable.Name, newcol); err != nil {
					return err
				}
			}

			if colChange {

				if err := dc.ColumnSet(newTable.Name, newcol); err != nil {
					return err
				}
			}
		}

		// Delete Unused Indexes
		for _, curidx := range curTable.Indexes {

			curExist := false

			for _, newidx := range newTable.Indexes {

				if newidx.Name == curidx.Name {
					curExist = true
					break
				}
			}

			if !curExist {
				//fmt.Println("index del", curidx.Name)
				if err := dc.IndexDel(newTable.Name, curidx); err != nil {
					return err
				}
			}
		}

		// Delete Unused Columns
		for _, curcol := range curTable.Columns {

			colExist := false

			for _, newcol := range newTable.Columns {

				if newcol.Name == curcol.Name {
					colExist = true
					break
				}
			}

			if !colExist {
				if err := dc.ColumnDel(newTable.Name, curcol); err != nil {
					return err
				}
			}
		}

		// Add New, or Update Changed Indexes
		for _, newidx := range newTable.Indexes {

			newIdxExist := false
			newIdxChange := false

			for _, curidx := range curTable.Indexes {

				if newidx.Name != curidx.Name {
					continue
				}

				newIdxExist = true

				sort.Strings(newidx.Cols)
				sort.Strings(curidx.Cols)

				if newidx.Type != curidx.Type ||
					strings.Join(newidx.Cols, ",") != strings.Join(curidx.Cols, ",") {

					newIdxChange = true
				}

				break
			}

			if newIdxChange {
				//fmt.Println("index set", newidx.Name)
				if err := dc.IndexSet(newTable.Name, newidx); err != nil {
					return err
				}

			} else if !newIdxExist {
				//fmt.Println("index add", newidx.Name)
				if err := dc.IndexSync(newTable.Name, newidx); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (dc *DialectModeler) SchemaSyncByJson(js string) error {
	ds, err := modeler.NewSchemaByJson(js)
	if err != nil {
		return err
	}
	return dc.SchemaSync(ds)
}

func (dc *DialectModeler) SchemaSyncByJsonFile(js_path string) error {
	ds, err := modeler.NewSchemaByJsonFile(js_path)
	if err != nil {
		return err
	}
	return dc.SchemaSync(ds)
}

func (dc *DialectModeler) SchemaDump() (*modeler.Schema, error) {

	ds := &modeler.Schema{}

	q := "SELECT `DEFAULT_CHARACTER_SET_NAME` "
	q += "FROM `INFORMATION_SCHEMA`.`SCHEMATA` "
	q += "WHERE `SCHEMA_NAME` = ?"

	err := dc.base.DB().QueryRow(q, dc.base.DBName()).Scan(&ds.Charset)
	if err != nil {
		return ds, err
	}

	ds.Tables, err = dc.TableDump()

	return ds, err
}

func (dc *DialectModeler) QuoteStr(str string) string {
	return dialectQuote + str + dialectQuote
}
