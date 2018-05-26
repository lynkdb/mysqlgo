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
	"github.com/lynkdb/iomix/rdb"
	"github.com/lynkdb/iomix/rdb/modeler"
)

const (
	dialect_quote = "`"
)

var dialect_column_types = map[string]string{
	"bool":            "bool",
	"string":          "varchar(%v)",
	"string-text":     "longtext",
	"date":            "date",
	"datetime":        "datetime",
	"int8":            "tinyint",
	"int16":           "smallint",
	"int32":           "integer",
	"int64":           "bigint",
	"uint8":           "tinyint unsigned",
	"uint16":          "smallint unsigned",
	"uint32":          "integer unsigned",
	"uint64":          "bigint unsigned",
	"float64":         "double precision",
	"float64-decimal": "numeric(%v, %v)",
}

var dialect_stmts = map[string]string{
	"insertIgnore": "INSERT IGNORE INTO %s (%s) VALUES (%s)",
}

func dialect_quote_str(name string) string {
	if name == "*" {
		return name
	}
	return dialect_quote + name + dialect_quote
}

type Dialect struct {
	rdb.Base
}

func (dc *Dialect) Modeler() (modeler.Modeler, error) {
	return &DialectModeler{
		base: dc,
	}, nil
}

func (dc *Dialect) QuoteStr(str string) string {
	return dialect_quote + str + dialect_quote
}

func (dc *Dialect) NewFilter() rdb.Filter {
	return NewFilter()
}

func (dc *Dialect) NewQueryer() rdb.Queryer {
	return NewQueryer()
}
