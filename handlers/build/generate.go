package build

import (
	"bytes"
	"fmt"
	"github.com/iancoleman/strcase"
	"go/format"
	"strings"
	"text/template"
)

const (
	tmplImportSpanner = `"cloud.google.com/go/spanner"`
	tmplImportCivil   = `"cloud.google.com/go/civil"`
	tmplImportBig     = `"math/big"`
	tmplImportTime    = `"time"`
	tmpl              = `package {{ .PackageName }}
{{ .Import }}
type {{ .StructName }} struct {
{{ range $field := .Fields }}	{{ $field }}
{{ end }}}
`
)

type tmplValues struct {
	PackageName string
	Import      string
	StructName  string
	Fields      []string
}

func generate(pkgName string, tableNameColumns map[string][]column) (map[string][]byte, error) {
	res := map[string][]byte{}
	for tableName, columns := range tableNameColumns {
		b, err := buildCode(buildTmplValues(pkgName, tableName, columns))
		if err != nil {
			return nil, err
		}
		res[tableName] = b
	}
	return res, nil
}

func buildTmplValues(pkgName, tableName string, columns []column) tmplValues {
	var fields []string
	var containsNullable, containsDate, containsBig, containsTimestamp bool
	for _, c := range columns {
		if c.nullable {
			if c.tp == tpString || c.tp == tpInt64 || c.tp == tpFloat64 || c.tp == tpNumeric || c.tp == tpBool || c.tp == tpDate || c.tp == tpTimestamp {
				containsNullable = true
			}
		}
		if c.tp == tpDate || c.tp == tpArrayDate {
			containsDate = true
		} else if c.tp == tpNumeric || c.tp == tpArrayNumeric {
			containsBig = true
		} else if c.tp == tpTimestamp || c.tp == tpArrayTimestamp {
			containsTimestamp = true
		}
		fields = append(fields, fmt.Sprintf("%s %s `%s`", strcase.ToCamel(c.name), buildType(c), buildFieldName(c)+buildPk(c)))
	}
	return tmplValues{
		PackageName: pkgName,
		StructName:  strcase.ToCamel(tableName),
		Import:      buildImport(containsNullable, containsDate, containsBig, containsTimestamp),
		Fields:      fields,
	}
}

func buildType(c column) string {
	switch c.tp {
	case tpString:
		if c.nullable {
			return "spanner.NullString"
		}
		return "string"
	case tpBytes:
		return "[]byte"
	case tpInt64:
		if c.nullable {
			return "spanner.NullInt64"
		}
		return "int64"
	case tpFloat64:
		if c.nullable {
			return "spanner.NullFloat64"
		}
		return "float64"
	case tpNumeric:
		if c.nullable {
			return "spanner.NullNumeric"
		}
		return "big.Rat"
	case tpBool:
		if c.nullable {
			return "spanner.NullBool"
		}
		return "bool"
	case tpDate:
		if c.nullable {
			return "spanner.NullDate"
		}
		return "civil.Date"
	case tpTimestamp:
		if c.nullable {
			return "spanner.NullTime"
		}
		return "time.Time"
	case rpArrayString:
		return "[]string"
	case tpArrayBytes:
		return "[][]byte"
	case tpArrayInt64:
		return "[]int64"
	case tpArrayFloat64:
		return "[]float64"
	case tpArrayNumeric:
		return "[]big.Rat"
	case tpArrayBool:
		return "[]bool"
	case tpArrayDate:
		return "[]civil.Date"
	case tpArrayTimestamp:
		return "[]time.Time"
	}
	return "undefinedType"
}

func buildFieldName(c column) string {
	return fmt.Sprintf(`spanner:"%s"`, c.name)
}

func buildPk(c column) string {
	if !c.isPk {
		return ""
	}
	return fmt.Sprintf(` pk:"%d"`, c.pkOrder)
}

func buildImport(containsNullable, containsDate, containsBig, containsTimestamp bool) string {
	var imports []string
	if containsNullable {
		imports = append(imports, tmplImportSpanner)
	}
	if containsDate {
		imports = append(imports, tmplImportCivil)
	}
	if containsBig {
		imports = append(imports, tmplImportBig)
	}
	if containsTimestamp {
		imports = append(imports, tmplImportTime)
	}

	if len(imports) == 0 {
		return ""
	}
	if len(imports) == 1 {
		return "import " + imports[0]
	}
	return fmt.Sprintf(`import (%s)`, strings.Join(imports, "\n"))
}

func buildCode(v tmplValues) ([]byte, error) {
	var buf bytes.Buffer
	tmpl := template.Must(template.New("").Parse(tmpl))
	if err := tmpl.Execute(&buf, v); err != nil {
		return nil, err
	}
	return format.Source(buf.Bytes())
}
