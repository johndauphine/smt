package postgres

import (
	"fmt"
	"net/url"
	"strings"
)

// Dialect implements driver.Dialect for PostgreSQL.
type Dialect struct{}

func (d *Dialect) DBType() string { return "postgres" }

func (d *Dialect) QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (d *Dialect) QualifyTable(schema, table string) string {
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	dsn := fmt.Sprintf("postgres://%s@%s:%d/%s",
		url.UserPassword(user, password).String(), host, port, url.PathEscape(database))

	params := url.Values{}
	if sslMode, ok := opts["sslmode"].(string); ok && sslMode != "" {
		params.Set("sslmode", sslMode)
	} else {
		params.Set("sslmode", "prefer")
	}

	if len(params) > 0 {
		dsn += "?" + params.Encode()
	}
	return dsn
}

func (d *Dialect) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("$%d", index)
}
