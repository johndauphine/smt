package mysql

import (
	"fmt"
	"net/url"
	"strings"

	gomysql "github.com/go-sql-driver/mysql"
)

// Dialect implements driver.Dialect for MySQL/MariaDB.
type Dialect struct{}

func (d *Dialect) DBType() string { return "mysql" }

func (d *Dialect) QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (d *Dialect) QualifyTable(schema, table string) string {
	// MySQL uses database.table, but schema is often empty (database is in DSN)
	if schema == "" {
		return d.QuoteIdentifier(table)
	}
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	params := url.Values{}
	params.Set("parseTime", "true")
	params.Set("multiStatements", "true")
	params.Set("interpolateParams", "true")

	// Handle SSL/TLS mode
	if sslMode, ok := opts["ssl_mode"].(string); ok && sslMode != "" {
		switch strings.ToLower(sslMode) {
		case "disable", "disabled", "false":
			params.Set("tls", "false")
		case "require", "required", "true":
			params.Set("tls", "true")
		case "verify-ca", "verify_ca":
			params.Set("tls", "true")
		case "verify-full", "verify_full", "verify-identity", "verify_identity":
			params.Set("tls", "true")
		default:
			params.Set("tls", "preferred")
		}
	} else {
		params.Set("tls", "preferred")
	}

	// Handle charset
	if charset, ok := opts["charset"].(string); ok && charset != "" {
		params.Set("charset", charset)
	} else {
		params.Set("charset", "utf8mb4")
	}

	// Handle collation
	if collation, ok := opts["collation"].(string); ok && collation != "" {
		params.Set("collation", collation)
	}

	// Handle timezone
	if loc, ok := opts["loc"].(string); ok && loc != "" {
		params.Set("loc", loc)
	} else {
		params.Set("loc", "UTC")
	}

	// Set read/write timeouts to prevent indefinite hangs on large batch inserts.
	// These are go-sql-driver/mysql DSN parameters that set deadlines on the
	// underlying TCP connection. 5 minutes is generous for bulk operations.
	if _, ok := opts["writeTimeout"]; !ok {
		params.Set("writeTimeout", "5m")
	}
	if _, ok := opts["readTimeout"]; !ok {
		params.Set("readTimeout", "5m")
	}

	cfg := gomysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", host, port)
	cfg.DBName = database
	cfg.Params = map[string]string{}
	for key, vals := range params {
		if len(vals) > 0 {
			cfg.Params[key] = vals[len(vals)-1]
		}
	}
	return cfg.FormatDSN()
}

func (d *Dialect) ParameterPlaceholder(_ int) string {
	return "?"
}
