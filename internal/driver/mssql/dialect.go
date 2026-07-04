package mssql

import (
	"fmt"
	"net/url"
	"strings"
)

// Dialect implements driver.Dialect for SQL Server.
type Dialect struct{}

func (d *Dialect) DBType() string { return "mssql" }

func (d *Dialect) QuoteIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

func (d *Dialect) QualifyTable(schema, table string) string {
	return d.QuoteIdentifier(schema) + "." + d.QuoteIdentifier(table)
}

func (d *Dialect) BuildDSN(host string, port int, database, user, password string, opts map[string]any) string {
	dsn := fmt.Sprintf("sqlserver://%s@%s:%d?database=%s",
		url.UserPassword(user, password).String(), host, port, url.QueryEscape(database))

	// Add optional parameters
	if encrypt, ok := opts["encrypt"].(bool); ok {
		if encrypt {
			dsn += "&encrypt=true"
		} else {
			dsn += "&encrypt=disable"
		}
	}
	if trustCert, ok := opts["trustServerCertificate"].(bool); ok && trustCert {
		dsn += "&TrustServerCertificate=true"
	}
	if packetSize, ok := opts["packetSize"].(int); ok && packetSize > 0 {
		// Note: "packet size" is the go-mssqldb parameter name; + is URL encoding for space
		dsn += fmt.Sprintf("&packet%%20size=%d", packetSize)
	}

	// Set connection timeout to prevent indefinite hangs during login/connect.
	// go-mssqldb parameter: "connection timeout" in seconds.
	if _, ok := opts["connection timeout"]; !ok {
		dsn += "&connection+timeout=30"
	}

	// Set dial timeout for TCP connection establishment.
	if _, ok := opts["dial timeout"]; !ok {
		dsn += "&dial+timeout=15"
	}

	return dsn
}

func (d *Dialect) ParameterPlaceholder(index int) string {
	return fmt.Sprintf("@p%d", index)
}
