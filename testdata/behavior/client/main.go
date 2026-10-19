// behavior-client runs only inside a newly owned, network-isolated container.
// It deliberately has no configurable network endpoint or credentials.
package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	mssql "github.com/microsoft/go-mssqldb"
)

type parameter struct {
	Name  string `json:"name"`
	Kind  string `json:"kind"`
	Value any    `json:"value"`
}
type step struct {
	SQL    string      `json:"sql"`
	Params []parameter `json:"params"`
}
type request struct {
	Engine   string `json:"engine"`
	Admin    bool   `json:"admin"`
	Setup    bool   `json:"setup"`
	Settings string `json:"settings"`
	Steps    []step `json:"steps"`
}

func failure(err error) map[string]any {
	r := map[string]any{"ok": false, "error": err.Error()}
	var ms mssql.Error
	var pg *pgconn.PgError
	if errors.As(err, &ms) {
		r["number"] = ms.Number
	}
	if errors.As(err, &pg) {
		r["sqlstate"] = pg.Code
	}
	return r
}

func arguments(s step, engine string) ([]any, error) {
	args := make([]any, len(s.Params))
	for i, p := range s.Params {
		v := p.Value
		switch p.Kind {
		case "int":
			n, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("invalid integer parameter")
			}
			v = int64(n)
		case "bytes":
			text, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("invalid bytes parameter")
			}
			b, err := hex.DecodeString(text)
			if err != nil {
				return nil, err
			}
			v = b
		case "string", "null":
		default:
			return nil, fmt.Errorf("unsupported parameter kind %q", p.Kind)
		}
		if engine == "mssql" {
			args[i] = sql.Named(p.Name, v)
		} else {
			args[i] = v
		}
	}
	return args, nil
}

func query() map[string]any {
	var input request
	if err := json.NewDecoder(io.LimitReader(os.Stdin, 4<<20)).Decode(&input); err != nil {
		return failure(err)
	}
	if len(input.Steps) == 0 {
		return failure(fmt.Errorf("steps required"))
	}
	var driver, dsn string
	switch input.Engine {
	case "mssql":
		driver = "sqlserver"
		u := &url.URL{Scheme: "sqlserver", Host: "127.0.0.1:1433", User: url.UserPassword("sa", os.Getenv("MSSQL_SA_PASSWORD"))}
		q := u.Query()
		q.Set("database", "smt_behavior")
		if input.Admin {
			q.Set("database", "master")
		}
		q.Set("encrypt", "disable")
		q.Set("connection timeout", "5")
		u.RawQuery = q.Encode()
		dsn = u.String()
	case "postgres":
		driver = "pgx"
		dsn = "postgres://postgres@/smt_behavior?host=/var/run/postgresql&sslmode=disable"
		if input.Admin {
			dsn = "postgres://postgres@/postgres?host=/var/run/postgresql&sslmode=disable"
		}
	default:
		return failure(fmt.Errorf("unsupported engine"))
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return failure(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return failure(err)
	}
	defer conn.Close()
	if input.Settings != "" {
		if _, err = conn.ExecContext(ctx, input.Settings); err != nil {
			return failure(err)
		}
	}
	if input.Setup {
		for _, s := range input.Steps {
			if _, err = conn.ExecContext(ctx, s.SQL); err != nil {
				return failure(err)
			}
		}
		return map[string]any{"ok": true, "rows": []any{}}
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return failure(err)
	}
	defer tx.Rollback()
	for _, s := range input.Steps[:len(input.Steps)-1] {
		args, err := arguments(s, input.Engine)
		if err != nil {
			return failure(err)
		}
		if _, err = tx.ExecContext(ctx, s.SQL, args...); err != nil {
			return failure(err)
		}
	}
	s := input.Steps[len(input.Steps)-1]
	args, err := arguments(s, input.Engine)
	if err != nil {
		return failure(err)
	}
	rows, err := tx.QueryContext(ctx, s.SQL, args...)
	if err != nil {
		return failure(err)
	}
	defer rows.Close()
	output := []map[string]any{}
	for {
		cols, err := rows.Columns()
		if err != nil {
			return failure(err)
		}
		for rows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return failure(err)
			}
			r := map[string]any{}
			for i, n := range cols {
				if b, ok := values[i].([]byte); ok {
					r[n] = string(b)
				} else {
					r[n] = values[i]
				}
			}
			output = append(output, r)
		}
		if err := rows.Err(); err != nil {
			return failure(err)
		}
		if !rows.NextResultSet() {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return failure(err)
	}
	return map[string]any{"ok": true, "rows": output}
}

func main() {
	if err := json.NewEncoder(os.Stdout).Encode(query()); err != nil {
		os.Exit(1)
	}
}
