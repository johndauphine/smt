package mssql

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"smt/internal/driver"
)

// TestReaderDatabaseContext_Populated is the mssql side of the issue #13
// regression guard. Mirrors the postgres/mysql tests.
func TestReaderDatabaseContext_Populated(t *testing.T) {
	body := readReaderSource(t)
	for _, needle := range []string{
		"func (r *Reader) DatabaseContext()",
		"dbContextOnce.Do",
		"gatherDatabaseContext(",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("reader.go missing required marker %q", needle)
		}
	}
}

// readReaderSource returns the contents of reader.go as a string. Uses
// runtime.Caller to locate the file by absolute path so the test doesn't
// depend on the working directory.
func readReaderSource(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed; cannot locate reader.go")
	}
	src, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "reader.go"))
	if err != nil {
		t.Fatalf("read reader.go: %v", err)
	}
	return string(src)
}

func TestDriverRegistration(t *testing.T) {
	// The driver should be registered via init()
	d, err := driver.Get("mssql")
	if err != nil {
		t.Fatalf("Failed to get mssql driver: %v", err)
	}

	if d.Name() != "mssql" {
		t.Errorf("Expected driver name 'mssql', got %q", d.Name())
	}

	// Test aliases
	for _, alias := range []string{"sqlserver", "sql-server"} {
		d, err := driver.Get(alias)
		if err != nil {
			t.Errorf("Failed to get driver by alias %q: %v", alias, err)
			continue
		}
		if d.Name() != "mssql" {
			t.Errorf("Expected driver name 'mssql' for alias %q, got %q", alias, d.Name())
		}
	}
}

func TestDialect(t *testing.T) {
	dialect := &Dialect{}

	tests := []struct {
		name     string
		method   func() string
		expected string
	}{
		{"DBType", dialect.DBType, "mssql"},
		{"QuoteIdentifier", func() string { return dialect.QuoteIdentifier("test") }, `[test]`},
		{"QualifyTable", func() string { return dialect.QualifyTable("dbo", "users") }, `[dbo].[users]`},
		{"ParameterPlaceholder", func() string { return dialect.ParameterPlaceholder(1) }, "@p1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.method()
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestAvailableDrivers(t *testing.T) {
	available := driver.Available()
	found := false
	for _, name := range available {
		if name == "mssql" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("MSSQL driver not in available list: %v", available)
	}
}

func TestQuoteIdentifierWithSpecialChars(t *testing.T) {
	dialect := &Dialect{}

	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "[simple]"},
		{"with space", "[with space]"},
		{"with]bracket", "[with]]bracket]"},
		{"schema.table", "[schema.table]"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := dialect.QuoteIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("QuoteIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestBuildDSNEncrypt(t *testing.T) {
	dialect := &Dialect{}

	tests := []struct {
		name     string
		encrypt  bool
		expected string
	}{
		{"encrypt true", true, "&encrypt=true"},
		{"encrypt false uses disable", false, "&encrypt=disable"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := dialect.BuildDSN("localhost", 1433, "testdb", "sa", "pass", map[string]any{
				"encrypt": tt.encrypt,
			})
			if !strings.Contains(dsn, tt.expected) {
				t.Errorf("BuildDSN with encrypt=%v should contain %q, got %q", tt.encrypt, tt.expected, dsn)
			}
		})
	}
}
