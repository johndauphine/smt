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

func TestReaderLoadColumnsUsesQuotedNullSafeIdentityProbe(t *testing.T) {
	body := readerFunctionSource(t, "func (r *Reader) loadColumns", "func (r *Reader) loadComputedColumns")
	for _, needle := range []string{
		"OBJECT_ID(QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME))",
		"sql.NullInt64",
		"isIdentity.Valid && isIdentity.Int64 == 1",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("loadColumns missing identity safety marker %q", needle)
		}
	}
}

func TestReaderLoadIndexesSeparatesKeyAndIncludeColumns(t *testing.T) {
	body := readerFunctionSource(t, "func (r *Reader) LoadIndexes", "// fkColumnDelimiter")
	for _, needle := range []string{
		"AND ic.is_included_column = 0",
		"AND ic.is_included_column = 1",
		"WITHIN GROUP (ORDER BY ic.key_ordinal)",
		"WITHIN GROUP (ORDER BY ic.index_column_id)",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("LoadIndexes missing INCLUDE/key-column marker %q", needle)
		}
	}
}

func TestReaderChecksIterationErrors(t *testing.T) {
	tests := []struct {
		name  string
		start string
		end   string
		want  string
	}{
		{
			name:  "ExtractSchema",
			start: "func (r *Reader) ExtractSchema",
			end:   "func (r *Reader) applyActualRowSizes",
			want:  `return nil, fmt.Errorf("iterating tables: %w", err)`,
		},
		{
			name:  "loadPrimaryKey",
			start: "func (r *Reader) loadPrimaryKey",
			end:   "func (r *Reader) loadRowCount",
			want:  `return fmt.Errorf("iterating primary key: %w", err)`,
		},
		{
			name:  "LoadIndexes",
			start: "func (r *Reader) LoadIndexes",
			end:   "// fkColumnDelimiter",
			want:  `return fmt.Errorf("iterating indexes: %w", err)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := readerFunctionSource(t, tt.start, tt.end)
			if !strings.Contains(body, tt.want) {
				t.Fatalf("%s missing rows.Err guard %q", tt.name, tt.want)
			}
		})
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

func readerFunctionSource(t *testing.T, start, end string) string {
	t.Helper()
	body := readReaderSource(t)
	startIdx := strings.Index(body, start)
	if startIdx < 0 {
		t.Fatalf("reader.go missing start marker %q", start)
	}
	endIdx := strings.Index(body[startIdx:], end)
	if endIdx < 0 {
		t.Fatalf("reader.go missing end marker %q after %q", end, start)
	}
	return body[startIdx : startIdx+endIdx]
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
		{"TableHint", func() string { return dialect.TableHint(false) }, "WITH (NOLOCK)"},
		{"TableHintStrict", func() string { return dialect.TableHint(true) }, ""},
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

func TestIsASCIINumeric(t *testing.T) {
	tests := []struct {
		input    []byte
		expected bool
	}{
		{[]byte("123"), true},
		{[]byte("-45.67"), true},
		{[]byte("+0.5"), true},
		{[]byte("1.5E+10"), true},
		{[]byte("1e-5"), true},
		{[]byte(".5"), true},
		{[]byte(""), false},
		{[]byte("."), false},
		{[]byte("+-1"), false},
		{[]byte("1.2.3"), false},
		{[]byte("abc"), false},
		{[]byte{0x01, 0x02, 0x03}, false}, // binary data
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			result := isASCIINumeric(tt.input)
			if result != tt.expected {
				t.Errorf("isASCIINumeric(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
