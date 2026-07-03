package mysql

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
)

func TestBuildDSNTimeouts(t *testing.T) {
	d := &Dialect{}
	dsn := d.BuildDSN("localhost", 3306, "testdb", "root", "pass", map[string]any{})

	if !strings.Contains(dsn, "writeTimeout=5m") {
		t.Errorf("DSN missing writeTimeout: %s", dsn)
	}
	if !strings.Contains(dsn, "readTimeout=5m") {
		t.Errorf("DSN missing readTimeout: %s", dsn)
	}
}

func TestBuildDSNTimeoutOverride(t *testing.T) {
	d := &Dialect{}
	dsn := d.BuildDSN("localhost", 3306, "testdb", "root", "pass", map[string]any{
		"writeTimeout": "10m",
		"readTimeout":  "10m",
	})

	// User-provided values should not be overridden
	if strings.Contains(dsn, "writeTimeout=5m") {
		t.Errorf("DSN should not override user writeTimeout: %s", dsn)
	}
	if strings.Contains(dsn, "readTimeout=5m") {
		t.Errorf("DSN should not override user readTimeout: %s", dsn)
	}
}

func TestBuildDSNCredentialsRoundTrip(t *testing.T) {
	d := &Dialect{}
	user := "u@%+ space"
	password := "p@:/%+ space"
	dsn := d.BuildDSN("localhost", 3306, "test db", user, password, map[string]any{})
	cfg, err := gomysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", dsn, err)
	}
	if cfg.User != user {
		t.Fatalf("User = %q, want %q", cfg.User, user)
	}
	if cfg.Passwd != password {
		t.Fatalf("Passwd = %q, want %q", cfg.Passwd, password)
	}
	if cfg.DBName != "test db" {
		t.Fatalf("DBName = %q, want test db", cfg.DBName)
	}
}

func TestBuildDSNVerifyCADoesNotSkipVerification(t *testing.T) {
	d := &Dialect{}
	dsn := d.BuildDSN("localhost", 3306, "testdb", "root", "pass", map[string]any{
		"ssl_mode": "verify-ca",
	})
	cfg, err := gomysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", dsn, err)
	}
	if got := cfg.TLSConfig; got == "skip-verify" {
		t.Fatalf("verify-ca mapped to tls=%q in %s", got, dsn)
	}
	if got := cfg.TLSConfig; got != "true" {
		t.Fatalf("verify-ca tls = %q, want true", got)
	}
}

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
