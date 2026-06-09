package mysql

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
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
