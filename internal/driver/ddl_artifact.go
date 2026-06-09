package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WriteDDLArtifact writes a rendered DDL statement to the run artifact folder.
func WriteDDLArtifact(dir, name, ddl string) error {
	if dir == "" {
		return nil
	}
	if name == "" {
		name = "ddl.sql"
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("creating DDL artifact dir: %w", err)
	}
	path := filepath.Join(dir, name)
	body := ddl
	if !strings.HasSuffix(strings.TrimSpace(body), ";") {
		body += ";"
	}
	body += "\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		return fmt.Errorf("writing DDL artifact %s: %w", path, err)
	}
	return nil
}
