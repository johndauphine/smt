//go:build unix

package secrets

import (
	"fmt"
	"os"
)

func checkSecretsFilePermissions(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return nil
	}
	mode := info.Mode().Perm()
	if mode&0077 != 0 {
		return fmt.Errorf("secrets file %s has insecure permissions (%04o). "+
			"Other users can read your API keys. Run: chmod 600 %s", path, mode, path)
	}
	return nil
}
