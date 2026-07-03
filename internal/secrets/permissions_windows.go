//go:build windows

package secrets

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func checkSecretsFilePermissions(path string) error {
	if _, err := os.Stat(path); err != nil {
		return nil
	}

	output, err := exec.Command("icacls", path).Output()
	if err != nil {
		return nil
	}

	if hasBroadWindowsReadACL(string(output)) {
		return fmt.Errorf("secrets file %s may have insecure permissions. "+
			"Other users can read your API keys. Run in PowerShell to secure: "+
			"icacls \"%s\" /inheritance:r /grant:r \"%%USERNAME%%:F\"", path, path)
	}
	return nil
}

func hasBroadWindowsReadACL(output string) bool {
	output = strings.ToLower(output)
	insecureEntries := []string{
		"everyone:",
		"everyone:(",
		"authenticated users:",
		"authenticated users:(",
		"builtin\\users:",
		"builtin\\users:(",
	}
	for _, entry := range insecureEntries {
		if strings.Contains(output, entry) {
			return true
		}
	}
	return false
}
