//go:build !unix && !windows

package secrets

func checkSecretsFilePermissions(string) error {
	return nil
}
