package orchestrator

import (
	"os"
	"path/filepath"
	"testing"

	"smt/internal/config"
	"smt/internal/secrets"
)

func TestNewNotifierHonorsDisabledSlackConfig(t *testing.T) {
	configureSlackWebhookSecrets(t)

	notifier := newNotifier(&config.Config{
		Slack: &config.SlackConfig{Enabled: false},
	})
	if notifierIsEnabled(t, notifier) {
		t.Fatal("newNotifier enabled Slack from secrets despite disabled Slack config")
	}
}

func TestNewNotifierFallsBackToSecretsWhenSlackOmitted(t *testing.T) {
	configureSlackWebhookSecrets(t)

	notifier := newNotifier(&config.Config{})
	if !notifierIsEnabled(t, notifier) {
		t.Fatal("newNotifier did not enable Slack from secrets when Slack config was omitted")
	}
}

func configureSlackWebhookSecrets(t *testing.T) {
	t.Helper()
	secretsPath := filepath.Join(t.TempDir(), "secrets.yaml")
	secretsYAML := `
notifications:
  slack:
    webhook_url: "https://hooks.slack.com/services/SECRET"
`
	if err := os.WriteFile(secretsPath, []byte(secretsYAML), 0600); err != nil {
		t.Fatalf("write secrets: %v", err)
	}
	secrets.Reset()
	t.Setenv(secrets.SecretsFileEnvVar, secretsPath)
	t.Cleanup(secrets.Reset)
}

func notifierIsEnabled(t *testing.T, notifier any) bool {
	t.Helper()
	state, ok := notifier.(interface{ IsEnabled() bool })
	if !ok {
		t.Fatalf("notifier %T does not expose IsEnabled", notifier)
	}
	return state.IsEnabled()
}
