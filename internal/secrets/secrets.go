// Package secrets provides secure configuration loading for API keys and encryption keys.
package secrets

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"

	"smt/internal/logging"
)

const (
	// DefaultSecretsDir is the default directory for secrets
	DefaultSecretsDir = ".secrets"
	// DefaultSecretsFile is the default filename for secrets
	DefaultSecretsFile = "smt-config.yaml"
	// SecretsFileEnvVar allows overriding the secrets file location
	SecretsFileEnvVar = "SMT_SECRETS_FILE"
	// SecureDirMode is the permission mode for the secrets directory
	SecureDirMode = 0700
	// SecureFileMode is the permission mode for the secrets file
	SecureFileMode = 0600
)

// Config represents the complete secrets configuration
type Config struct {
	AI                AIConfig            `yaml:"ai"`
	Encryption        EncryptionConfig    `yaml:"encryption"`
	Notifications     NotificationsConfig `yaml:"notifications"`
	MigrationDefaults MigrationDefaults   `yaml:"migration_defaults"`
}

// MigrationDefaults holds global default settings applied to every migration,
// overridable per-migration in a config.yaml. This is the v1-supported shape:
// only fields SMT actually consumes survive here. The broad DMT-era
// data-transfer tuning surface (workers, memory/buffer/reader/writer counts,
// chunk-checkpoint, table-retry, row-sample validation, AI runtime adjustment)
// was removed in v1 — SMT runs DDL, not row transfer. Legacy keys still present
// in a secrets file are warned about once and ignored (see legacyMigrationDefaultKeys).
type MigrationDefaults struct {
	// Connection pool sizing (applied to the schema source/target pools).
	MaxSourceConnections int `yaml:"max_source_connections,omitempty"` // Max source DB connections
	MaxTargetConnections int `yaml:"max_target_connections,omitempty"` // Max target DB connections

	// Schema-object defaults (use *bool to distinguish "not set" from "false").
	CreateIndexes          *bool `yaml:"create_indexes,omitempty"`           // Create non-PK indexes (default: true)
	CreateForeignKeys      *bool `yaml:"create_foreign_keys,omitempty"`      // Create FK constraints (default: true)
	CreateCheckConstraints *bool `yaml:"create_check_constraints,omitempty"` // Create CHECK constraints (default: false)

	// Directory for the SMT state DB, snapshots, and run artifacts.
	DataDir string `yaml:"data_dir,omitempty"`
}

// legacyMigrationDefaultKeys are migration_defaults keys SMT no longer honors
// (DMT-era data-transfer tuning, removed for v1). They are warned about once on
// load and otherwise ignored — see warnLegacyMigrationDefaults.
var legacyMigrationDefaultKeys = []string{
	"workers", "max_memory_mb", "read_ahead_buffers", "write_ahead_writers",
	"parallel_readers", "strict_consistency", "sample_validation", "sample_size",
	"checkpoint_frequency", "max_retries", "history_retention_days",
	"ai_adjust", "ai_adjust_interval",
}

// AIConfig holds AI provider configuration
type AIConfig struct {
	DefaultProvider string               `yaml:"default_provider"`
	Providers       map[string]*Provider `yaml:"providers"`
}

// Provider represents an AI provider configuration
type Provider struct {
	// Type optionally aliases this entry to a known provider type ("anthropic",
	// "openai", "google", "ollama", "lmstudio"). When set, the YAML key is
	// just a label and dispatch uses Type. Lets you have multiple entries that
	// share a backend — e.g. "anthropic-haiku" and "anthropic-sonnet" both
	// with provider: anthropic but different model fields. Empty (the common
	// case) means the YAML key IS the dispatch type — backward-compatible.
	//
	// Caveat: when aliasing, set Model explicitly. The Model fallback is keyed
	// on the dispatch type (DefaultModels[Type]), not the YAML label, so
	// `anthropic-haiku: { provider: anthropic }` with no `model:` field gets
	// Anthropic's default model (Sonnet) — not Haiku as the label suggests.
	// Always set `model:` on aliased entries so the label and the actual
	// model agree.
	Type string `yaml:"provider,omitempty"`

	APIKey           string   `yaml:"api_key,omitempty"`           // Required for cloud providers
	BaseURL          string   `yaml:"base_url,omitempty"`          // Required for local providers, optional for cloud
	Model            string   `yaml:"model,omitempty"`             // Optional, uses smart defaults
	ContextWindow    int      `yaml:"context_window,omitempty"`    // Optional, context window size in tokens (for Ollama/local providers)
	MaxTokens        int      `yaml:"max_tokens,omitempty"`        // Optional, max output tokens (default: 16000 for local, 4000 for cloud)
	TimeoutSeconds   int      `yaml:"timeout_seconds,omitempty"`   // Optional, API timeout in seconds (default: 30 for cloud, 120 for local)
	ModelTemperature *float64 `yaml:"model_temperature,omitempty"` // Optional sampling temperature for the model. Defaults to 0 (deterministic). Some providers reject 0 for certain models — e.g. OpenAI reasoning models (o-series, gpt-5.x) require model_temperature: 1.
}

// EffectiveType returns the dispatch-type for this provider entry: Type if
// set (alias case), else providerName (the YAML key, the legacy contract).
// Used by AI typemapper construction to decide which API client to invoke
// without forcing the YAML key to match a known provider name.
func (p *Provider) EffectiveType(providerName string) string {
	if p.Type != "" {
		return NormalizeProviderName(p.Type)
	}
	return NormalizeProviderName(providerName)
}

// NormalizeProviderName returns the canonical provider name used for dispatch.
// "gemini" remains accepted as a legacy alias for Google's Gemini API.
func NormalizeProviderName(name string) string {
	normalized := strings.ToLower(name)
	if normalized == "gemini" {
		return "google"
	}
	return normalized
}

// EncryptionConfig holds encryption-related secrets
type EncryptionConfig struct {
	MasterKey string `yaml:"master_key"`
}

// NotificationsConfig holds notification service credentials
type NotificationsConfig struct {
	Slack SlackConfig `yaml:"slack"`
}

// SlackConfig holds Slack webhook configuration
type SlackConfig struct {
	WebhookURL string `yaml:"webhook_url"`
}

// ProviderType categorizes providers by their API style
type ProviderType int

const (
	ProviderTypeCloud ProviderType = iota // Requires API key
	ProviderTypeLocal                     // Uses local base_url, no API key
)

// KnownProviders maps provider names to their types and default base URLs
var KnownProviders = map[string]struct {
	Type       ProviderType
	DefaultURL string
}{
	"anthropic": {ProviderTypeCloud, "https://api.anthropic.com"},
	"openai":    {ProviderTypeCloud, "https://api.openai.com"},
	"google":    {ProviderTypeCloud, "https://generativelanguage.googleapis.com"},
	"ollama":    {ProviderTypeLocal, "http://localhost:11434"},
	"lmstudio":  {ProviderTypeLocal, "http://localhost:1234"},
}

// DefaultModels maps providers to their default models.
// Anthropic default is Sonnet because Haiku, even with the cleaned-up
// introspection-facts prompt, was observed to misrender source-dialect
// computed-column metadata when translating to MSSQL (e.g. emitting
// `NUMERIC(15,2) AS (...) PERSISTED` where MSSQL forbids the explicit
// type on a computed column). Volume is low (one cached call per table);
// Sonnet's cost is negligible.
var DefaultModels = map[string]string{
	"anthropic": "claude-sonnet-4-6",
	"openai":    "gpt-4o",
	"google":    "gemini-2.0-flash",
	"gemini":    "gemini-2.0-flash", // Legacy alias for existing secrets files.
	"ollama":    "llama3",
	"lmstudio":  "local-model",
}

var (
	globalConfig *Config
	configOnce   sync.Once
	configErr    error
)

// Load loads the secrets configuration from the default or override location.
// It caches the result and returns the same config on subsequent calls.
func Load() (*Config, error) {
	configOnce.Do(func() {
		globalConfig, configErr = loadFromFile()
	})
	return globalConfig, configErr
}

// Reset clears the cached config (useful for testing).
func Reset() {
	configOnce = sync.Once{}
	globalConfig = nil
	configErr = nil
}

// GetSecretsPath returns the path to the secrets file
func GetSecretsPath() string {
	if envPath := os.Getenv(SecretsFileEnvVar); envPath != "" {
		return envPath
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(".", DefaultSecretsDir, DefaultSecretsFile)
	}
	return filepath.Join(homeDir, DefaultSecretsDir, DefaultSecretsFile)
}

// EnsureSecretsDir creates the secrets directory with secure permissions if it doesn't exist
func EnsureSecretsDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("getting home directory: %w", err)
	}

	secretsDir := filepath.Join(homeDir, DefaultSecretsDir)

	// Check if directory exists
	info, err := os.Stat(secretsDir)
	if os.IsNotExist(err) {
		// Create directory with secure permissions
		if err := os.MkdirAll(secretsDir, SecureDirMode); err != nil {
			return "", fmt.Errorf("creating secrets directory: %w", err)
		}
		return secretsDir, nil
	} else if err != nil {
		return "", fmt.Errorf("checking secrets directory: %w", err)
	}

	if !info.IsDir() {
		return "", fmt.Errorf("%s exists but is not a directory", secretsDir)
	}

	return secretsDir, nil
}

func loadFromFile() (*Config, error) {
	path := GetSecretsPath()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &SecretsNotFoundError{Path: path}
		}
		return nil, fmt.Errorf("reading secrets file: %w", err)
	}

	// Check file permissions - reject if too permissive (security requirement)
	info, err := os.Stat(path)
	if err == nil {
		mode := info.Mode().Perm()
		if mode&0077 != 0 {
			return nil, fmt.Errorf("secrets file %s has insecure permissions (%04o). "+
				"Other users can read your API keys. Run: chmod 600 %s", path, mode, path)
		}
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("parsing secrets file: %w", err)
	}

	// Warn (once) about removed DMT-era migration_defaults keys still present in
	// the file. They are ignored, not fatal — keep old secrets files loadable.
	warnLegacyMigrationDefaults(data)

	// Validate configuration
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &config, nil
}

// warnLegacyMigrationDefaults emits a single warning naming any removed
// migration_defaults keys present in the raw secrets YAML.
func warnLegacyMigrationDefaults(data []byte) {
	var raw struct {
		MigrationDefaults map[string]any `yaml:"migration_defaults"`
	}
	if err := yaml.Unmarshal(data, &raw); err != nil || len(raw.MigrationDefaults) == 0 {
		return
	}
	var found []string
	for _, key := range legacyMigrationDefaultKeys {
		if _, ok := raw.MigrationDefaults[key]; ok {
			found = append(found, key)
		}
	}
	if len(found) > 0 {
		logging.Warn("secrets: ignoring removed migration_defaults keys (DMT-era, dropped in v1): %s",
			strings.Join(found, ", "))
	}
}

// Validate checks that the configuration is valid
func (c *Config) Validate() error {
	// AI settings are optional - only validate if configured
	if c.AI.DefaultProvider != "" {
		// Check that default provider exists
		provider, err := c.GetProvider(c.AI.DefaultProvider)
		if err != nil {
			return fmt.Errorf("default provider %q not found in providers", c.AI.DefaultProvider)
		}

		// Validate the default provider has required fields
		if err := validateProvider(c.AI.DefaultProvider, provider); err != nil {
			return err
		}
	}

	return nil
}

func validateProvider(name string, p *Provider) error {
	providerType := p.EffectiveType(name)
	known, isKnown := KnownProviders[providerType]

	if isKnown {
		if known.Type == ProviderTypeCloud && p.APIKey == "" {
			return fmt.Errorf("provider %q requires api_key", name)
		}
		if known.Type == ProviderTypeLocal && p.BaseURL == "" {
			// Use default URL for known local providers
			p.BaseURL = known.DefaultURL
		}
	} else {
		// Unknown provider - must have either API key or base URL
		if p.APIKey == "" && p.BaseURL == "" {
			return fmt.Errorf("provider %q requires either api_key or base_url", name)
		}
	}

	return nil
}

// GetDefaultProvider returns the configured default AI provider
func (c *Config) GetDefaultProvider() (*Provider, string, error) {
	if c.AI.DefaultProvider == "" {
		return nil, "", fmt.Errorf("no default provider configured")
	}

	provider, err := c.GetProvider(c.AI.DefaultProvider)
	if err != nil {
		return nil, "", fmt.Errorf("default provider %q not found", c.AI.DefaultProvider)
	}

	providerName := c.AI.DefaultProvider
	if strings.EqualFold(providerName, "gemini") {
		providerName = "google"
	}
	return provider, providerName, nil
}

// GetProvider returns a specific AI provider by name
func (c *Config) GetProvider(name string) (*Provider, error) {
	provider, ok := c.AI.Providers[name]
	if !ok {
		normalized := NormalizeProviderName(name)
		if normalized != name {
			provider, ok = c.AI.Providers[normalized]
		}
		if !ok && normalized == "google" {
			provider, ok = c.AI.Providers["gemini"]
		}
	}
	if !ok {
		return nil, fmt.Errorf("provider %q not found", name)
	}
	return provider, nil
}

// GetMasterKey returns the encryption master key
func (c *Config) GetMasterKey() string {
	return c.Encryption.MasterKey
}

// GetMigrationDefaults returns the global migration defaults.
func (c *Config) GetMigrationDefaults() *MigrationDefaults {
	defaults := c.MigrationDefaults
	return &defaults
}

// GetEffectiveBaseURL returns the base URL for a provider, using defaults if not specified
func (p *Provider) GetEffectiveBaseURL(providerName string) string {
	if p.BaseURL != "" {
		return p.BaseURL
	}
	providerName = NormalizeProviderName(providerName)
	if known, ok := KnownProviders[providerName]; ok {
		return known.DefaultURL
	}
	return ""
}

// GetEffectiveModel returns the model for a provider, using defaults if not specified
func (p *Provider) GetEffectiveModel(providerName string) string {
	if p.Model != "" {
		return p.Model
	}
	providerName = NormalizeProviderName(providerName)
	if defaultModel, ok := DefaultModels[providerName]; ok {
		return defaultModel
	}
	return ""
}

// GetEffectiveContextWindow returns the context window size for a provider.
// Returns the configured value if set, otherwise returns a conservative default of 8192 tokens.
// Users should configure this based on their specific model's capabilities:
// - llama3:8b, llama3.2: 8192 tokens
// - llama3:70b, llama3.1: 131072 tokens (128K)
// - qwen2.5, deepseek: 32768 tokens (32K)
// - mistral, mixtral: 8192-32768 tokens (varies by version)
func (p *Provider) GetEffectiveContextWindow() int {
	if p.ContextWindow > 0 {
		return p.ContextWindow
	}
	// Conservative default that works with most models
	return 8192
}

// GetEffectiveMaxTokens returns the max output tokens for a provider.
// Returns the configured value if set, otherwise returns a default based on provider type.
// Local providers default to 16000 (reasoning models need headroom for thinking + output).
// Cloud providers default to 4000.
func (p *Provider) GetEffectiveMaxTokens(providerName string) int {
	if p.MaxTokens > 0 {
		return p.MaxTokens
	}
	// Treat as local if it's a known local provider OR has a base_url without an API key
	// (custom OpenAI-compatible local providers)
	if IsLocalProvider(providerName) || (p.BaseURL != "" && p.APIKey == "") {
		return 16000
	}
	return 4000
}

// GetEffectiveModelTemperature returns the sampling temperature for a provider's
// model. Returns the configured value if set, otherwise 0 (deterministic).
// Some providers reject 0 for certain models (e.g. OpenAI reasoning models
// require model_temperature: 1) — set ModelTemperature explicitly in the
// secrets file for those.
func (p *Provider) GetEffectiveModelTemperature() float64 {
	if p.ModelTemperature != nil {
		return *p.ModelTemperature
	}
	return 0
}

// IsLocalProvider returns true if the provider is a local provider (no API key needed)
func IsLocalProvider(name string) bool {
	name = NormalizeProviderName(name)
	if known, ok := KnownProviders[name]; ok {
		return known.Type == ProviderTypeLocal
	}
	return false
}

// SecretsNotFoundError is returned when the secrets file doesn't exist
type SecretsNotFoundError struct {
	Path string
}

func (e *SecretsNotFoundError) Error() string {
	return fmt.Sprintf(`secrets file not found: %s

To create a secrets file, run:
  smt init-secrets

Or create %s manually with:

ai:
  default_provider: anthropic
  providers:
    anthropic:
      api_key: "your-api-key"

encryption:
  master_key: "your-master-key"
`, e.Path, e.Path)
}

// GenerateTemplate returns a template secrets file content
func GenerateTemplate() string {
	return `# SMT Secrets Configuration
# This file contains sensitive configuration that should not be committed to version control.
# Permissions should be restricted: chmod 600 ~/.secrets/smt-config.yaml

ai:
  default_provider: anthropic  # Which provider to use by default

  providers:
    # Cloud providers (require API key)
    anthropic:
      api_key: ""  # Get from https://console.anthropic.com/
      model: "claude-sonnet-4-6"  # optional

    openai:
      api_key: ""  # Get from https://platform.openai.com/
      model: "gpt-4o"  # optional
      # model_temperature: 1  # required when model is an OpenAI reasoning family
      #                       # (o-series, gpt-5.x) — they reject the default 0

    google:
      api_key: ""  # Get from https://aistudio.google.com/app/apikey
      model: "gemini-2.0-flash"  # optional
      # model_temperature: 0  # optional sampling temperature (default 0 = deterministic)

    # Local providers (no API key needed)
    ollama:
      base_url: "http://localhost:11434"
      model: "llama3"
      # context_window: 8192  # optional, defaults to 8192 (conservative)
      # max_tokens: 16000     # optional, max output tokens (default: 16000 for local, 4000 for cloud)
      # Common context_window values:
      # - llama3:8b, llama3.2: 8192
      # - llama3:70b, llama3.1: 131072 (128K)
      # - qwen2.5, deepseek: 32768 (32K)
      # - mistral, mixtral: 8192-32768 (varies)

    lmstudio:
      base_url: "http://localhost:1234"
      model: "local-model"
      # context_window: 8192  # optional, configure based on your model
      # max_tokens: 16000     # optional, increase for reasoning models (e.g., Qwen3, GPT-OSS)

encryption:
  master_key: ""  # Used for encrypting profiles, generate with: openssl rand -base64 32

notifications:
  slack:
    webhook_url: ""  # Slack webhook URL for migration notifications

# Global migration defaults (can be overridden per-migration)
migration_defaults:
  # Schema-object defaults
  create_indexes: true            # Create non-PK indexes
  create_foreign_keys: true       # Create FK constraints
  create_check_constraints: false # Create CHECK constraints

  # Optional: connection pool sizing and state/artifact directory
  # max_source_connections: 4
  # max_target_connections: 4
  # data_dir: ~/.smt
`
}
