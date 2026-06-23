package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"smt/internal/logging"
	"smt/internal/secrets"
)

// Retry configuration constants
const (
	// defaultMaxRetries is the default number of retry attempts for transient failures.
	defaultMaxRetries = 3

	// defaultBaseDelay is the initial delay between retries.
	defaultBaseDelay = 1 * time.Second

	// defaultMaxDelay is the maximum delay between retries (cap for exponential backoff).
	defaultMaxDelay = 10 * time.Second
)

// AIProvider represents supported AI providers for type mapping.
type AIProvider string

const (
	// ProviderAnthropic uses Anthropic's Claude API.
	ProviderAnthropic AIProvider = "anthropic"
	// ProviderOpenAI uses OpenAI's API.
	ProviderOpenAI AIProvider = "openai"
	// ProviderGoogle uses Google's Gemini API.
	ProviderGoogle AIProvider = "google"
	// ProviderGemini is the legacy name for ProviderGoogle.
	ProviderGemini AIProvider = "gemini"
	// ProviderOllama uses local Ollama with OpenAI-compatible API.
	ProviderOllama AIProvider = "ollama"
	// ProviderLMStudio uses local LM Studio with OpenAI-compatible API.
	ProviderLMStudio AIProvider = "lmstudio"
)

// IsValidAIProvider returns true if the provider name is valid (case-insensitive).
func IsValidAIProvider(provider string) bool {
	switch AIProvider(strings.ToLower(provider)) {
	case ProviderAnthropic, ProviderOpenAI, ProviderGoogle, ProviderGemini, ProviderOllama, ProviderLMStudio:
		return true
	}
	return false
}

// NormalizeAIProvider returns the canonical lowercase provider name.
// Returns empty string if the provider is invalid.
func NormalizeAIProvider(provider string) string {
	normalized := strings.ToLower(provider)
	if AIProvider(normalized) == ProviderGemini {
		return string(ProviderGoogle)
	}
	if IsValidAIProvider(normalized) {
		return normalized
	}
	return ""
}

// AITypeMapper uses AI to map database types.
// It implements the TypeMapper interface.
type AITypeMapper struct {
	providerName string // YAML key in the secrets file (used for cache/log labels)
	providerType string // dispatch type — equals providerName unless aliased via provider: in YAML
	provider     *secrets.Provider
	client       *http.Client
	cache        *TypeMappingCache
	cacheFile    string
	cacheMu      sync.RWMutex
	// requestsMu was previously held across CallAI/queryAI to serialize
	// outbound HTTP, an inheritance from DMT's data-transfer workers
	// where many goroutines could fire type-mapping calls in parallel.
	// SMT controls concurrency at the orchestrator layer (via
	// Migration.AIConcurrency in phases.go), so request serialization
	// is now done at a single point with a known bound rather than at
	// the per-request layer. The HTTP layer's 429 retry handles
	// provider-side rate limits.
	inflight       sync.Map // Track in-flight requests to avoid duplicate API calls
	timeoutSeconds int
}

// inflightRequest tracks an in-progress API request for a specific type.
type inflightRequest struct {
	done   chan struct{}
	result string
	err    error
}

// NewAITypeMapper creates a new AI-powered type mapper using the secrets configuration.
func NewAITypeMapper(providerName string, provider *secrets.Provider) (*AITypeMapper, error) {
	if provider == nil {
		return nil, fmt.Errorf("AI provider configuration is required")
	}

	// providerType drives dispatch + IsLocal checks. providerName is the YAML
	// key we keep around for cache/log identity. Equal in the legacy contract
	// (no `provider:` alias); differ when the user opts into multiple entries
	// per backend (e.g. anthropic-haiku + anthropic-sonnet, both Type:
	// anthropic).
	providerType := provider.EffectiveType(providerName)
	if normalized := NormalizeAIProvider(providerType); normalized != "" {
		providerType = normalized
	}

	// Validate cloud providers have API key
	if !secrets.IsLocalProvider(providerType) && provider.APIKey == "" {
		return nil, fmt.Errorf("AI provider %q requires an API key", providerName)
	}

	// Get effective model
	model := provider.GetEffectiveModel(providerType)
	if model == "" {
		return nil, fmt.Errorf("no model specified for provider %q", providerName)
	}

	// Set up cache file
	homeDir, _ := os.UserHomeDir()
	cacheFile := filepath.Join(homeDir, ".smt", "type-cache.json")

	// Determine API timeout: user-configured > local provider default > cloud default.
	// Local providers and thinking models need more time for inference.
	timeoutSec := 60
	if IsLocalProvider(providerType) {
		timeoutSec = 120
	}
	if provider.TimeoutSeconds > 0 {
		timeoutSec = provider.TimeoutSeconds
	}

	mapper := &AITypeMapper{
		providerName: providerName,
		providerType: providerType,
		provider:     provider,
		client: &http.Client{
			Timeout: time.Duration(timeoutSec) * time.Second,
		},
		cache:          NewTypeMappingCache(),
		cacheFile:      cacheFile,
		timeoutSeconds: timeoutSec,
	}

	// Load existing cache
	if err := mapper.loadCache(); err != nil {
		logging.Warn("Failed to load AI type mapping cache: %v", err)
	}

	return mapper, nil
}

// NewAITypeMapperFromSecrets creates an AI type mapper from the global secrets configuration.
func NewAITypeMapperFromSecrets() (*AITypeMapper, error) {
	config, err := secrets.Load()
	if err != nil {
		return nil, fmt.Errorf("loading secrets: %w", err)
	}

	provider, name, err := config.GetDefaultProvider()
	if err != nil {
		return nil, fmt.Errorf("getting default AI provider: %w", err)
	}

	return NewAITypeMapper(name, provider)
}

// NewAITypeMapperByName creates an AI type mapper for a specific provider
// entry in the secrets file. Used by the orchestrator to build the verifier
// mapper from ai_review.model. Errors if the provider name is not found in the
// secrets config.
func NewAITypeMapperByName(name string) (*AITypeMapper, error) {
	config, err := secrets.Load()
	if err != nil {
		return nil, fmt.Errorf("loading secrets: %w", err)
	}

	provider, err := config.GetProvider(name)
	if err != nil {
		return nil, fmt.Errorf("getting AI provider %q: %w", name, err)
	}

	return NewAITypeMapper(name, provider)
}

// MapType maps a source type to the target type using AI.
// This method is safe to call concurrently - it uses in-flight request tracking
// to avoid duplicate API calls for the same type.
// Core schema DDL generation is deterministic and does not call this mapper.
// This method panics on error - use MapTypeWithError for error handling.
func (m *AITypeMapper) MapType(info TypeInfo) string {
	result, err := m.MapTypeWithError(info)
	if err != nil {
		panic(fmt.Sprintf("AI type mapping failed for %s.%s: %v", info.SourceDBType, info.DataType, err))
	}
	return result
}

// MapTypeWithError maps a source type to the target type using AI, returning any error.
func (m *AITypeMapper) MapTypeWithError(info TypeInfo) (string, error) {
	// Validate input
	if info.DataType == "" {
		return "", fmt.Errorf("DataType is required")
	}
	if info.SourceDBType == "" {
		return "", fmt.Errorf("SourceDBType is required")
	}
	if info.TargetDBType == "" {
		return "", fmt.Errorf("TargetDBType is required")
	}

	cacheKey := m.cacheKey(info)

	// Check cache first (fast path)
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		return cached, nil
	}
	m.cacheMu.RUnlock()

	// Check if there's already an in-flight request for this key
	req := &inflightRequest{done: make(chan struct{})}
	if existing, loaded := m.inflight.LoadOrStore(cacheKey, req); loaded {
		// Another goroutine is already fetching this type, wait for it
		existingReq := existing.(*inflightRequest)
		<-existingReq.done
		if existingReq.err != nil {
			return "", existingReq.err
		}
		return existingReq.result, nil
	}

	// We're the first to request this type, do the API call
	defer func() {
		close(req.done) // Signal waiting goroutines
		m.inflight.Delete(cacheKey)
	}()

	// Double-check cache after acquiring the slot
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		req.result = cached
		return cached, nil
	}
	m.cacheMu.RUnlock()

	// Log that we're calling AI (not in cache)
	logging.Debug("AI type mapping: %s.%s -> %s (not cached, calling API)", info.SourceDBType, info.DataType, info.TargetDBType)

	// Call AI API
	result, err := m.queryAI(info)
	if err != nil {
		req.err = err
		return "", fmt.Errorf("AI type mapping failed for %s.%s -> %s: %w",
			info.SourceDBType, info.DataType, info.TargetDBType, err)
	}

	// Cache the result
	m.cacheMu.Lock()
	m.cache.Set(cacheKey, result)
	m.cacheMu.Unlock()

	// Persist cache
	if err := m.saveCache(); err != nil {
		logging.Warn("Failed to save AI type mapping cache: %v", err)
	}

	logging.Debug("AI mapped %s.%s -> %s.%s (cached for future use)",
		info.SourceDBType, info.DataType, info.TargetDBType, result)

	req.result = result
	return result, nil
}

// CanMap returns true - AI mapper can attempt to map any type.
func (m *AITypeMapper) CanMap(sourceDBType, targetDBType string) bool {
	return true
}

// SupportedTargets returns ["*"] indicating AI can map to any target.
func (m *AITypeMapper) SupportedTargets() []string {
	return []string{"*"}
}

func (m *AITypeMapper) cacheKey(info TypeInfo) string {
	return fmt.Sprintf("%s:%s:%s:%d:%d:%d",
		info.SourceDBType, info.TargetDBType, strings.ToLower(info.DataType),
		info.MaxLength, info.Precision, info.Scale)
}

// Ask is a generic free-form prompt entrypoint for callers outside this
// package (e.g. schemadiff's AI SQL renderer) that need to use the same
// multi-provider HTTP plumbing as the type mapper. It does not consult
// the type-mapping cache. Caller supplies its own context with timeout.
func (m *AITypeMapper) Ask(ctx context.Context, prompt string) (string, error) {
	return m.dispatch(ctx, prompt)
}

func (m *AITypeMapper) queryAI(info TypeInfo) (string, error) {
	prompt := m.buildPrompt(info)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.timeoutSeconds)*time.Second)
	defer cancel()

	result, err := m.dispatch(ctx, prompt)
	if err != nil {
		return "", err
	}

	// Type-mapping responses are short type names; lowercase them so the
	// cache key normalizes consistently.
	result = strings.TrimSpace(result)
	result = strings.Trim(result, "\"'`")
	result = strings.ToLower(result)

	return result, nil
}

// dispatch sends a prompt to the configured provider and returns the raw
// response text. It is shared by queryAI (cached type mapping) and Ask
// (free-form callers like schemadiff). Both share the multi-provider HTTP
// implementations in this file so adding a new provider is a one-place edit.
func (m *AITypeMapper) dispatch(ctx context.Context, prompt string) (string, error) {
	switch AIProvider(m.providerType) {
	case ProviderAnthropic:
		return m.queryAnthropicAPI(ctx, prompt)
	case ProviderOpenAI:
		return m.queryOpenAIAPI(ctx, prompt, "https://api.openai.com/v1/chat/completions")
	case ProviderGoogle, ProviderGemini:
		return m.queryGeminiAPI(ctx, prompt)
	case ProviderOllama:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerType)
		return m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	case ProviderLMStudio:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerType)
		return m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	default:
		// Unknown providers can ride the OpenAI-compatible endpoint if
		// they configured a base_url (covers vLLM, llama.cpp server, etc.).
		if m.provider.BaseURL != "" {
			return m.queryOpenAICompatAPI(ctx, prompt, m.provider.BaseURL+"/v1/chat/completions")
		}
		return "", fmt.Errorf("unsupported AI provider: %s (dispatch type %q)", m.providerName, m.providerType)
	}
}

// maxSampleValueLen is the maximum length of a single sample value in prompts.
const maxSampleValueLen = 100

// sanitizeSampleValue cleans and truncates a sample value for inclusion in AI prompts.
// It redacts potential PII patterns and limits length.
func sanitizeSampleValue(value string) string {
	if value == "" {
		return value
	}

	// Truncate to max length
	if len(value) > maxSampleValueLen {
		value = value[:maxSampleValueLen] + "..."
	}

	// Redact potential email addresses
	if strings.Contains(value, "@") && strings.Contains(value, ".") {
		parts := strings.SplitN(value, "@", 2)
		if len(parts) == 2 && len(parts[0]) > 0 {
			value = "[EMAIL]@" + parts[1]
		}
	}

	// Redact potential SSN patterns (XXX-XX-XXXX)
	if len(value) == 11 && value[3] == '-' && value[6] == '-' {
		allDigits := true
		for i, c := range value {
			if i == 3 || i == 6 {
				continue
			}
			if c < '0' || c > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			value = "[SSN]"
		}
	}

	// Redact potential phone numbers (10+ consecutive digits)
	digitCount := 0
	for _, c := range value {
		if c >= '0' && c <= '9' {
			digitCount++
		}
	}
	if digitCount >= 10 && digitCount <= 15 {
		nonDigits := len(value) - digitCount
		if nonDigits <= 4 {
			value = "[PHONE]"
		}
	}

	return value
}

func (m *AITypeMapper) buildPrompt(info TypeInfo) string {
	var sb strings.Builder
	sb.WriteString("You are a database type mapping expert.\n\n")
	sb.WriteString("Based on DDL metadata only (no sample data), ")
	sb.WriteString(fmt.Sprintf("map this %s type to %s:\n", info.SourceDBType, info.TargetDBType))
	sb.WriteString(fmt.Sprintf("- Type: %s\n", info.DataType))
	if info.MaxLength > 0 {
		sb.WriteString(fmt.Sprintf("- Max length: %d\n", info.MaxLength))
	} else if info.MaxLength == -1 {
		sb.WriteString("- Max length: MAX\n")
	}
	if info.Precision > 0 {
		sb.WriteString(fmt.Sprintf("- Precision: %d\n", info.Precision))
	}
	if info.Scale > 0 {
		sb.WriteString(fmt.Sprintf("- Scale: %d\n", info.Scale))
	}

	// Sample values are no longer collected (privacy improvement)
	// Type mapping now works purely from DDL metadata

	// Add target database context
	switch info.TargetDBType {
	case "postgres":
		sb.WriteString("\nTarget: Standard PostgreSQL (no extensions installed).\n")
	case "mssql":
		sb.WriteString("\nTarget: SQL Server with full native type support.\n")
	case "mysql":
		sb.WriteString("\nTarget: MySQL 8.0+ or MariaDB 10.5+ with InnoDB engine.\n")
		sb.WriteString("Note: MySQL varchar has 65535 byte max (use TEXT for longer). Use utf8mb4 charset.\n")
	}

	sb.WriteString("\nReturn ONLY the ")
	sb.WriteString(info.TargetDBType)
	sb.WriteString(" type name (e.g., varchar(255), numeric(10,2), text).\n")
	sb.WriteString("No explanation, just the type.")

	return sb.String()
}

// Anthropic API types
type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens"`
	System    string             `json:"system,omitempty"`
	Messages  []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// sanitizeErrorResponse truncates and sanitizes API error response bodies.
func sanitizeErrorResponse(body []byte, maxLen int) string {
	if maxLen <= 0 {
		maxLen = 200
	}

	s := string(body)
	if len(s) > maxLen {
		s = s[:maxLen] + "..."
	}

	keyPatterns := []string{"sk-", "api-", "key-", "secret-", "token-"}
	for _, pattern := range keyPatterns {
		for {
			idx := strings.Index(strings.ToLower(s), pattern)
			if idx == -1 {
				break
			}
			endIdx := idx + len(pattern) + 40
			if endIdx > len(s) {
				endIdx = len(s)
			}
			s = s[:idx] + "[REDACTED]" + s[endIdx:]
		}
	}

	return s
}

// isRetryableError determines if an error is transient and should be retried.
// Returns true for network timeouts, temporary network errors, connection errors,
// server errors (5xx), and rate limiting responses (429).
func isRetryableError(err error, statusCode int) bool {
	// Check for retryable HTTP status codes (server errors, rate limiting)
	if statusCode >= 500 || statusCode == 429 {
		return true
	}

	if err == nil {
		return false
	}

	// Check for context deadline exceeded (timeout)
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for EOF errors
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for network errors (timeout or temporary)
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Retry on timeout or temporary network errors
		//nolint:staticcheck // Temporary() is deprecated but still useful for some net errors
		return netErr.Timeout() || netErr.Temporary()
	}

	// Check for connection errors - only retry on temporary or dial errors
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		// Only retry dial errors (connection refused, etc.) or temporary errors
		//nolint:staticcheck // Temporary() is deprecated but still useful for some net errors
		if opErr.Op == "dial" || opErr.Temporary() {
			return true
		}
		return false
	}

	// Check for common retryable error messages (fallback for wrapped errors)
	errMsg := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"tls handshake timeout",
		"connection reset",
		"connection refused",
		"broken pipe",
		"no such host",
		"temporary failure",
		"i/o timeout",
		"unexpected eof", // Fallback for wrapped EOF errors
	}
	for _, pattern := range retryablePatterns {
		if strings.Contains(errMsg, pattern) {
			return true
		}
	}

	return false
}

// retryableHTTPDo executes an HTTP request with exponential backoff retry logic.
// It retries on transient network errors and server errors (5xx, 429).
func (m *AITypeMapper) retryableHTTPDo(ctx context.Context, reqFunc func() (*http.Request, error)) (*http.Response, []byte, error) {
	var lastErr error
	var lastStatusCode int

	for attempt := 0; attempt <= defaultMaxRetries; attempt++ {
		// Check context before each attempt
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		// Create fresh request for each attempt
		req, err := reqFunc()
		if err != nil {
			return nil, nil, fmt.Errorf("creating request: %w", err)
		}

		// Execute request
		resp, err := m.client.Do(req)
		if err != nil {
			lastErr = err
			lastStatusCode = 0

			if !isRetryableError(err, 0) {
				return nil, nil, fmt.Errorf("API request failed: %w", err)
			}

			// Log retry attempt
			if attempt < defaultMaxRetries {
				delay := calculateBackoff(attempt)
				logging.Debug("AI API request failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, defaultMaxRetries+1, err, delay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			continue
		}

		// Read response body
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr != nil {
			lastErr = readErr

			// Only retry if the read error is retryable
			if !isRetryableError(readErr, 0) {
				return nil, nil, fmt.Errorf("reading response body: %w", readErr)
			}

			if attempt < defaultMaxRetries {
				delay := calculateBackoff(attempt)
				logging.Debug("AI API response read failed (attempt %d/%d): %v, retrying in %v",
					attempt+1, defaultMaxRetries+1, readErr, delay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			continue
		}

		lastStatusCode = resp.StatusCode

		// Check for retryable status codes
		if isRetryableError(nil, resp.StatusCode) {
			lastErr = fmt.Errorf("API returned status %d", resp.StatusCode)

			if attempt < defaultMaxRetries {
				delay := calculateBackoff(attempt)
				logging.Debug("AI API returned status %d (attempt %d/%d), retrying in %v",
					resp.StatusCode, attempt+1, defaultMaxRetries+1, delay)

				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}
			continue
		}

		// Success or non-retryable error
		return resp, body, nil
	}

	// All retries exhausted
	if lastErr != nil {
		return nil, nil, fmt.Errorf("API request failed after %d attempts: %w", defaultMaxRetries+1, lastErr)
	}
	return nil, nil, fmt.Errorf("API request failed after %d attempts (status %d)", defaultMaxRetries+1, lastStatusCode)
}

// calculateBackoff returns the delay for a given retry attempt using exponential backoff with jitter.
func calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: baseDelay * 2^attempt
	delay := defaultBaseDelay * time.Duration(1<<attempt)

	// Cap at max delay
	if delay > defaultMaxDelay {
		delay = defaultMaxDelay
	}

	// Add jitter (±25% randomization to prevent thundering herd)
	jitter := time.Duration(rand.Int63n(int64(delay) / 2))
	delay = delay - delay/4 + jitter

	return delay
}

func (m *AITypeMapper) queryAnthropicAPI(ctx context.Context, prompt string) (string, error) {
	model := m.provider.GetEffectiveModel(m.providerType)

	maxTokens := 1024
	if len(prompt) > 500 {
		maxTokens = 4096
	}
	systemPrompt := anthropicSystemPromptFor(prompt)

	reqBody := anthropicRequest{
		Model:     model,
		MaxTokens: maxTokens,
		System:    systemPrompt,
		Messages: []anthropicMessage{
			{Role: "user", Content: prompt},
		},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	// Use retry logic for transient failures
	resp, body, err := m.retryableHTTPDo(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.anthropic.com/v1/messages", bytes.NewReader(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-api-key", m.provider.APIKey)
		req.Header.Set("anthropic-version", "2023-06-01")
		return req, nil
	})
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, sanitizeErrorResponse(body, 200))
	}

	var anthropicResp anthropicResponse
	if err := json.Unmarshal(body, &anthropicResp); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}

	if anthropicResp.Error != nil {
		return "", fmt.Errorf("API error: %s", anthropicResp.Error.Message)
	}

	if len(anthropicResp.Content) == 0 || anthropicResp.Content[0].Text == "" {
		return "", fmt.Errorf("empty response from API")
	}

	return anthropicResp.Content[0].Text, nil
}

func anthropicSystemPromptFor(prompt string) string {
	if len(prompt) <= 500 {
		return ""
	}
	upperPrompt := strings.ToUpper(prompt[:min(len(prompt), 1200)])
	switch {
	case strings.Contains(upperPrompt, "SQL DDL PARSER") ||
		strings.Contains(upperPrompt, "=== OUTPUT SCHEMA ==="):
		return "You are a SQL DDL parser. Return ONLY valid JSON. No markdown fences, no explanation outside the JSON."
	case strings.Contains(upperPrompt, "DATABASE MIGRATION AUDITOR") ||
		strings.Contains(upperPrompt, "=== AUDIT CRITERIA ==="):
		return "You are a database migration auditor. Return ONLY OK or ISSUES in the requested format. Do not rewrite SQL."
	case strings.Contains(upperPrompt, "RETURN ONLY VALID JSON"):
		return "You are a database migration tuning assistant. Return ONLY valid JSON. No markdown fences, no explanation outside the JSON."
	default:
		return ""
	}
}

// OpenAI API types
type openAIRequest struct {
	Model               string                 `json:"model"`
	Messages            []openAIMessage        `json:"messages"`
	MaxCompletionTokens int                    `json:"max_completion_tokens,omitempty"`
	MaxTokens           int                    `json:"max_tokens,omitempty"`
	Temperature         float64                `json:"temperature"`
	Options             map[string]interface{} `json:"options,omitempty"` // Provider-specific options (e.g., Ollama's num_ctx for context window size)
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"` // Reasoning/thinking models (e.g., Qwen3)
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	// Error is stored as RawMessage so that both shapes are accepted:
	//   OpenAI/Anthropic style: {"error": {"message": "...", "type": "..."}}
	//   LM Studio style:        {"error": "..."}
	// Without this, a string-shaped error blows up the entire response unmarshal,
	// turning a meaningful provider error message into "cannot unmarshal string
	// into Go struct field openAIResponse.error".
	Error json.RawMessage `json:"error,omitempty"`
}

// ErrorMessage extracts a human-readable error message from openAIResponse.Error,
// handling both the struct shape ({"message": "..."}) used by OpenAI/Anthropic
// and the bare-string shape ("...") used by LM Studio. Returns "" if there is
// no error in the response. All non-empty results pass through
// sanitizeErrorResponse for length capping and API-key redaction, matching the
// treatment given to non-200 response bodies elsewhere in this file.
func (r *openAIResponse) ErrorMessage() string {
	// Trim whitespace so " null\n" and similar are recognized as "no error".
	trimmed := bytes.TrimSpace(r.Error)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	// Try struct shape first.
	var asStruct struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(trimmed, &asStruct); err == nil && asStruct.Message != "" {
		return sanitizeErrorResponse([]byte(asStruct.Message), 200)
	}
	// Fall back to string shape.
	var asString string
	if err := json.Unmarshal(trimmed, &asString); err == nil && asString != "" {
		return sanitizeErrorResponse([]byte(asString), 200)
	}
	// Unknown shape — surface the raw JSON so the user can at least see it,
	// but truncated and key-redacted like every other error surface.
	return sanitizeErrorResponse(trimmed, 200)
}

func (m *AITypeMapper) queryOpenAIAPI(ctx context.Context, prompt string, url string) (string, error) {
	return m.queryOpenAIAPIWithTokens(ctx, prompt, url, 100)
}

// queryOpenAIAPIWithTokens queries OpenAI API with configurable max tokens.
func (m *AITypeMapper) queryOpenAIAPIWithTokens(ctx context.Context, prompt string, url string, maxTokens int) (string, error) {
	model := m.provider.GetEffectiveModel(m.providerType)

	// Detect if this is a type mapping query (short, simple) vs general AI query (long, complex)
	systemMsg := "You are a helpful AI assistant."
	isTypeMapping := len(prompt) < 500 && maxTokens <= 100
	if isTypeMapping {
		systemMsg = "You are a database type mapping expert. Respond with only the target type, no explanation."
	} else {
		// For complex queries, use the provider's configured max tokens
		maxTokens = m.provider.GetEffectiveMaxTokens(m.providerType)
	}

	reqBody := openAIRequest{
		Model: model,
		Messages: []openAIMessage{
			{Role: "system", Content: systemMsg},
			{Role: "user", Content: prompt},
		},
		MaxCompletionTokens: maxTokens,
		Temperature:         m.provider.GetEffectiveModelTemperature(),
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	// Use retry logic for transient failures
	resp, body, err := m.retryableHTTPDo(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+m.provider.APIKey)
		return req, nil
	})
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, sanitizeErrorResponse(body, 200))
	}

	var openAIResp openAIResponse
	if err := json.Unmarshal(body, &openAIResp); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}

	if msg := openAIResp.ErrorMessage(); msg != "" {
		return "", fmt.Errorf("API error: %s", msg)
	}

	if len(openAIResp.Choices) == 0 {
		return "", fmt.Errorf("empty response from API")
	}

	content := openAIResp.Choices[0].Message.Content
	if content == "" {
		if openAIResp.Choices[0].Message.ReasoningContent != "" {
			return "", fmt.Errorf("model used all tokens on reasoning with no output — try increasing max_tokens or using a non-reasoning model")
		}
		return "", fmt.Errorf("empty response from API")
	}

	return content, nil
}

// queryOpenAICompatAPI queries local providers using OpenAI-compatible API (no auth required).
func (m *AITypeMapper) queryOpenAICompatAPI(ctx context.Context, prompt string, url string) (string, error) {
	return m.queryOpenAICompatAPIWithTokens(ctx, prompt, url, 100)
}

// queryOpenAICompatAPIWithTokens queries local providers with configurable max tokens.
func (m *AITypeMapper) queryOpenAICompatAPIWithTokens(ctx context.Context, prompt string, url string, maxTokens int) (string, error) {
	model := m.provider.GetEffectiveModel(m.providerType)

	// Detect if this is a type mapping query (short, simple) vs general AI query (long, complex)
	systemMsg := "You are a helpful AI assistant."
	isTypeMapping := len(prompt) < 500 && maxTokens <= 100
	if isTypeMapping {
		systemMsg = "You are a database type mapping expert. Respond with only the target type, no explanation."
	}

	// For complex queries, use the provider's configured max tokens.
	// Reasoning models (e.g., Qwen3) consume tokens on thinking before generating,
	// so they need significantly more headroom.
	if !isTypeMapping {
		maxTokens = m.provider.GetEffectiveMaxTokens(m.providerType)
	}

	reqBody := openAIRequest{
		Model: model,
		Messages: []openAIMessage{
			{Role: "system", Content: systemMsg},
			{Role: "user", Content: prompt},
		},
		MaxCompletionTokens: maxTokens,
		Temperature:         m.provider.GetEffectiveModelTemperature(),
	}

	// For local providers (Ollama/LMStudio), use max_tokens (older OpenAI-compatible API)
	if AIProvider(m.providerType) == ProviderOllama || AIProvider(m.providerType) == ProviderLMStudio {
		reqBody.MaxTokens = reqBody.MaxCompletionTokens
		reqBody.MaxCompletionTokens = 0
	}
	if AIProvider(m.providerType) == ProviderOllama {
		contextWindow := m.provider.GetEffectiveContextWindow()
		reqBody.Options = map[string]interface{}{
			"num_ctx": contextWindow, // Use configured context window (default: 8192)
		}
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	providerName := m.providerName // capture YAML key for log identity in retry callback

	// Use retry logic for transient failures
	resp, body, err := m.retryableHTTPDo(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		// No Authorization header for local providers
		return req, nil
	})
	if err != nil {
		return "", fmt.Errorf("API request failed (is %s running?): %w", providerName, err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, sanitizeErrorResponse(body, 200))
	}

	var openAIResp openAIResponse
	if err := json.Unmarshal(body, &openAIResp); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}

	if msg := openAIResp.ErrorMessage(); msg != "" {
		return "", fmt.Errorf("API error: %s", msg)
	}

	if len(openAIResp.Choices) == 0 {
		return "", fmt.Errorf("empty response from API")
	}

	content := openAIResp.Choices[0].Message.Content
	if content == "" {
		// Reasoning models (e.g., Qwen3) may put all output in reasoning_content
		// and leave content empty when max_tokens is too low for both thinking + output.
		if openAIResp.Choices[0].Message.ReasoningContent != "" {
			return "", fmt.Errorf("model used all tokens on reasoning with no output — try increasing max_tokens or using a non-reasoning model")
		}
		return "", fmt.Errorf("empty response from API")
	}

	return content, nil
}

// Gemini API types
type geminiRequest struct {
	Contents         []geminiContent `json:"contents"`
	GenerationConfig geminiGenConfig `json:"generationConfig"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text string `json:"text"`
}

type geminiGenConfig struct {
	MaxOutputTokens int     `json:"maxOutputTokens"`
	Temperature     float64 `json:"temperature"`
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (m *AITypeMapper) queryGeminiAPI(ctx context.Context, prompt string) (string, error) {
	// Short prompts (type mapping) need ~100 tokens; complex prompts
	// (DDL generation, smart config) need the provider's configured max.
	// Gemini 3+ models use thinking tokens, so they need more headroom.
	maxTokens := 100
	if len(prompt) > 500 {
		maxTokens = m.provider.GetEffectiveMaxTokens(m.providerType)
		if maxTokens < 8192 {
			maxTokens = 8192 // Gemini thinking models need headroom
		}
	}

	reqBody := geminiRequest{
		Contents: []geminiContent{
			{
				Parts: []geminiPart{
					{Text: prompt},
				},
			},
		},
		GenerationConfig: geminiGenConfig{
			MaxOutputTokens: maxTokens,
			Temperature:     m.provider.GetEffectiveModelTemperature(),
		},
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	model := m.provider.GetEffectiveModel(m.providerType)
	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", model)

	// Use retry logic for transient failures
	resp, body, err := m.retryableHTTPDo(ctx, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-goog-api-key", m.provider.APIKey)
		return req, nil
	})
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API returned status %d: %s", resp.StatusCode, sanitizeErrorResponse(body, 200))
	}

	var geminiResp geminiResponse
	if err := json.Unmarshal(body, &geminiResp); err != nil {
		return "", fmt.Errorf("parsing response: %w", err)
	}

	if geminiResp.Error != nil {
		return "", fmt.Errorf("API error: %s", geminiResp.Error.Message)
	}

	if len(geminiResp.Candidates) == 0 ||
		len(geminiResp.Candidates[0].Content.Parts) == 0 ||
		geminiResp.Candidates[0].Content.Parts[0].Text == "" {
		return "", fmt.Errorf("empty response from API")
	}

	return geminiResp.Candidates[0].Content.Parts[0].Text, nil
}

// TypeMappingCache stores AI-generated type mappings.
type TypeMappingCache struct {
	mu       sync.RWMutex
	mappings map[string]string
}

// NewTypeMappingCache creates a new empty cache.
func NewTypeMappingCache() *TypeMappingCache {
	return &TypeMappingCache{
		mappings: make(map[string]string),
	}
}

// Get retrieves a cached mapping.
func (c *TypeMappingCache) Get(key string) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.mappings[key]
	return val, ok
}

// Set stores a mapping in the cache.
func (c *TypeMappingCache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mappings[key] = value
}

// All returns all cached mappings.
func (c *TypeMappingCache) All() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]string, len(c.mappings))
	for k, v := range c.mappings {
		result[k] = v
	}
	return result
}

// Load populates the cache from a map.
func (c *TypeMappingCache) Load(mappings map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range mappings {
		c.mappings[k] = v
	}
}

func (m *AITypeMapper) loadCache() error {
	data, err := os.ReadFile(m.cacheFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading cache file: %w", err)
	}

	var mappings map[string]string
	if err := json.Unmarshal(data, &mappings); err != nil {
		return fmt.Errorf("parsing cache file: %w", err)
	}

	m.cache.Load(mappings)
	logging.Debug("Loaded %d AI type mappings from cache", len(mappings))
	return nil
}

func (m *AITypeMapper) saveCache() error {
	dir := filepath.Dir(m.cacheFile)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}

	// Hold cacheMu.Lock across the whole save. Two protections at once:
	// (1) snapshot the in-memory cache atomically, and (2) serialize
	// concurrent saves so they don't race on the temp-file rename below.
	// Required after the requestsMu removal in this PR — without it,
	// concurrent goroutines (now allowed by AIConcurrency >1) would
	// race writing the same JSON file.
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	mappings := m.cache.All()
	data, err := json.MarshalIndent(mappings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling cache: %w", err)
	}

	// Atomic write: write to a temp file, then rename. The rename is
	// atomic on POSIX, so the cache file is always either the old
	// content or the new content — never a partial / interleaved write.
	tempFile := m.cacheFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0600); err != nil {
		return fmt.Errorf("writing cache temp file: %w", err)
	}
	if err := os.Rename(tempFile, m.cacheFile); err != nil {
		_ = os.Remove(tempFile) // best-effort cleanup
		return fmt.Errorf("renaming cache temp file into place: %w", err)
	}
	return nil
}

// CacheSize returns the number of cached mappings.
func (m *AITypeMapper) CacheSize() int {
	return len(m.cache.All())
}

// ClearCache removes all cached mappings.
func (m *AITypeMapper) ClearCache() error {
	m.cacheMu.Lock()
	m.cache = NewTypeMappingCache()
	m.cacheMu.Unlock()

	if err := os.Remove(m.cacheFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing cache file: %w", err)
	}
	return nil
}

// ExportCache exports cached mappings for review or sharing.
func (m *AITypeMapper) ExportCache(w io.Writer) error {
	mappings := m.cache.All()
	data, err := json.MarshalIndent(mappings, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling cache: %w", err)
	}
	_, err = w.Write(data)
	return err
}

// CallAI sends a prompt to the configured AI provider and returns the response.
// This is a generic method for arbitrary prompts (not just type mapping).
//
// Concurrency: safe for concurrent use; callers should bound concurrency
// as appropriate for their workload. The orchestrator's create phases
// use Migration.AIConcurrency for that purpose; other callers
// (e.g. AIErrorDiagnoser) currently invoke CallAI from a single
// goroutine. The HTTP layer's 429 retry handles provider-side rate
// limits regardless of who's calling.
func (m *AITypeMapper) CallAI(ctx context.Context, prompt string) (string, error) {

	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), time.Duration(m.timeoutSeconds)*time.Second)
		defer cancel()
	}

	var result string
	var err error

	switch AIProvider(m.providerType) {
	case ProviderAnthropic:
		result, err = m.queryAnthropicAPI(ctx, prompt)
	case ProviderOpenAI:
		result, err = m.queryOpenAIAPI(ctx, prompt, "https://api.openai.com/v1/chat/completions")
	case ProviderGoogle, ProviderGemini:
		result, err = m.queryGeminiAPI(ctx, prompt)
	case ProviderOllama:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerType)
		result, err = m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	case ProviderLMStudio:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerType)
		result, err = m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	default:
		if m.provider.BaseURL != "" {
			result, err = m.queryOpenAICompatAPI(ctx, prompt, m.provider.BaseURL+"/v1/chat/completions")
		} else {
			return "", fmt.Errorf("unsupported AI provider: %s (dispatch type %q)", m.providerName, m.providerType)
		}
	}

	return result, err
}

// ProviderName returns the YAML key of the configured provider entry —
// useful for log identity and error messages. For dispatch decisions
// (which API client to invoke, IsLocal etc.) use providerType internally.
func (m *AITypeMapper) ProviderName() string {
	return m.providerName
}

// TimeoutSeconds returns the configured API timeout.
func (m *AITypeMapper) TimeoutSeconds() int {
	return m.timeoutSeconds
}

// IsLocalProvider returns true if the provider runs inference locally
// (Ollama or LMStudio) rather than calling a cloud API.
func IsLocalProvider(providerName string) bool {
	return providerName == string(ProviderOllama) || providerName == string(ProviderLMStudio)
}

// Model returns the model being used.
func (m *AITypeMapper) Model() string {
	return m.provider.GetEffectiveModel(m.providerType)
}

// truncateString truncates a string to maxLen and adds "..." if truncated.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// stripMarkdownFence removes a leading ```lang fence and matching trailing
// ``` from an AI response. Local models (qwen, gpt-oss, llama) frequently
// wrap DDL in markdown despite explicit "no markdown" prompt instructions.
// No-op when the response isn't fenced. Idempotent.
func stripMarkdownFence(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	if nl := strings.IndexByte(s, '\n'); nl >= 0 {
		s = s[nl+1:]
	} else {
		return s
	}
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, "```")
	return strings.TrimSpace(s)
}
