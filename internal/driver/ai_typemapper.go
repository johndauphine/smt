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

	"smt/internal/ident"
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

func init() {
	// Seed the random number generator for jitter in backoff calculations.
	// Go 1.20+ seeds automatically, but this ensures compatibility with older versions.
	rand.Seed(time.Now().UnixNano())
}

// AIProvider represents supported AI providers for type mapping.
type AIProvider string

const (
	// ProviderAnthropic uses Anthropic's Claude API.
	ProviderAnthropic AIProvider = "anthropic"
	// ProviderOpenAI uses OpenAI's API.
	ProviderOpenAI AIProvider = "openai"
	// ProviderGemini uses Google's Gemini API.
	ProviderGemini AIProvider = "gemini"
	// ProviderOllama uses local Ollama with OpenAI-compatible API.
	ProviderOllama AIProvider = "ollama"
	// ProviderLMStudio uses local LM Studio with OpenAI-compatible API.
	ProviderLMStudio AIProvider = "lmstudio"
)

// ValidAIProviders returns the list of supported AI provider names.
func ValidAIProviders() []string {
	return []string{
		string(ProviderAnthropic),
		string(ProviderOpenAI),
		string(ProviderGemini),
		string(ProviderOllama),
		string(ProviderLMStudio),
	}
}

// IsValidAIProvider returns true if the provider name is valid (case-insensitive).
func IsValidAIProvider(provider string) bool {
	switch AIProvider(strings.ToLower(provider)) {
	case ProviderAnthropic, ProviderOpenAI, ProviderGemini, ProviderOllama, ProviderLMStudio:
		return true
	}
	return false
}

// NormalizeAIProvider returns the canonical lowercase provider name.
// Returns empty string if the provider is invalid.
func NormalizeAIProvider(provider string) string {
	normalized := strings.ToLower(provider)
	if IsValidAIProvider(normalized) {
		return normalized
	}
	return ""
}

// AITypeMapper uses AI to map database types.
// It implements the TypeMapper interface.
type AITypeMapper struct {
	providerName string
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

	// Validate cloud providers have API key
	if !secrets.IsLocalProvider(providerName) && provider.APIKey == "" {
		return nil, fmt.Errorf("AI provider %q requires an API key", providerName)
	}

	// Get effective model
	model := provider.GetEffectiveModel(providerName)
	if model == "" {
		return nil, fmt.Errorf("no model specified for provider %q", providerName)
	}

	// Set up cache file
	homeDir, _ := os.UserHomeDir()
	cacheFile := filepath.Join(homeDir, ".smt", "type-cache.json")

	// Determine API timeout: user-configured > local provider default > cloud default.
	// Local providers and thinking models need more time for inference.
	timeoutSec := 60
	if IsLocalProvider(providerName) {
		timeoutSec = 120
	}
	if provider.TimeoutSeconds > 0 {
		timeoutSec = provider.TimeoutSeconds
	}

	mapper := &AITypeMapper{
		providerName: providerName,
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

// MapType maps a source type to the target type using AI.
// This method is safe to call concurrently - it uses in-flight request tracking
// to avoid duplicate API calls for the same type.
// Note: For table-level DDL generation, use GenerateTableDDL instead.
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
	switch AIProvider(m.providerName) {
	case ProviderAnthropic:
		return m.queryAnthropicAPI(ctx, prompt)
	case ProviderOpenAI:
		return m.queryOpenAIAPI(ctx, prompt, "https://api.openai.com/v1/chat/completions")
	case ProviderGemini:
		return m.queryGeminiAPI(ctx, prompt)
	case ProviderOllama:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerName)
		return m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	case ProviderLMStudio:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerName)
		return m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	default:
		// Unknown providers can ride the OpenAI-compatible endpoint if
		// they configured a base_url (covers vLLM, llama.cpp server, etc.).
		if m.provider.BaseURL != "" {
			return m.queryOpenAICompatAPI(ctx, prompt, m.provider.BaseURL+"/v1/chat/completions")
		}
		return "", fmt.Errorf("unsupported AI provider: %s", m.providerName)
	}
}

// maxSampleValueLen is the maximum length of a single sample value in prompts.
const maxSampleValueLen = 100

// maxSamplesInPrompt is the maximum number of sample values to include in prompts.
const maxSamplesInPrompt = 5

// maxTotalSampleBytes is the maximum total bytes of sample data to include.
const maxTotalSampleBytes = 500

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
	model := m.provider.GetEffectiveModel(m.providerName)

	// Detect if this is a type mapping query (short, simple) vs a complex query.
	// DDL generation prompts need raw SQL output, while AI monitor/smart config
	// prompts need structured JSON output. Use prompt content to distinguish.
	maxTokens := 1024
	systemPrompt := ""
	if len(prompt) > 500 {
		maxTokens = 4096
		upperPrompt := strings.ToUpper(prompt[:min(len(prompt), 200)])
		isDDL := strings.Contains(upperPrompt, "CREATE TABLE") ||
			strings.Contains(upperPrompt, "CREATE INDEX") ||
			strings.Contains(upperPrompt, "ALTER TABLE") ||
			strings.Contains(upperPrompt, "DROP TABLE")
		if isDDL {
			systemPrompt = "You are a database migration expert. Return ONLY the raw SQL statement. No JSON, no markdown, no explanation."
		} else {
			systemPrompt = "You are a database migration tuning assistant. Return ONLY valid JSON. No markdown fences, no explanation outside the JSON."
		}
	}

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
	model := m.provider.GetEffectiveModel(m.providerName)

	// Detect if this is a type mapping query (short, simple) vs general AI query (long, complex)
	systemMsg := "You are a helpful AI assistant."
	isTypeMapping := len(prompt) < 500 && maxTokens <= 100
	if isTypeMapping {
		systemMsg = "You are a database type mapping expert. Respond with only the target type, no explanation."
	} else {
		// For complex queries, use the provider's configured max tokens
		maxTokens = m.provider.GetEffectiveMaxTokens(m.providerName)
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
	model := m.provider.GetEffectiveModel(m.providerName)

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
		maxTokens = m.provider.GetEffectiveMaxTokens(m.providerName)
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
	if AIProvider(m.providerName) == ProviderOllama || AIProvider(m.providerName) == ProviderLMStudio {
		reqBody.MaxTokens = reqBody.MaxCompletionTokens
		reqBody.MaxCompletionTokens = 0
	}
	if AIProvider(m.providerName) == ProviderOllama {
		contextWindow := m.provider.GetEffectiveContextWindow()
		reqBody.Options = map[string]interface{}{
			"num_ctx": contextWindow, // Use configured context window (default: 8192)
		}
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshaling request: %w", err)
	}

	providerName := m.providerName // capture for closure

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
		maxTokens = m.provider.GetEffectiveMaxTokens(m.providerName)
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

	model := m.provider.GetEffectiveModel(m.providerName)
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

	switch AIProvider(m.providerName) {
	case ProviderAnthropic:
		result, err = m.queryAnthropicAPI(ctx, prompt)
	case ProviderOpenAI:
		result, err = m.queryOpenAIAPI(ctx, prompt, "https://api.openai.com/v1/chat/completions")
	case ProviderGemini:
		result, err = m.queryGeminiAPI(ctx, prompt)
	case ProviderOllama:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerName)
		result, err = m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	case ProviderLMStudio:
		baseURL := m.provider.GetEffectiveBaseURL(m.providerName)
		result, err = m.queryOpenAICompatAPI(ctx, prompt, baseURL+"/v1/chat/completions")
	default:
		if m.provider.BaseURL != "" {
			result, err = m.queryOpenAICompatAPI(ctx, prompt, m.provider.BaseURL+"/v1/chat/completions")
		} else {
			return "", fmt.Errorf("unsupported AI provider: %s", m.providerName)
		}
	}

	return result, err
}

// ProviderName returns the name of the configured provider.
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
	return m.provider.GetEffectiveModel(m.providerName)
}

// GenerateTableDDL generates complete CREATE TABLE DDL for the target database.
// This method provides full table context to the AI for smarter type mapping decisions.
func (m *AITypeMapper) GenerateTableDDL(ctx context.Context, req TableDDLRequest) (*TableDDLResponse, error) {
	if req.SourceTable == nil {
		return nil, fmt.Errorf("SourceTable is required")
	}
	if req.SourceDBType == "" {
		return nil, fmt.Errorf("SourceDBType is required")
	}
	if req.TargetDBType == "" {
		return nil, fmt.Errorf("TargetDBType is required")
	}

	// Build cache key based on table structure. Cache is consulted only on
	// first-try calls; retry calls (PreviousAttempt != nil) bypass the cache
	// in both directions — see TableDDLRequest.PreviousAttempt for rationale.
	cacheKey := m.tableCacheKey(req)
	isRetry := req.PreviousAttempt != nil

	if !isRetry {
		m.cacheMu.RLock()
		if cached, ok := m.cache.Get(cacheKey); ok {
			m.cacheMu.RUnlock()
			return m.parseTableDDLFromCache(cached, req.SourceTable)
		}
		m.cacheMu.RUnlock()
	}

	// Build the prompt with full table context
	prompt := m.buildTableDDLPrompt(req)

	logging.Debug("AI table DDL generation: %s.%s (%s -> %s, retry=%v)\n--- PROMPT ---\n%s\n--- END PROMPT ---",
		req.SourceTable.Schema, req.SourceTable.Name, req.SourceDBType, req.TargetDBType, isRetry, prompt)

	// Call AI API
	result, err := m.CallAI(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("AI table DDL generation failed for %s.%s: %w",
			req.SourceTable.Schema, req.SourceTable.Name, err)
	}

	// Parse the response to extract DDL
	response, err := m.parseTableDDLResponse(result, req.SourceTable)
	if err != nil {
		return nil, fmt.Errorf("failed to parse AI response for %s.%s: %w",
			req.SourceTable.Schema, req.SourceTable.Name, err)
	}

	// Cache writes are deferred to the writer's CacheTableDDL call after the
	// database confirms the DDL executes — see #32. Caching here would persist
	// AI output that hasn't been validated against the target DB; if the DDL
	// then fails (and the AI-classifier can't fix it on retry), the bad DDL
	// would stay cached and poison every subsequent run for the same table
	// shape with no chance to re-call the AI. Mapper is now read-only on the
	// cache; only validated DDL ever gets stored.

	logging.Debug("AI generated DDL for %s.%s (%d columns mapped, retry=%v)",
		req.SourceTable.Schema, req.SourceTable.Name, len(response.ColumnTypes), isRetry)

	return response, nil
}

// CacheTableDDL stores a known-good DDL for the request, replacing any prior
// cached value. This is the ONLY entry point that writes to the AI table-DDL
// cache (#32) — the mapper itself never caches because it only sees AI output,
// not validated DDL. The writer calls this after every successful CREATE TABLE
// execution (first-try success and retry success alike); a failed exec leaves
// the cache untouched so the next call gets a fresh AI invocation rather than
// a poisoned hit.
func (m *AITypeMapper) CacheTableDDL(req TableDDLRequest, ddl string) {
	cacheKey := m.tableCacheKey(req)
	m.cacheMu.Lock()
	m.cache.Set(cacheKey, ddl)
	m.cacheMu.Unlock()
	if err := m.saveCache(); err != nil {
		logging.Warn("Failed to save AI table DDL cache: %v", err)
	}
}

// tableCacheKey generates a cache key for table-level DDL.
// Uses a hash of the table structure to handle schema changes.
func (m *AITypeMapper) tableCacheKey(req TableDDLRequest) string {
	// Build a deterministic representation of the table structure
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("table:%s:%s:%s:%s.%s:",
		req.SourceDBType, req.TargetDBType, req.TargetSchema, req.SourceTable.Schema, req.SourceTable.Name))

	for _, col := range req.SourceTable.Columns {
		sb.WriteString(fmt.Sprintf("%s:%s:%d:%d:%d:%v:%v:%s:%v:%s:%v;",
			col.Name, col.DataType, col.MaxLength, col.Precision, col.Scale, col.IsNullable,
			col.IsIdentity, col.DefaultExpression, col.IsComputed, col.ComputedExpression, col.ComputedPersisted))
	}

	// Add PK info
	sb.WriteString("pk:")
	for _, pk := range req.SourceTable.PrimaryKey {
		sb.WriteString(pk + ",")
	}

	return sb.String()
}

// buildTableDDLPrompt creates the AI prompt for table-level DDL generation.
func (m *AITypeMapper) buildTableDDLPrompt(req TableDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate a CREATE TABLE statement.\n")
	sb.WriteString("IMPORTANT: Your entire response must be ONLY the raw SQL statement. No JSON, no markdown, no explanation.\n\n")

	// === SOURCE DATABASE CONTEXT ===
	sb.WriteString("=== SOURCE DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.SourceDBType))
	if req.SourceContext != nil {
		m.writeContextDetails(&sb, req.SourceContext, "Source")
	}
	sb.WriteString("\n")

	// === TARGET DATABASE CONTEXT ===
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		m.writeContextDetails(&sb, req.TargetContext, "Target")
	}
	sb.WriteString("\n")

	// === SOURCE TABLE DDL ===
	// SMT does not maintain Go-side per-dialect rendering rules. The model
	// already knows source-dialect semantics from its training; we hand it
	// the introspection facts (data_type, max_length, precision, scale,
	// nullability, identity, default, computed expression) and ask for the
	// target-dialect CREATE TABLE in one step. No source-DDL middleman.
	sb.WriteString(buildSourceIntrospectionBlock(req.SourceTable, req.SourceDBType))
	sb.WriteString("\n")

	// === REQUIRED TARGET COLUMN NAMES ===
	// Provide the exact column names the AI must use in the target DDL.
	// These match what the data transfer phase will use, preventing mismatches.
	sb.WriteString("=== REQUIRED TARGET COLUMN NAMES ===\n")
	sb.WriteString("You MUST use exactly these column names in the target CREATE TABLE. Do NOT modify, abbreviate, add, or remove any characters:\n")
	for _, col := range req.SourceTable.Columns {
		tgt := targetIdentifier(col.Name, req.TargetDBType)
		sb.WriteString(fmt.Sprintf("  %s -> %s\n", col.Name, tgt))
	}
	sb.WriteString("\n")

	// === MIGRATION RULES ===
	sb.WriteString("=== MIGRATION RULES ===\n")
	m.writeMigrationRules(&sb, req)

	// === OUTPUT REQUIREMENTS ===
	sb.WriteString("\n=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete CREATE TABLE statement for the target database.\n")
	targetTableName := targetIdentifier(req.SourceTable.Name, req.TargetDBType)
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("- Use fully qualified table name: %s.%s\n", req.TargetSchema, targetTableName))
	} else {
		sb.WriteString(fmt.Sprintf("- Use table name: %s\n", targetTableName))
	}
	sb.WriteString("- Use the EXACT column names from the REQUIRED TARGET COLUMN NAMES section above\n")
	sb.WriteString("- Include all columns with appropriate target types\n")

	sb.WriteString("- Preserve nullability exactly as reported in the introspection metadata — emit NOT NULL when nullable=false, allow NULL when nullable=true. SMT migrates schema, not data; do not relax nullability for loading flexibility.\n")
	sb.WriteString("- Primary key columns must be NOT NULL\n")
	sb.WriteString("- Include PRIMARY KEY constraint\n")
	sb.WriteString("- Do NOT include foreign keys (created separately in Finalize)\n")
	sb.WriteString("- Do NOT include indexes (created separately in Finalize)\n")
	sb.WriteString("- Do NOT include CHECK constraints (created separately in Finalize)\n")
	sb.WriteString("- Return ONLY the raw CREATE TABLE SQL statement as plain text\n")
	sb.WriteString("- Do NOT wrap the response in JSON, markdown code blocks, or any other format\n")
	sb.WriteString("- The response must start with 'CREATE TABLE' and end with a semicolon\n")

	// Database-specific identifier requirements from the target dialect
	if dialect := GetDialect(req.TargetDBType); dialect != nil {
		if aug := dialect.AIPromptAugmentation(); aug != "" {
			sb.WriteString(aug)
		}
	}

	// Check for reserved words in source table columns
	reservedWords := m.findReservedWords(req.SourceTable, req.TargetDBType)
	if len(reservedWords) > 0 {
		sb.WriteString("\nWARNING: The following source columns are reserved words in the target database:\n")
		for _, rw := range reservedWords {
			switch req.TargetDBType {
			case "mssql":
				sb.WriteString(fmt.Sprintf("- Column '%s' must be quoted as [%s]\n", rw, rw))
			case "mysql":
				sb.WriteString(fmt.Sprintf("- Column '%s' must be quoted as `%s`\n", rw, rw))
			case "postgres":
				sb.WriteString(fmt.Sprintf("- Column '%s' must be quoted as \"%s\"\n", rw, strings.ToLower(rw)))
			}
		}
	}

	// PRIOR ATTEMPT FAILED — last in the prompt so the corrective context is the
	// most recent thing the model sees before generating. Only present when this
	// is a retry call (PreviousAttempt != nil); see #29 for the validate-and-retry
	// design. Verbatim DDL + verbatim error give the model the exact ground truth
	// of what it produced and what the database said about it.
	if req.PreviousAttempt != nil {
		sb.WriteString("\n=== PRIOR ATTEMPT FAILED ===\n")
		sb.WriteString("The previous CREATE TABLE you generated was rejected by the target database.\n\n")
		sb.WriteString("Previous DDL (verbatim):\n")
		sb.WriteString(req.PreviousAttempt.DDL)
		sb.WriteString("\n\nDatabase error (verbatim):\n")
		sb.WriteString(req.PreviousAttempt.Error)
		sb.WriteString("\n\nGenerate a corrected CREATE TABLE for the same source table. Use the same target column names, types, and constraints — only fix what the error indicates is wrong. Do not regenerate from scratch.\n")
		writeRetryClassificationInstruction(&sb)
	}

	return sb.String()
}

// writeContextDetails writes database context details to the prompt.
func (m *AITypeMapper) writeContextDetails(sb *strings.Builder, ctx *DatabaseContext, label string) {
	if ctx.Version != "" {
		sb.WriteString(fmt.Sprintf("Version: %s\n", ctx.Version))
	}
	if ctx.DatabaseName != "" {
		sb.WriteString(fmt.Sprintf("Database: %s\n", ctx.DatabaseName))
	}

	// Character encoding section
	sb.WriteString("Character Encoding:\n")
	if ctx.Charset != "" {
		sb.WriteString(fmt.Sprintf("  Charset: %s\n", ctx.Charset))
	}
	if ctx.NationalCharset != "" {
		sb.WriteString(fmt.Sprintf("  National Charset: %s\n", ctx.NationalCharset))
	}
	if ctx.Encoding != "" {
		sb.WriteString(fmt.Sprintf("  Encoding: %s\n", ctx.Encoding))
	}
	if ctx.CodePage > 0 {
		sb.WriteString(fmt.Sprintf("  Code Page: %d\n", ctx.CodePage))
	}
	if ctx.Collation != "" {
		sb.WriteString(fmt.Sprintf("  Collation: %s\n", ctx.Collation))
	}
	if ctx.BytesPerChar > 0 {
		sb.WriteString(fmt.Sprintf("  Max Bytes Per Char: %d\n", ctx.BytesPerChar))
	}

	// Case sensitivity section
	sb.WriteString("Case Sensitivity:\n")
	if ctx.IdentifierCase != "" {
		sb.WriteString(fmt.Sprintf("  Identifier Case: %s\n", ctx.IdentifierCase))
	}
	if ctx.CaseSensitiveIdentifiers {
		sb.WriteString("  Identifiers: case-sensitive\n")
	} else {
		sb.WriteString("  Identifiers: case-insensitive\n")
	}
	if ctx.CaseSensitiveData {
		sb.WriteString("  String Comparisons: case-sensitive\n")
	} else {
		sb.WriteString("  String Comparisons: case-insensitive (collation-dependent)\n")
	}

	// Limits section
	sb.WriteString("Limits:\n")
	if ctx.MaxIdentifierLength > 0 {
		sb.WriteString(fmt.Sprintf("  Max Identifier Length: %d\n", ctx.MaxIdentifierLength))
	}
	if ctx.MaxVarcharLength > 0 {
		sb.WriteString(fmt.Sprintf("  Max VARCHAR Length: %d\n", ctx.MaxVarcharLength))
	}
	if ctx.MaxNVarcharLength > 0 {
		sb.WriteString(fmt.Sprintf("  Max NVARCHAR Length: %d characters\n", ctx.MaxNVarcharLength))
	}
	if ctx.VarcharSemantics != "" {
		sb.WriteString(fmt.Sprintf("  VARCHAR Semantics: %s (lengths are in %ss)\n", ctx.VarcharSemantics, ctx.VarcharSemantics))
	}

	// Features section
	if ctx.StorageEngine != "" {
		sb.WriteString(fmt.Sprintf("Storage Engine: %s\n", ctx.StorageEngine))
	}
	if len(ctx.Features) > 0 {
		sb.WriteString(fmt.Sprintf("Features: %s\n", strings.Join(ctx.Features, ", ")))
	}
	if ctx.Notes != "" {
		sb.WriteString(fmt.Sprintf("Notes: %s\n", ctx.Notes))
	}
}

// writeMigrationRules writes migration guidance derived dynamically from database context.
// All rules are generated from runtime metadata - no hardcoded database-specific rules.
// Per-target dialect gotchas live in each driver's Dialect.AIPromptAugmentation, not here.
func (m *AITypeMapper) writeMigrationRules(sb *strings.Builder, req TableDDLRequest) {
	// Source database characteristics - derived from SourceContext
	sb.WriteString("Source database characteristics:\n")
	if req.SourceContext != nil {
		m.writeVarcharGuidance(sb, req.SourceContext, "source")
		m.writeEncodingGuidance(sb, req.SourceContext, "source")
	} else {
		sb.WriteString("- No source context available, using standard type semantics\n")
	}

	sb.WriteString("\n")

	// Target database rules - derived from TargetContext. Identifier rules
	// deliberately omitted: target column names are pre-resolved in
	// REQUIRED TARGET COLUMN NAMES, the target table name is given in
	// OUTPUT REQUIREMENTS, and each target dialect's AIPromptAugmentation
	// supplies any dialect-specific case-folding rules.
	sb.WriteString("Target database rules:\n")
	if req.TargetContext != nil {
		m.writeVarcharGuidance(sb, req.TargetContext, "target")
		m.writeEncodingGuidance(sb, req.TargetContext, "target")
		m.writeLimitsGuidance(sb, req.TargetContext)
	} else {
		sb.WriteString("- No target context available, use standard type mappings\n")
	}

	// Cross-database conversion guidance
	sb.WriteString("\nConversion guidance:\n")
	m.writeConversionGuidance(sb, req.SourceContext, req.TargetContext)

	// Type validity is the AI's job — SMT does not maintain a per-pair translation
	// table. State the invariant once: every emitted type must exist in the target.
	if Canonicalize(req.SourceDBType) != Canonicalize(req.TargetDBType) {
		sb.WriteString("\nType validity (MANDATORY):\n")
		sb.WriteString("- Every column type in the output CREATE TABLE must be a real, valid type in the target database. Do not pass through a source-only keyword (e.g. MSSQL NVARCHAR/NCHAR/NTEXT/UNIQUEIDENTIFIER/DATETIMEOFFSET, MySQL ENUM/SET/MEDIUMTEXT, PostgreSQL TEXT[]/JSONB).\n")
		sb.WriteString("- For each source type, choose the target's closest semantic equivalent in storage class, length, precision, signedness, timezone-awareness, and Unicode capability. If the target charset already provides Unicode storage, prefer the standard type over a national-character variant.\n")
		sb.WriteString("- If unsure whether a keyword is valid in the target, pick the safest standard SQL type that preserves semantics. Never invent a type name.\n")
	}

	// Reserved words note
	sb.WriteString("\nReserved words: If any column name is a SQL reserved word, quote it appropriately for the target database.\n")

	// Column-attribute preservation rules. The introspection block above
	// reports nullable, default_expression, computed/computed_expression for
	// every column that has them — these rules force the AI to carry them
	// through to the target instead of silently dropping them.
	sb.WriteString("\nColumn-attribute preservation:\n")
	sb.WriteString("- Preserve every NOT NULL constraint (nullable=false in the introspection metadata). Do not change a non-nullable column to nullable.\n")
	sb.WriteString("- Preserve every default_expression reported in the introspection metadata. Translate dialect-specific function defaults to the target's equivalent:\n")
	sb.WriteString("  * GETDATE() / GETUTCDATE() / SYSDATETIMEOFFSET() / SYSDATETIME() (MSSQL) -> CURRENT_TIMESTAMP (postgres/mysql)\n")
	sb.WriteString("  * NEWID() (MSSQL) -> gen_random_uuid() (postgres) -> UUID() (mysql)\n")
	sb.WriteString("  * Strip outer parentheses MSSQL adds around literal defaults: ((0)) -> 0, ((1)) -> 1, ('pending') -> 'pending'\n")
	sb.WriteString("  * Cast literals if the target type requires it (e.g. PG bit -> boolean: ((1)) -> true)\n")
	sb.WriteString("- Computed columns appear in the introspection metadata as `computed: true` with a `computed_expression` and `computed_storage` of STORED or VIRTUAL. Translate to the target's generated-column syntax:\n")
	sb.WriteString("  * postgres: <type> GENERATED ALWAYS AS (expression) STORED  (PG only supports STORED; if source is VIRTUAL, emit STORED)\n")
	sb.WriteString("  * mysql:    <type> GENERATED ALWAYS AS (expression) STORED  (or VIRTUAL — preserve the source storage hint)\n")
	sb.WriteString("  * mssql:    AS (expression) PERSISTED  (MSSQL infers type; persisted = STORED). Do NOT include the column type before AS — MSSQL forbids it on computed columns.\n")
	sb.WriteString("  * If the source data_type is reported as `(inferred)` (MSSQL computed columns may omit it), infer the type from the expression and source columns.\n")
	sb.WriteString("  * Translate dialect-specific functions inside the expression too (e.g. CAST(x AS VARCHAR(10)) is portable; ISNULL(a,b) (MSSQL) -> COALESCE(a,b))\n")
}

// capitalizeFirst returns the string with its first character uppercased.
// This replaces the deprecated strings.Title function.
func capitalizeFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// writeVarcharGuidance writes VARCHAR semantics guidance based on context.
func (m *AITypeMapper) writeVarcharGuidance(sb *strings.Builder, ctx *DatabaseContext, role string) {
	if ctx.VarcharSemantics == "" {
		return
	}

	if ctx.VarcharSemantics == "char" {
		sb.WriteString(fmt.Sprintf("- %s VARCHAR lengths are in CHARACTERS\n", capitalizeFirst(role)))
	} else if ctx.VarcharSemantics == "byte" {
		sb.WriteString(fmt.Sprintf("- %s VARCHAR lengths are in BYTES\n", capitalizeFirst(role)))
		if ctx.BytesPerChar > 1 {
			sb.WriteString(fmt.Sprintf("- Each character may take up to %d bytes\n", ctx.BytesPerChar))
		}
	}
}

// writeEncodingGuidance writes character encoding guidance based on context.
func (m *AITypeMapper) writeEncodingGuidance(sb *strings.Builder, ctx *DatabaseContext, role string) {
	if ctx.Charset != "" {
		sb.WriteString(fmt.Sprintf("- Character set: %s\n", ctx.Charset))
	}
	if ctx.BytesPerChar > 0 {
		sb.WriteString(fmt.Sprintf("- Max bytes per character: %d\n", ctx.BytesPerChar))
	}
	if ctx.Encoding != "" && ctx.Encoding != ctx.Charset {
		sb.WriteString(fmt.Sprintf("- Encoding: %s\n", ctx.Encoding))
	}
}

// writeLimitsGuidance writes database limits guidance based on context.
func (m *AITypeMapper) writeLimitsGuidance(sb *strings.Builder, ctx *DatabaseContext) {
	if ctx.MaxIdentifierLength > 0 {
		sb.WriteString(fmt.Sprintf("- Maximum identifier length: %d characters\n", ctx.MaxIdentifierLength))
	}
	if ctx.MaxVarcharLength > 0 {
		sb.WriteString(fmt.Sprintf("- Maximum VARCHAR length: %d\n", ctx.MaxVarcharLength))
		if ctx.VarcharSemantics == "byte" {
			sb.WriteString("- Use CLOB/TEXT equivalent for content exceeding max VARCHAR\n")
		}
	}
}

// writeConversionGuidance writes guidance for cross-database type conversion.
func (m *AITypeMapper) writeConversionGuidance(sb *strings.Builder, srcCtx, tgtCtx *DatabaseContext) {
	if srcCtx == nil || tgtCtx == nil {
		sb.WriteString("- Map types based on semantic equivalence\n")
		return
	}

	// VARCHAR semantics conversion
	if srcCtx.VarcharSemantics == "char" && tgtCtx.VarcharSemantics == "byte" {
		sb.WriteString("- Source VARCHAR/CHAR lengths are in CHARACTERS\n")
		sb.WriteString("- Target VARCHAR lengths are in BYTES (not characters)\n")
	} else if srcCtx.VarcharSemantics == "byte" && tgtCtx.VarcharSemantics == "char" {
		sb.WriteString("- Source uses BYTE lengths, target uses CHARACTER lengths\n")
		if srcCtx.BytesPerChar > 1 {
			sb.WriteString(fmt.Sprintf("- Source VARCHAR(n) with %d bytes/char = approximately n/%d characters\n", srcCtx.BytesPerChar, srcCtx.BytesPerChar))
		}
	} else if srcCtx.VarcharSemantics == "char" && tgtCtx.VarcharSemantics == "char" {
		sb.WriteString("- Both source and target use CHARACTER lengths - preserve lengths directly\n")
	}

	// Case handling guidance
	if srcCtx.IdentifierCase != tgtCtx.IdentifierCase && tgtCtx.IdentifierCase != "" {
		switch strings.ToLower(tgtCtx.IdentifierCase) {
		case "upper":
			sb.WriteString("- Convert all identifiers to UPPERCASE for target\n")
		case "lower":
			sb.WriteString("- Convert all identifiers to lowercase for target\n")
		}
	}
}

// findReservedWords checks source table columns for SQL reserved words.
func (m *AITypeMapper) findReservedWords(t *Table, targetDBType string) []string {
	// Common SQL reserved words that cause issues
	reservedWords := map[string]bool{
		"date": true, "time": true, "timestamp": true, "year": true, "month": true, "day": true,
		"user": true, "order": true, "group": true, "table": true, "index": true, "key": true,
		"type": true, "name": true, "value": true, "size": true, "number": true, "level": true,
		"comment": true, "desc": true, "asc": true, "limit": true, "offset": true,
		"select": true, "insert": true, "update": true, "delete": true, "from": true, "where": true,
		"and": true, "or": true, "not": true, "null": true, "true": true, "false": true,
		"primary": true, "foreign": true, "references": true, "constraint": true,
		"create": true, "alter": true, "drop": true, "truncate": true,
		"row": true, "rows": true, "column": true, "schema": true, "database": true,
		"function": true, "procedure": true, "trigger": true, "view": true,
		"id": false, // not reserved in most DBs
	}

	var found []string
	for _, col := range t.Columns {
		colLower := strings.ToLower(col.Name)
		if reservedWords[colLower] {
			found = append(found, col.Name)
		}
	}
	return found
}

// targetIdentifier returns the exact column/table name the transfer phase will
// use for the target database. Uses the shared ident.SanitizePG implementation
// so prompt-generated names always match what WriteBatch/CopyFrom expects.
func targetIdentifier(name, targetDBType string) string {
	if targetDBType != "postgres" {
		return name
	}
	return ident.SanitizePG(name)
}

// buildSourceIntrospectionBlock renders the source table's introspection
// metadata as a structured prompt section. The AI gets raw facts (data_type,
// max_length, precision, scale, nullability, identity, default expression,
// computed expression) and uses its own dialect knowledge to interpret them.
//
// Each column is emitted as a single JSON object on its own line. JSON is
// chosen over a comma-separated key:value form because default_expression
// and computed_expression are raw SQL fragments that can legitimately
// contain commas, quotes, parentheses, or newlines (e.g. literal defaults
// like 'a, b' or multi-line CHECK predicates) — encoding/json handles all
// the escaping. Models parse JSON-per-line fluently.
//
// Optional fields are omitted when zero/empty so the line stays compact.
// MSSQL's MAX-length sentinel (MaxLength=-1) is surfaced as the string
// "MAX" rather than -1 so the model doesn't have to know the sentinel
// convention.
func buildSourceIntrospectionBlock(t *Table, sourceDBType string) string {
	var sb strings.Builder

	sb.WriteString("=== SOURCE TABLE (introspection metadata) ===\n")
	sb.WriteString("source_dialect: ")
	sb.WriteString(sourceDBType)
	sb.WriteString("\n")
	if t.Schema != "" {
		sb.WriteString("schema: ")
		sb.WriteString(t.Schema)
		sb.WriteString("\n")
	}
	sb.WriteString("table: ")
	sb.WriteString(t.Name)
	sb.WriteString("\n\n")

	sb.WriteString("columns (in ordinal position, one JSON object per line):\n")
	for _, col := range t.Columns {
		row := columnIntrospection(col)
		// Marshal errors here would mean a bug in our own struct shape — fall
		// back to a literal { } string rather than panic; tests will catch it.
		buf, err := json.Marshal(row)
		if err != nil {
			sb.WriteString("  {}\n")
			continue
		}
		sb.WriteString("  ")
		sb.Write(buf)
		sb.WriteString("\n")
	}

	if len(t.PrimaryKey) > 0 {
		sb.WriteString("\nprimary_key: (")
		sb.WriteString(strings.Join(t.PrimaryKey, ", "))
		sb.WriteString(")\n")
	}

	return sb.String()
}

// columnIntrospection builds the per-column JSON-marshalable shape used in
// the introspection block. Optional attributes use omitempty so a non-identity
// column doesn't carry an "identity":false key, etc. — keeps the line short.
// MaxLength is rendered as a string ("MAX" or a decimal) so we can surface
// the MSSQL MaxLength=-1 sentinel as "MAX" without exposing the magic number.
func columnIntrospection(col Column) map[string]any {
	m := map[string]any{
		"name":     col.Name,
		"nullable": col.IsNullable,
	}
	if col.DataType == "" {
		// MSSQL computed columns can omit the declared type; surface that
		// explicitly so the model knows to infer it from computed_expression.
		m["data_type"] = "(inferred)"
	} else {
		m["data_type"] = col.DataType
	}
	switch {
	case col.MaxLength == -1:
		m["max_length"] = "MAX"
	case col.MaxLength > 0:
		m["max_length"] = col.MaxLength
	}
	if col.Precision > 0 {
		m["precision"] = col.Precision
	}
	if col.Scale > 0 {
		m["scale"] = col.Scale
	}
	if col.IsIdentity {
		m["identity"] = true
	}
	if col.DefaultExpression != "" {
		m["default_expression"] = col.DefaultExpression
	}
	if col.IsComputed {
		m["computed"] = true
		if col.ComputedExpression != "" {
			m["computed_expression"] = col.ComputedExpression
		}
		if col.ComputedPersisted {
			m["computed_storage"] = "STORED"
		} else {
			m["computed_storage"] = "VIRTUAL"
		}
	}
	return m
}

// parseTableDDLResponse extracts the DDL and column types from AI response.
// On retry calls (where the prompt invited a NOT_RETRYABLE classification),
// the model may emit the marker instead of DDL — that case is detected here
// and surfaced as ErrNotRetryable so the writer can break out of its retry
// loop with the original DB error preserved.
func (m *AITypeMapper) parseTableDDLResponse(response string, sourceTable *Table) (*TableDDLResponse, error) {
	if abort, reason := classifyRetryResponse(response); abort {
		return nil, WrapNotRetryable(reason)
	}

	ddl := strings.TrimSpace(response)

	// Basic validation - should start with CREATE TABLE
	upperDDL := strings.ToUpper(ddl)
	if !strings.HasPrefix(upperDDL, "CREATE TABLE") {
		return nil, fmt.Errorf("response does not contain valid CREATE TABLE statement: %s", truncateString(ddl, 100))
	}

	// Extract column types from DDL for reference
	columnTypes := m.extractColumnTypesFromDDL(ddl, sourceTable)

	return &TableDDLResponse{
		CreateTableDDL: ddl,
		ColumnTypes:    columnTypes,
		Notes:          "",
	}, nil
}

// parseTableDDLFromCache creates a response from cached DDL.
func (m *AITypeMapper) parseTableDDLFromCache(cachedDDL string, sourceTable *Table) (*TableDDLResponse, error) {
	columnTypes := m.extractColumnTypesFromDDL(cachedDDL, sourceTable)

	return &TableDDLResponse{
		CreateTableDDL: cachedDDL,
		ColumnTypes:    columnTypes,
		Notes:          "(from cache)",
	}, nil
}

// extractColumnTypesFromDDL attempts to extract column name -> type mappings from DDL.
// This is best-effort for logging/debugging purposes.
func (m *AITypeMapper) extractColumnTypesFromDDL(ddl string, sourceTable *Table) map[string]string {
	columnTypes := make(map[string]string)

	// Simple extraction: look for each source column name in the DDL
	for _, col := range sourceTable.Columns {
		// Look for patterns like: column_name TYPE or "column_name" TYPE
		patterns := []string{
			col.Name + " ",
			col.Name + "\t",
			`"` + col.Name + `" `,
			`"` + col.Name + `"	`,
			strings.ToUpper(col.Name) + " ",
			strings.ToLower(col.Name) + " ",
		}

		for _, pattern := range patterns {
			idx := strings.Index(ddl, pattern)
			if idx >= 0 {
				// Extract the type after the column name
				start := idx + len(pattern)
				rest := ddl[start:]

				// Find end of type (comma, newline, or closing paren)
				end := strings.IndexAny(rest, ",\n)")
				if end > 0 {
					typeStr := strings.TrimSpace(rest[:end])
					// Remove NOT NULL, NULL, etc.
					typeStr = strings.Split(typeStr, " NOT ")[0]
					typeStr = strings.Split(typeStr, " NULL")[0]
					typeStr = strings.Split(typeStr, " DEFAULT")[0]
					typeStr = strings.TrimSpace(typeStr)
					if typeStr != "" {
						columnTypes[col.Name] = typeStr
					}
				}
				break
			}
		}
	}

	return columnTypes
}

// truncateString truncates a string to maxLen and adds "..." if truncated.
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// GenerateFinalizationDDL generates DDL for indexes, foreign keys, or check constraints using AI.
func (m *AITypeMapper) GenerateFinalizationDDL(ctx context.Context, req FinalizationDDLRequest) (string, error) {
	if req.Table == nil {
		return "", fmt.Errorf("Table is required")
	}
	if req.TargetDBType == "" {
		return "", fmt.Errorf("TargetDBType is required")
	}

	var prompt string
	var entityName string
	var validatePrefix string

	switch req.Type {
	case DDLTypeIndex:
		if req.Index == nil {
			return "", fmt.Errorf("Index is required for DDLTypeIndex")
		}
		prompt = m.buildIndexDDLPrompt(req)
		entityName = req.Index.Name
		// validatePrefix not used for index - has custom validation below
		logging.Debug("AI index DDL generation: %s on %s.%s (%s)",
			req.Index.Name, req.TargetSchema, req.Table.Name, req.TargetDBType)

	case DDLTypeForeignKey:
		if req.ForeignKey == nil {
			return "", fmt.Errorf("ForeignKey is required for DDLTypeForeignKey")
		}
		prompt = m.buildForeignKeyDDLPrompt(req)
		entityName = req.ForeignKey.Name
		validatePrefix = "ALTER TABLE"
		logging.Debug("AI FK DDL generation: %s on %s.%s (%s)",
			req.ForeignKey.Name, req.TargetSchema, req.Table.Name, req.TargetDBType)

	case DDLTypeCheckConstraint:
		if req.CheckConstraint == nil {
			return "", fmt.Errorf("CheckConstraint is required for DDLTypeCheckConstraint")
		}
		prompt = m.buildCheckConstraintDDLPrompt(req)
		entityName = req.CheckConstraint.Name
		validatePrefix = "ALTER TABLE"
		logging.Debug("AI check constraint DDL generation: %s on %s.%s (%s)",
			req.CheckConstraint.Name, req.TargetSchema, req.Table.Name, req.TargetDBType)

	default:
		return "", fmt.Errorf("unknown DDL type: %s", req.Type)
	}

	result, err := m.CallAI(ctx, prompt)
	if err != nil {
		return "", fmt.Errorf("AI DDL generation failed for %s.%s: %w",
			req.Table.Name, entityName, err)
	}

	// On retry calls the prompt invited a NOT_RETRYABLE classification; if the
	// model chose that path, surface ErrNotRetryable to the writer so it
	// breaks out of the retry loop with the original DB error preserved.
	if abort, reason := classifyRetryResponse(result); abort {
		return "", WrapNotRetryable(reason)
	}

	ddl := strings.TrimSpace(result)

	// Validate response starts with expected prefix
	upperDDL := strings.ToUpper(ddl)
	if req.Type == DDLTypeIndex {
		if !strings.HasPrefix(upperDDL, "CREATE") || !strings.Contains(upperDDL, "INDEX") {
			return "", fmt.Errorf("response does not contain valid CREATE INDEX statement: %s", truncateString(ddl, 100))
		}
	} else if !strings.HasPrefix(upperDDL, validatePrefix) {
		return "", fmt.Errorf("response does not contain valid %s statement: %s", validatePrefix, truncateString(ddl, 100))
	}

	logging.Debug("AI generated DDL:\n%s", ddl)

	return ddl, nil
}

// writeFinalizationPriorAttempt appends a "PRIOR ATTEMPT FAILED" section to a
// finalization-DDL prompt when req.PreviousAttempt is set. Shared by the three
// finalization prompt builders (index / FK / CHECK) so the retry-corrective
// section has the same shape across all DDL types. See #29 PR B for the
// validate-and-retry design; the table-creation equivalent is the inline block
// in buildTableDDLPrompt.
func writeFinalizationPriorAttempt(sb *strings.Builder, req FinalizationDDLRequest) {
	if req.PreviousAttempt == nil {
		return
	}
	sb.WriteString("\n=== PRIOR ATTEMPT FAILED ===\n")
	sb.WriteString("The previous DDL you generated was rejected by the target database.\n\n")
	sb.WriteString("Previous DDL (verbatim):\n")
	sb.WriteString(req.PreviousAttempt.DDL)
	sb.WriteString("\n\nDatabase error (verbatim):\n")
	sb.WriteString(req.PreviousAttempt.Error)
	sb.WriteString("\n\nGenerate a corrected DDL for the same target object. Keep the same identifiers, columns, and intent — only fix what the error indicates is wrong. Do not regenerate from scratch.\n")
	writeRetryClassificationInstruction(sb)
}

// writeRetryClassificationInstruction adds a "fix or bail" tail to retry-path
// prompts. The AI must either return corrected DDL (the normal output the
// validators look for) or — if the error is a non-fixable state issue — emit
// the literal NOT_RETRYABLE marker on the first line. The writer's retry loop
// errors.Is-checks for ErrNotRetryable and surfaces the original DB error
// instead of consuming the rest of the retry budget.
//
// This is the load-bearing prompt change behind the AI-classifier conversion
// (#29 follow-up): it lets us delete per-driver isRetryableDDLError SQLSTATE
// allowlists and rely on the model to recognize cases where retry is futile.
// Phrasing examples come from real cases that surfaced in matrix runs:
// PG 42883 (uuid()), MySQL 1213 (deadlock), MSSQL 2714 (already exists).
//
// The strict-output paragraph at the end is the #34 fix: smaller / chattier
// models (haiku 4.5 in particular) sometimes interpret the corrective context
// as inviting analysis and respond in natural language, which the parser
// rejects as malformed and surfaces as a hard run-killer. The asymmetric old
// wording strictly enforced "no prose" only on the bail branch; this version
// applies the same strictness to the fix branch too. See issue #34 for the
// observed haiku case (concat() not immutable on PG generated column).
func writeRetryClassificationInstruction(sb *strings.Builder) {
	sb.WriteString("\nIMPORTANT — RETRY CLASSIFICATION:\n")
	sb.WriteString("If, after reading the database error, you determine that retrying will NOT help — for example: the object already exists in the target, an FK references a missing parent, the user lacks permission, the error is a real data-integrity violation, the operation deadlocked, or the connection failed — respond with ONLY the literal text NOT_RETRYABLE on the first line, optionally followed by ': ' and a one-sentence reason. Emit no DDL, no code fences, no other text in that case.\n")
	sb.WriteString("Otherwise (the error indicates a fixable defect in the DDL — wrong syntax, unknown type, undeclared function, malformed clause, etc.), return the corrected DDL as instructed above.\n")
	sb.WriteString("\nSTRICT OUTPUT REQUIREMENT: your entire response must be EITHER a complete DDL statement OR the NOT_RETRYABLE marker (with optional reason). Do not include explanations, analysis, commentary, code fences, markdown, or any prose outside SQL syntax. If you would otherwise discuss the problem in words, write the corrected DDL instead — or, if no DDL can fix it, write NOT_RETRYABLE: <reason>. Output the SQL statement (or the marker) directly with no preamble.\n")
}

// buildIndexDDLPrompt creates the AI prompt for index DDL generation.
func (m *AITypeMapper) buildIndexDDLPrompt(req FinalizationDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate a CREATE INDEX statement.\n\n")

	// Target database context
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		sb.WriteString(fmt.Sprintf("Max Identifier Length: %d\n", req.TargetContext.MaxIdentifierLength))
		if req.TargetContext.IdentifierCase != "" {
			sb.WriteString(fmt.Sprintf("Identifier Case: %s\n", req.TargetContext.IdentifierCase))
		}
	}
	sb.WriteString("\n")

	// Target table DDL for context
	if req.TargetTableDDL != "" {
		sb.WriteString("=== TARGET TABLE DDL ===\n")
		sb.WriteString(req.TargetTableDDL)
		sb.WriteString("\n\n")
	}

	// Index details
	sb.WriteString("=== INDEX TO CREATE ===\n")
	sb.WriteString(fmt.Sprintf("Table: %s\n", req.Table.Name))
	sb.WriteString(fmt.Sprintf("Index Name: %s\n", req.Index.Name))
	sb.WriteString(fmt.Sprintf("Columns: %s\n", strings.Join(req.Index.Columns, ", ")))
	sb.WriteString(fmt.Sprintf("Is Unique: %v\n", req.Index.IsUnique))
	if len(req.Index.IncludeCols) > 0 {
		sb.WriteString(fmt.Sprintf("Include Columns: %s\n", strings.Join(req.Index.IncludeCols, ", ")))
	}
	if req.Index.Filter != "" {
		sb.WriteString(fmt.Sprintf("Filter (WHERE clause): %s\n", req.Index.Filter))
	}
	sb.WriteString("\n")

	// Output requirements
	sb.WriteString("=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete CREATE INDEX statement for the target database.\n")
	sb.WriteString("- Use appropriate index name (prefix with idx_ if needed, respect max identifier length)\n")
	sb.WriteString("- Include UNIQUE keyword if IsUnique is true\n")
	sb.WriteString("- Include INCLUDE clause if target supports it (SQL Server, PostgreSQL 11+)\n")
	sb.WriteString("- Include WHERE clause for filtered indexes if target supports it\n")
	sb.WriteString("- Quote identifiers appropriately for the target database\n")
	sb.WriteString("- Return ONLY the raw CREATE INDEX SQL statement as plain text\n")
	sb.WriteString("- Do NOT wrap the response in JSON, markdown code blocks, or any other format\n")

	// Database-specific identifier requirements from the target dialect
	if dialect := GetDialect(req.TargetDBType); dialect != nil {
		if aug := dialect.AIPromptAugmentation(); aug != "" {
			sb.WriteString(aug)
		}
	}

	// Retry-corrective context — see #29 PR B. Appended last so the model
	// gives the prior failure maximum weight when generating the next try.
	writeFinalizationPriorAttempt(&sb, req)

	return sb.String()
}

// buildForeignKeyDDLPrompt creates the AI prompt for foreign key DDL generation.
func (m *AITypeMapper) buildForeignKeyDDLPrompt(req FinalizationDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate an ALTER TABLE statement to add a foreign key constraint.\n\n")

	// Target database context
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		sb.WriteString(fmt.Sprintf("Max Identifier Length: %d\n", req.TargetContext.MaxIdentifierLength))
		if req.TargetContext.IdentifierCase != "" {
			sb.WriteString(fmt.Sprintf("Identifier Case: %s\n", req.TargetContext.IdentifierCase))
		}
	}
	sb.WriteString("\n")

	// Target table DDL for context
	if req.TargetTableDDL != "" {
		sb.WriteString("=== TARGET TABLE DDL ===\n")
		sb.WriteString(req.TargetTableDDL)
		sb.WriteString("\n\n")
	}

	// Foreign key details
	sb.WriteString("=== FOREIGN KEY TO CREATE ===\n")
	sb.WriteString(fmt.Sprintf("Table: %s\n", req.Table.Name))
	sb.WriteString(fmt.Sprintf("FK Name: %s\n", req.ForeignKey.Name))
	sb.WriteString(fmt.Sprintf("Columns: %s\n", strings.Join(req.ForeignKey.Columns, ", ")))
	refTable := req.ForeignKey.RefTable
	if req.ForeignKey.RefSchema != "" && req.ForeignKey.RefSchema != req.TargetSchema {
		refTable = req.ForeignKey.RefSchema + "." + req.ForeignKey.RefTable
	}
	sb.WriteString(fmt.Sprintf("References Table: %s\n", refTable))
	sb.WriteString(fmt.Sprintf("References Columns: %s\n", strings.Join(req.ForeignKey.RefColumns, ", ")))
	if req.ForeignKey.OnDelete != "" {
		sb.WriteString(fmt.Sprintf("ON DELETE: %s\n", req.ForeignKey.OnDelete))
	}
	if req.ForeignKey.OnUpdate != "" {
		sb.WriteString(fmt.Sprintf("ON UPDATE: %s\n", req.ForeignKey.OnUpdate))
	}
	sb.WriteString("\n")

	// Output requirements
	sb.WriteString("=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete ALTER TABLE ... ADD CONSTRAINT statement for the foreign key.\n")
	sb.WriteString("- Use appropriate constraint name (prefix with fk_ if needed, respect max identifier length)\n")
	sb.WriteString("- Include ON DELETE and ON UPDATE actions if specified\n")
	sb.WriteString("- Map referential actions to target database syntax (NO ACTION, CASCADE, SET NULL, etc.)\n")
	sb.WriteString("- Quote identifiers appropriately for the target database\n")
	sb.WriteString("- Return ONLY the raw ALTER TABLE SQL statement as plain text\n")
	sb.WriteString("- Do NOT wrap the response in JSON, markdown code blocks, or any other format\n")

	// Database-specific identifier requirements from the target dialect
	if dialect := GetDialect(req.TargetDBType); dialect != nil {
		if aug := dialect.AIPromptAugmentation(); aug != "" {
			sb.WriteString(aug)
		}
	}

	// Retry-corrective context — see #29 PR B. Appended last so the model
	// gives the prior failure maximum weight when generating the next try.
	writeFinalizationPriorAttempt(&sb, req)

	return sb.String()
}

// buildCheckConstraintDDLPrompt creates the AI prompt for check constraint DDL generation.
func (m *AITypeMapper) buildCheckConstraintDDLPrompt(req FinalizationDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate an ALTER TABLE statement to add a check constraint.\n\n")

	// Source database context (for translating expressions)
	if req.SourceDBType != "" {
		sb.WriteString("=== SOURCE DATABASE ===\n")
		sb.WriteString(fmt.Sprintf("Type: %s\n", req.SourceDBType))
		sb.WriteString("\n")
	}

	// Target database context
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		sb.WriteString(fmt.Sprintf("Max Identifier Length: %d\n", req.TargetContext.MaxIdentifierLength))
		if req.TargetContext.IdentifierCase != "" {
			sb.WriteString(fmt.Sprintf("Identifier Case: %s\n", req.TargetContext.IdentifierCase))
		}
	}
	sb.WriteString("\n")

	// Target table DDL for context
	if req.TargetTableDDL != "" {
		sb.WriteString("=== TARGET TABLE DDL ===\n")
		sb.WriteString(req.TargetTableDDL)
		sb.WriteString("\n\n")
	}

	// Check constraint details
	sb.WriteString("=== CHECK CONSTRAINT TO CREATE ===\n")
	sb.WriteString(fmt.Sprintf("Table: %s\n", req.Table.Name))
	sb.WriteString(fmt.Sprintf("Constraint Name: %s\n", req.CheckConstraint.Name))
	sb.WriteString(fmt.Sprintf("Definition: %s\n", req.CheckConstraint.Definition))
	sb.WriteString("\n")

	// Output requirements
	sb.WriteString("=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete ALTER TABLE ... ADD CONSTRAINT statement for the check constraint.\n")
	sb.WriteString("- Use appropriate constraint name (prefix with chk_ if needed, respect max identifier length)\n")
	sb.WriteString("- Convert the check expression syntax from source database to target database\n")
	sb.WriteString("- Convert functions appropriately (e.g., GETDATE() -> NOW(), SYSDATE, CURRENT_TIMESTAMP)\n")
	sb.WriteString("- Quote identifiers appropriately for the target database\n")
	sb.WriteString("- Return ONLY the raw ALTER TABLE SQL statement as plain text\n")
	sb.WriteString("- Do NOT wrap the response in JSON, markdown code blocks, or any other format\n")

	// Database-specific identifier requirements from the target dialect
	if dialect := GetDialect(req.TargetDBType); dialect != nil {
		if aug := dialect.AIPromptAugmentation(); aug != "" {
			sb.WriteString(aug)
		}
	}

	// Retry-corrective context — see #29 PR B. Appended last so the model
	// gives the prior failure maximum weight when generating the next try.
	writeFinalizationPriorAttempt(&sb, req)

	return sb.String()
}

// GenerateDropTableDDL generates DDL statement(s) for dropping a table.
// The AI generates database-specific syntax that properly handles foreign key constraints.
func (m *AITypeMapper) GenerateDropTableDDL(ctx context.Context, req DropTableDDLRequest) (string, error) {
	if req.TableName == "" {
		return "", fmt.Errorf("TableName is required")
	}
	if req.TargetDBType == "" {
		return "", fmt.Errorf("TargetDBType is required")
	}

	// Build cache key
	cacheKey := fmt.Sprintf("drop:%s:%s.%s", req.TargetDBType, req.TargetSchema, req.TableName)

	// Check cache first
	m.cacheMu.RLock()
	if cached, ok := m.cache.Get(cacheKey); ok {
		m.cacheMu.RUnlock()
		return cached, nil
	}
	m.cacheMu.RUnlock()

	logging.Debug("AI drop table DDL generation: %s.%s (%s)", req.TargetSchema, req.TableName, req.TargetDBType)

	// Build prompt
	prompt := m.buildDropTableDDLPrompt(req)

	// Call AI API
	result, err := m.CallAI(ctx, prompt)
	if err != nil {
		return "", fmt.Errorf("AI drop table DDL generation failed for %s.%s: %w",
			req.TargetSchema, req.TableName, err)
	}

	ddl := strings.TrimSpace(result)

	// Basic validation - should contain DROP
	upperDDL := strings.ToUpper(ddl)
	if !strings.Contains(upperDDL, "DROP") {
		return "", fmt.Errorf("response does not contain valid DROP statement: %s", truncateString(ddl, 100))
	}

	// Cache the result
	m.cacheMu.Lock()
	m.cache.Set(cacheKey, ddl)
	m.cacheMu.Unlock()

	// Persist cache
	if err := m.saveCache(); err != nil {
		logging.Warn("Failed to save AI drop table DDL cache: %v", err)
	}

	logging.Debug("AI generated DROP DDL:\n%s", ddl)

	return ddl, nil
}

// buildDropTableDDLPrompt creates the AI prompt for DROP TABLE DDL generation.
func (m *AITypeMapper) buildDropTableDDLPrompt(req DropTableDDLRequest) string {
	var sb strings.Builder

	sb.WriteString("You are a database migration expert. Generate a DROP TABLE statement.\n\n")

	// Target database context
	sb.WriteString("=== TARGET DATABASE ===\n")
	sb.WriteString(fmt.Sprintf("Type: %s\n", req.TargetDBType))
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	if req.TargetContext != nil {
		sb.WriteString(fmt.Sprintf("Version: %s\n", req.TargetContext.Version))
		if req.TargetContext.MaxIdentifierLength > 0 {
			sb.WriteString(fmt.Sprintf("Max Identifier Length: %d\n", req.TargetContext.MaxIdentifierLength))
		}
	}
	sb.WriteString("\n")

	// Table to drop
	sb.WriteString("=== TABLE TO DROP ===\n")
	if req.TargetSchema != "" {
		sb.WriteString(fmt.Sprintf("Schema: %s\n", req.TargetSchema))
	}
	sb.WriteString(fmt.Sprintf("Table: %s\n", req.TableName))
	sb.WriteString("\n")

	// Output requirements
	sb.WriteString("=== OUTPUT REQUIREMENTS ===\n")
	sb.WriteString("Generate the complete statement(s) to drop the table, ensuring foreign key constraints do not block the drop.\n")
	sb.WriteString("Return ONLY the raw SQL statement(s) as plain text.\n")
	sb.WriteString("Do NOT wrap the response in JSON, markdown code blocks, or any other format.\n\n")

	// Database-specific instructions from dialect
	if dialect := GetDialect(req.TargetDBType); dialect != nil {
		if aug := dialect.AIDropTablePromptAugmentation(); aug != "" {
			sb.WriteString(aug)
		}
	} else {
		sb.WriteString("- Use DROP TABLE IF EXISTS with appropriate syntax for the target database\n")
		sb.WriteString("- Handle foreign key constraints appropriately\n")
	}

	return sb.String()
}
