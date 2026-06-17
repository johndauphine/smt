package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// FileState implements StateBackend using a single YAML file.
// Designed for Airflow and headless environments where SQLite is impractical.
type FileState struct {
	path  string
	mu    sync.RWMutex
	state *fileStateData
}

// fileStateData is the YAML structure for the state file.
type fileStateData struct {
	RunID        string     `yaml:"run_id"`
	StartedAt    time.Time  `yaml:"started_at"`
	CompletedAt  *time.Time `yaml:"completed_at,omitempty"`
	Status       string     `yaml:"status"` // running, success, failed
	Phase        string     `yaml:"phase"`  // initializing, finalizing, complete
	Error        string     `yaml:"error,omitempty"`
	SourceSchema string     `yaml:"source_schema"`
	TargetSchema string     `yaml:"target_schema"`
	ConfigHash   string     `yaml:"config_hash,omitempty"`
	ProfileName  string     `yaml:"profile_name,omitempty"`
	ConfigPath   string     `yaml:"config_path,omitempty"`
}

// NewFileState creates a file-based state manager.
// If the file exists, it loads the existing state.
func NewFileState(path string) (*FileState, error) {
	fs := &FileState{
		path:  path,
		state: &fileStateData{},
	}

	// Load existing state if file exists
	if _, err := os.Stat(path); err == nil {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading state file: %w", err)
		}
		if err := yaml.Unmarshal(data, fs.state); err != nil {
			return nil, fmt.Errorf("parsing state file: %w", err)
		}
	}

	return fs, nil
}

// save writes the current state to the YAML file.
func (fs *FileState) save() error {
	data, err := yaml.Marshal(fs.state)
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}
	if err := os.WriteFile(fs.path, data, 0600); err != nil {
		return fmt.Errorf("writing state file: %w", err)
	}
	return nil
}

// CreateRun initializes a new migration run. The file backend does not
// surface run history, so kind is accepted for interface parity only.
func (fs *FileState) CreateRun(id, kind, sourceSchema, targetSchema string, config any, profileName, configPath string) error {
	_ = kind
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Compute config hash for change detection
	configJSON, _ := json.Marshal(config)
	hash := sha256.Sum256(configJSON)

	fs.state = &fileStateData{
		RunID:        id,
		StartedAt:    time.Now(),
		Status:       "running",
		SourceSchema: sourceSchema,
		TargetSchema: targetSchema,
		ConfigHash:   hex.EncodeToString(hash[:8]), // First 8 bytes
		ProfileName:  profileName,
		ConfigPath:   configPath,
	}

	return fs.save()
}

// CompleteRun marks the run as complete.
func (fs *FileState) CompleteRun(id string, status string, errorMsg string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state.RunID != id {
		return fmt.Errorf("run ID mismatch: expected %s, got %s", fs.state.RunID, id)
	}

	now := time.Now()
	fs.state.Status = status
	fs.state.CompletedAt = &now
	fs.state.Error = errorMsg

	return fs.save()
}

// UpdatePhase updates the current phase of a migration run.
func (fs *FileState) UpdatePhase(runID, phase string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.state != nil && fs.state.RunID == runID {
		fs.state.Phase = phase
		return fs.save()
	}
	return nil
}

// GetAllRuns returns the current run only (file state doesn't track history).
func (fs *FileState) GetAllRuns() ([]Run, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.RunID != "" {
		return []Run{fs.currentRun()}, nil
	}
	return nil, nil
}

// GetRunByID returns the run if it matches.
func (fs *FileState) GetRunByID(runID string) (*Run, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if fs.state.RunID == runID {
		r := fs.currentRun()
		return &r, nil
	}
	return nil, nil
}

// currentRun builds a Run from the in-memory state. Caller holds the lock.
func (fs *FileState) currentRun() Run {
	return Run{
		ID:           fs.state.RunID,
		StartedAt:    fs.state.StartedAt,
		CompletedAt:  fs.state.CompletedAt,
		Status:       fs.state.Status,
		Phase:        fs.state.Phase,
		Error:        fs.state.Error,
		SourceSchema: fs.state.SourceSchema,
		TargetSchema: fs.state.TargetSchema,
		ConfigHash:   fs.state.ConfigHash,
		ProfileName:  fs.state.ProfileName,
		ConfigPath:   fs.state.ConfigPath,
	}
}

// Close is a no-op for file state.
func (fs *FileState) Close() error {
	return nil
}

// Path returns the state file path.
func (fs *FileState) Path() string {
	return fs.path
}

// Ensure FileState implements StateBackend
var _ StateBackend = (*FileState)(nil)
