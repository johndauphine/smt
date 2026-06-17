package checkpoint

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// State manages migration state in SQLite
type State struct {
	db *sql.DB
}

// Run represents a migration run
type Run struct {
	ID           string
	Kind         string // "apply" (executed DDL on a target) or "generate" (preview only)
	StartedAt    time.Time
	CompletedAt  *time.Time
	Status       string
	Phase        string // Current phase: initializing, transferring, finalizing, validating, complete
	SourceSchema string
	TargetSchema string
	Config       string
	// ConfigHash is the hash of the migration config, used for change detection on resume.
	// Both SQLite and file-based backends persist this field for config validation.
	ConfigHash  string
	ProfileName string
	ConfigPath  string
	Error       string // Error message if status is "failed"
}

// New creates a new state manager
func New(dataDir string) (*State, error) {
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("creating data dir: %w", err)
	}
	// Enforce permissions in case umask relaxed them.
	if err := os.Chmod(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("setting data dir permissions: %w", err)
	}

	dbPath := filepath.Join(dataDir, "migrate.db")
	// Ensure the DB file exists with restrictive permissions before sql.Open creates it.
	if _, err := os.Stat(dbPath); errors.Is(err, fs.ErrNotExist) {
		if f, createErr := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL, 0600); createErr == nil {
			f.Close()
		} else {
			return nil, fmt.Errorf("creating db file: %w", createErr)
		}
	}
	// WAL mode for better concurrency, busy_timeout to retry on lock contention
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(30000)")
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Configure connection pool for multi-process access:
	// - MaxIdleConns(0): Close connections after use to ensure fresh reads across processes
	// - MaxOpenConns(1): Single connection at a time to avoid lock contention
	// This ensures each query sees the latest committed data from other processes
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(1)

	s := &State{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating schema: %w", err)
	}

	return s, nil
}

func (s *State) migrate() error {
	schema := `
	CREATE TABLE IF NOT EXISTS runs (
		id TEXT PRIMARY KEY,
		kind TEXT NOT NULL DEFAULT 'apply',
		started_at TEXT NOT NULL,
		completed_at TEXT,
		status TEXT NOT NULL DEFAULT 'running',
		phase TEXT NOT NULL DEFAULT 'initializing',
		source_schema TEXT NOT NULL,
		target_schema TEXT NOT NULL,
		config TEXT,
		profile_name TEXT,
		config_path TEXT
	);

	CREATE TABLE IF NOT EXISTS profiles (
		name TEXT PRIMARY KEY,
		description TEXT,
		config_enc BLOB NOT NULL,
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL
	);
	`
	// Note (#158): the DMT-era tasks / task_outputs / transfer_progress /
	// table_sync_timestamps tables are no longer created. Pre-existing state DBs
	// keep them as harmless empties (no DROP migration — forward-compat option a).

	if _, err := s.db.Exec(schema); err != nil {
		return err
	}

	if err := s.ensureRunColumns(); err != nil {
		return err
	}
	if err := s.ensureProfileColumns(); err != nil {
		return err
	}
	if err := s.ensureSnapshotsTable(); err != nil {
		return err
	}

	// One-time migration: sanitize any passwords stored in config column
	return s.sanitizeStoredConfigs()
}

func (s *State) ensureRunColumns() error {
	columns, err := s.tableColumns("runs")
	if err != nil {
		return err
	}

	needsProfile := true
	needsConfigPath := true
	needsError := true
	needsPhase := true
	needsConfigHash := true
	needsKind := true
	for _, col := range columns {
		switch col {
		case "profile_name":
			needsProfile = false
		case "config_path":
			needsConfigPath = false
		case "error":
			needsError = false
		case "phase":
			needsPhase = false
		case "config_hash":
			needsConfigHash = false
		case "kind":
			needsKind = false
		}
	}

	if needsProfile {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN profile_name TEXT`); err != nil {
			return err
		}
	}
	if needsConfigPath {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN config_path TEXT`); err != nil {
			return err
		}
	}
	if needsError {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN error TEXT`); err != nil {
			return err
		}
	}
	if needsPhase {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN phase TEXT DEFAULT 'initializing'`); err != nil {
			return err
		}
	}
	if needsConfigHash {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN config_hash TEXT`); err != nil {
			return err
		}
	}
	if needsKind {
		if _, err := s.db.Exec(`ALTER TABLE runs ADD COLUMN kind TEXT NOT NULL DEFAULT 'apply'`); err != nil {
			return err
		}
	}

	return nil
}

func (s *State) ensureProfileColumns() error {
	columns, err := s.tableColumns("profiles")
	if err != nil {
		return err
	}

	hasDescription := false
	for _, col := range columns {
		if col == "description" {
			hasDescription = true
			break
		}
	}

	if !hasDescription {
		if _, err := s.db.Exec(`ALTER TABLE profiles ADD COLUMN description TEXT`); err != nil {
			return err
		}
	}
	return nil
}

// validTableNames is a whitelist of allowed table names for schema queries.
// This prevents SQL injection via the table parameter in tableColumns().
var validTableNames = map[string]bool{
	"runs":     true,
	"profiles": true,
}

func (s *State) tableColumns(table string) ([]string, error) {
	// Validate table name against whitelist to prevent SQL injection
	// SQLite PRAGMA table_info doesn't support parameterized queries
	if !validTableNames[table] {
		return nil, fmt.Errorf("invalid table name: %s", table)
	}

	rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dfltValue any
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, name)
	}
	return cols, rows.Err()
}

// sanitizeStoredConfigs removes any passwords accidentally stored in config JSON
func (s *State) sanitizeStoredConfigs() error {
	rows, err := s.db.Query(`SELECT id, config FROM runs WHERE config IS NOT NULL AND config != ''`)
	if err != nil {
		return err
	}
	defer rows.Close()

	type update struct {
		id     string
		config string
	}
	var updates []update

	for rows.Next() {
		var id, configStr string
		if err := rows.Scan(&id, &configStr); err != nil {
			continue
		}

		// Check if this config contains unredacted passwords
		if !strings.Contains(configStr, `"Password"`) {
			continue
		}

		// Parse and sanitize
		var configMap map[string]any
		if err := json.Unmarshal([]byte(configStr), &configMap); err != nil {
			continue
		}

		modified := false
		for _, section := range []string{"Source", "Target"} {
			if sec, ok := configMap[section].(map[string]any); ok {
				if pw, ok := sec["Password"].(string); ok && pw != "" && pw != "[REDACTED]" {
					sec["Password"] = "[REDACTED]"
					modified = true
				}
			}
		}
		// Also sanitize Slack webhook
		if slack, ok := configMap["Slack"].(map[string]any); ok {
			if wh, ok := slack["WebhookURL"].(string); ok && wh != "" && wh != "[REDACTED]" {
				slack["WebhookURL"] = "[REDACTED]"
				modified = true
			}
		}

		if modified {
			newConfig, _ := json.Marshal(configMap)
			updates = append(updates, update{id: id, config: string(newConfig)})
		}
	}

	// Apply updates
	for _, u := range updates {
		if _, err := s.db.Exec(`UPDATE runs SET config = ? WHERE id = ?`, u.config, u.id); err != nil {
			return fmt.Errorf("sanitizing config for run %s: %w", u.id, err)
		}
	}

	return nil
}

// Close closes the database connection
func (s *State) Close() error {
	return s.db.Close()
}

// CreateRun creates a new migration run
func (s *State) CreateRun(id, kind, sourceSchema, targetSchema string, config any, profileName, configPath string) error {
	configJSON, _ := json.Marshal(config)

	// Compute config hash for change detection on resume (matches filestate behavior)
	hash := sha256.Sum256(configJSON)
	configHash := hex.EncodeToString(hash[:8])

	if kind == "" {
		kind = RunKindApply
	}
	_, err := s.db.Exec(`
		INSERT INTO runs (id, kind, started_at, status, source_schema, target_schema, config, profile_name, config_path, config_hash)
		VALUES (?, ?, datetime('now'), 'running', ?, ?, ?, ?, ?, ?)
	`, id, kind, sourceSchema, targetSchema, string(configJSON), profileName, configPath, configHash)
	return err
}

// CompleteRun marks a run as complete
func (s *State) CompleteRun(id string, status string, errorMsg string) error {
	_, err := s.db.Exec(`
		UPDATE runs SET status = ?, completed_at = datetime('now'), error = ?
		WHERE id = ?
	`, status, errorMsg, id)
	return err
}

// UpdatePhase updates the current phase of a migration run
func (s *State) UpdatePhase(runID, phase string) error {
	_, err := s.db.Exec(`UPDATE runs SET phase = ? WHERE id = ?`, phase, runID)
	return err
}

// GetAllRuns returns all runs for history
func (s *State) GetAllRuns() ([]Run, error) {
	rows, err := s.db.Query(`
		SELECT id, COALESCE(kind, 'apply'), started_at, completed_at, status, source_schema, target_schema, config, profile_name, config_path, error
		FROM runs ORDER BY started_at DESC LIMIT 20
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var runs []Run
	for rows.Next() {
		var r Run
		var startedAtStr string
		var completedAtStr sql.NullString
		var configStr sql.NullString
		var profileName, configPath, errorMsg sql.NullString
		if err := rows.Scan(&r.ID, &r.Kind, &startedAtStr, &completedAtStr, &r.Status, &r.SourceSchema, &r.TargetSchema, &configStr, &profileName, &configPath, &errorMsg); err != nil {
			return nil, err
		}
		r.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAtStr)
		if completedAtStr.Valid {
			t, _ := time.Parse("2006-01-02 15:04:05", completedAtStr.String)
			r.CompletedAt = &t
		}
		if configStr.Valid {
			r.Config = configStr.String
		}
		if profileName.Valid {
			r.ProfileName = profileName.String
		}
		if configPath.Valid {
			r.ConfigPath = configPath.String
		}
		if errorMsg.Valid {
			r.Error = errorMsg.String
		}
		runs = append(runs, r)
	}
	return runs, nil
}

// GetRunByID returns a specific run by ID
func (s *State) GetRunByID(runID string) (*Run, error) {
	var r Run
	var startedAtStr string
	var completedAtStr sql.NullString
	var configStr sql.NullString

	var profileName, configPath, errorMsg sql.NullString
	err := s.db.QueryRow(`
		SELECT id, COALESCE(kind, 'apply'), started_at, completed_at, status, source_schema, target_schema, config, profile_name, config_path, error
		FROM runs WHERE id = ?
	`, runID).Scan(&r.ID, &r.Kind, &startedAtStr, &completedAtStr, &r.Status, &r.SourceSchema, &r.TargetSchema, &configStr, &profileName, &configPath, &errorMsg)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	r.StartedAt, _ = time.Parse("2006-01-02 15:04:05", startedAtStr)
	if completedAtStr.Valid {
		t, _ := time.Parse("2006-01-02 15:04:05", completedAtStr.String)
		r.CompletedAt = &t
	}
	if configStr.Valid {
		r.Config = configStr.String
	}
	if profileName.Valid {
		r.ProfileName = profileName.String
	}
	if configPath.Valid {
		r.ConfigPath = configPath.String
	}
	if errorMsg.Valid {
		r.Error = errorMsg.String
	}
	return &r, nil
}
