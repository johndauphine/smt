package checkpoint

// Run kinds distinguish runs that executed DDL against a target from
// generate-only previews in history and notifications (#90).
const (
	RunKindApply    = "apply"
	RunKindGenerate = "generate"
)

// StateBackend defines the interface for state persistence.
// Implementations: SQLite (full-featured, with history) and file-based (minimal,
// for Airflow / headless runs).
//
// SMT records each schema run for history; it does not move rows, so the DMT-era
// task/transfer-progress/resume surface was removed in v1 (#158).
type StateBackend interface {
	// Run management
	CreateRun(id, kind, sourceSchema, targetSchema string, config any, profileName, configPath string) error
	CompleteRun(id string, status string, errorMsg string) error
	UpdatePhase(runID, phase string) error

	// History (optional - file backend may return just the current run)
	GetAllRuns() ([]Run, error)
	GetRunByID(runID string) (*Run, error)

	// Lifecycle
	Close() error
}

// HistoryBackend extends StateBackend with profile management.
// Only SQLite implements this; the file backend does not support profiles.
type HistoryBackend interface {
	StateBackend

	// Profile management (encrypted config storage)
	SaveProfile(name, description string, config []byte) error
	GetProfile(name string) ([]byte, error)
	ListProfiles() ([]ProfileInfo, error)
	DeleteProfile(name string) error
}

// Ensure State implements HistoryBackend
var _ HistoryBackend = (*State)(nil)
