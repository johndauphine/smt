// Package orchestrator coordinates SMT's schema operations: open source
// and target connections, extract the source schema, then issue CREATE
// (or in Phase 6, ALTER) DDL against the target. SMT runs SQL serially
// against open connections — no goroutine pools, no chunking, no
// row-level progress. Each phase is a small named method (see phases.go);
// the whole run is recorded to checkpoint state for history.
//
// The driver layer is registry-based: source/target are opened via
// pool.NewSourcePool / pool.NewTargetPool, which dispatch through the
// driver registry. Adding a new database engine means dropping a package
// under internal/driver/ that calls driver.Register in init() and adding
// a blank import to internal/pool/factory.go — no orchestrator changes.
package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"smt/internal/checkpoint"
	"smt/internal/config"
	"smt/internal/driver"
	"smt/internal/notify"
	"smt/internal/pool"
	"smt/internal/source"
)

// Options configures the orchestrator.
type Options struct {
	// StateFile overrides SQLite with a YAML state file (for headless/Airflow runs).
	StateFile string

	// RunID lets callers supply a deterministic run ID (otherwise UUID).
	RunID string

	// SourceOnly skips the target connection (used by inspection commands).
	SourceOnly bool
}

// Orchestrator opens source/target connections via the driver registry and
// runs schema phases in order. The schema phases live in phases.go; this
// file is just construction, lifecycle, and small accessors.
type Orchestrator struct {
	config     *config.Config
	source     pool.SourcePool
	target     pool.TargetPool
	state      checkpoint.StateBackend
	notifier   notify.Provider
	tables     []source.Table
	runProfile string
	runConfig  string
	opts       Options
}

// New constructs an Orchestrator with default options.
func New(cfg *config.Config) (*Orchestrator, error) {
	return NewWithOptions(cfg, Options{})
}

// NewWithOptions constructs an Orchestrator with the given options.
func NewWithOptions(cfg *config.Config, opts Options) (*Orchestrator, error) {
	o := &Orchestrator{config: cfg, opts: opts}

	src, err := pool.NewSourcePool(&cfg.Source, cfg.Migration.MaxSourceConnections)
	if err != nil {
		return nil, fmt.Errorf("opening source: %w", err)
	}
	o.source = src

	if !opts.SourceOnly {
		mapper, _ := driver.GetAITypeMapper()
		tgt, err := pool.NewTargetPool(&cfg.Target, cfg.Migration.MaxTargetConnections, cfg.Source.Type, mapper)
		if err != nil {
			src.Close()
			return nil, fmt.Errorf("opening target: %w", err)
		}
		o.target = tgt
	}

	state, err := openState(cfg, opts.StateFile)
	if err != nil {
		o.Close()
		return nil, fmt.Errorf("opening state: %w", err)
	}
	o.state = state

	o.notifier = newNotifier(cfg)

	return o, nil
}

// Close releases all underlying resources.
func (o *Orchestrator) Close() {
	if o.source != nil {
		o.source.Close()
	}
	if o.target != nil {
		o.target.Close()
	}
	if o.state != nil {
		o.state.Close()
	}
}

// SetRunContext records the profile/config-path attached to the next run.
func (o *Orchestrator) SetRunContext(profileName, configPath string) {
	o.runProfile = profileName
	o.runConfig = configPath
}

// Tables returns the tables in scope after include/exclude filtering.
// Populated after Run or after the extract-schema phase has been called.
func (o *Orchestrator) Tables() []source.Table { return o.tables }

// Source returns the underlying source Reader. Used by schema-diff and
// other inspection commands that introspect the source directly.
func (o *Orchestrator) Source() pool.SourcePool { return o.source }

// Target returns the underlying target Writer. Used by schema-diff and
// other commands that need to apply DDL directly.
func (o *Orchestrator) Target() pool.TargetPool { return o.target }

// State returns the underlying state backend.
func (o *Orchestrator) State() checkpoint.StateBackend { return o.state }

// ConfigHash returns a short hex hash of the sanitized config — used for
// detecting config drift across runs.
func (o *Orchestrator) ConfigHash() string {
	configJSON, _ := json.Marshal(o.config.Sanitized())
	hash := sha256.Sum256(configJSON)
	return hex.EncodeToString(hash[:8])
}

func newNotifier(cfg *config.Config) notify.Provider {
	if cfg.Slack != nil && cfg.Slack.Enabled {
		return notify.New(&notify.SlackConfig{
			WebhookURL: cfg.Slack.WebhookURL,
			Channel:    cfg.Slack.Channel,
			Username:   cfg.Slack.Username,
			Enabled:    cfg.Slack.Enabled,
		})
	}
	return notify.NewFromSecrets()
}

func openState(cfg *config.Config, stateFile string) (checkpoint.StateBackend, error) {
	if stateFile != "" {
		return checkpoint.NewFileState(stateFile)
	}
	dataDir := cfg.Migration.DataDir
	if dataDir == "" {
		var err error
		dataDir, err = config.DefaultDataDir()
		if err != nil {
			return nil, err
		}
	}
	return checkpoint.New(dataDir)
}
