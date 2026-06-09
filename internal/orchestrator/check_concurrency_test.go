package orchestrator

import (
	"testing"

	"smt/internal/config"
)

// #81/#25 — the CHECK phase serializes on MySQL targets to avoid InnoDB
// metadata-lock deadlocks (Error 1213); other targets keep full concurrency.
func TestCheckConcurrencySerializesMySQL(t *testing.T) {
	for targetType, want := range map[string]int{
		"mysql":    1,
		"mariadb":  1,
		"postgres": defaultAIConcurrency,
		"mssql":    defaultAIConcurrency,
	} {
		cfg := &config.Config{}
		cfg.Target.Type = targetType
		o := &Orchestrator{config: cfg}
		if got := o.checkConcurrency(); got != want {
			t.Errorf("checkConcurrency(target=%s) = %d, want %d", targetType, got, want)
		}
	}
}
