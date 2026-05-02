package orchestrator

import (
	"context"
	"sync"
	"time"
)

// HealthCheckResult is the outcome of a connection-test run.
type HealthCheckResult struct {
	Timestamp        string `json:"timestamp"`
	SourceDBType     string `json:"source_db_type"`
	TargetDBType     string `json:"target_db_type"`
	SourceConnected  bool   `json:"source_connected"`
	TargetConnected  bool   `json:"target_connected"`
	SourceLatencyMs  int64  `json:"source_latency_ms"`
	TargetLatencyMs  int64  `json:"target_latency_ms"`
	SourceError      string `json:"source_error,omitempty"`
	TargetError      string `json:"target_error,omitempty"`
	SourceTableCount int    `json:"source_table_count,omitempty"`
	Healthy          bool   `json:"healthy"`
}

// HealthCheck pings source and target in parallel with independent 30s
// budgets and reports their status. It also counts tables in the source
// schema as a side-effect of confirming the schema query path works.
func (o *Orchestrator) HealthCheck(ctx context.Context) (*HealthCheckResult, error) {
	const checkTimeout = 30 * time.Second

	result := &HealthCheckResult{
		Timestamp:    time.Now().Format(time.RFC3339),
		SourceDBType: o.source.DBType(),
		TargetDBType: o.target.DBType(),
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go o.pingSource(ctx, checkTimeout, result, &wg)
	go o.pingTarget(ctx, checkTimeout, result, &wg)
	wg.Wait()

	result.Healthy = result.SourceConnected && result.TargetConnected
	return result, nil
}

func (o *Orchestrator) pingSource(ctx context.Context, timeout time.Duration, result *HealthCheckResult, wg *sync.WaitGroup) {
	defer wg.Done()
	start := time.Now()
	defer func() { result.SourceLatencyMs = time.Since(start).Milliseconds() }()

	sub, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	db := o.source.DB()
	if db == nil {
		result.SourceError = "no source connection"
		return
	}
	if err := db.PingContext(sub); err != nil {
		result.SourceError = err.Error()
		return
	}
	result.SourceConnected = true
	if tables, err := o.source.ExtractSchema(sub, o.config.Source.Schema); err == nil {
		result.SourceTableCount = len(tables)
	}
}

func (o *Orchestrator) pingTarget(ctx context.Context, timeout time.Duration, result *HealthCheckResult, wg *sync.WaitGroup) {
	defer wg.Done()
	start := time.Now()
	defer func() { result.TargetLatencyMs = time.Since(start).Milliseconds() }()

	sub, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := o.target.Ping(sub); err != nil {
		result.TargetError = err.Error()
		return
	}
	result.TargetConnected = true
}
