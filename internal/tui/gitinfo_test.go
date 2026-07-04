package tui

import (
	"testing"
	"time"
)

// TestUpdate_TickDoesNotProbeGitSynchronously guards #188: a TickMsg must not
// call GetGitInfo inline (it shells out to git and would block the event
// loop). Instead it schedules an async probe and marks it in flight.
func TestUpdate_TickSchedulesAsyncGitProbe(t *testing.T) {
	m := InitialModel()
	m.gitInfoInFlight = false

	updated, cmd := m.Update(TickMsg(time.Now()))
	if cmd == nil {
		t.Fatal("TickMsg should return a batch command (tick + git probe)")
	}
	if !updated.(Model).gitInfoInFlight {
		t.Fatal("TickMsg should mark a git probe in flight")
	}
}

// TestUpdate_TickSkipsGitProbeWhenInFlight confirms the in-flight guard: a
// slow probe must not pile up across ticks.
func TestUpdate_TickSkipsGitProbeWhenInFlight(t *testing.T) {
	m := InitialModel()
	m.gitInfoInFlight = true

	updated, _ := m.Update(TickMsg(time.Now()))
	if !updated.(Model).gitInfoInFlight {
		t.Fatal("in-flight flag should stay set (no second probe spawned)")
	}
}

// TestUpdate_GitInfoMsgStoresResult confirms the async result lands and clears
// the in-flight flag.
func TestUpdate_GitInfoMsgStoresResult(t *testing.T) {
	m := InitialModel()
	m.gitInfoInFlight = true

	want := GitInfo{Branch: "feature-x", Status: "Dirty"}
	updated, _ := m.Update(gitInfoMsg(want))
	got := updated.(Model)
	if got.gitInfo != want {
		t.Fatalf("gitInfo = %+v, want %+v", got.gitInfo, want)
	}
	if got.gitInfoInFlight {
		t.Fatal("gitInfoMsg should clear the in-flight flag")
	}
}
