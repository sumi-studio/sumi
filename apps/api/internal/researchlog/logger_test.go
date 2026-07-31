package researchlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

type fakeConsentChecker struct {
	active bool
	err    error
}

func (f fakeConsentChecker) ResearchConsentActive(context.Context, string) (bool, error) {
	return f.active, f.err
}

func TestLoggerGatesContentOnConsent(t *testing.T) {
	researchDir := t.TempDir()
	telemetryDir := t.TempDir()

	t.Run("without consent: no research content, telemetry present", func(t *testing.T) {
		logger, err := New(fakeConsentChecker{active: false}, researchDir, telemetryDir)
		if err != nil {
			t.Fatalf("new logger: %v", err)
		}
		if err := logger.LogCommand(context.Background(), "human-1", "agent-1", "cmd-1", 1, []byte(`{"message":"secret content"}`)); err != nil {
			t.Fatalf("log command: %v", err)
		}
		// Research content must NOT be written.
		if _, err := os.Stat(filepath.Join(researchDir, "agent-1.jsonl")); !os.IsNotExist(err) {
			t.Fatalf("expected no research content without consent, got err=%v", err)
		}
		// Telemetry metadata must be present and must NOT contain the content.
		got, err := os.ReadFile(filepath.Join(telemetryDir, "commands.jsonl"))
		if err != nil {
			t.Fatalf("read telemetry: %v", err)
		}
		if !contains(got, "cmd-1") {
			t.Fatalf("telemetry missing command id: %s", got)
		}
		if contains(got, "secret content") {
			t.Fatalf("telemetry must not contain command content: %s", got)
		}
		if !contains(got, `"research_consented":false`) {
			t.Fatalf("telemetry should mark consent false: %s", got)
		}
	})

	t.Run("with consent: research content captured", func(t *testing.T) {
		logger, err := New(fakeConsentChecker{active: true}, researchDir, telemetryDir)
		if err != nil {
			t.Fatalf("new logger: %v", err)
		}
		if err := logger.LogCommand(context.Background(), "human-2", "agent-2", "cmd-2", 2, []byte(`{"message":"consented content"}`)); err != nil {
			t.Fatalf("log command: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(researchDir, "agent-2.jsonl"))
		if err != nil {
			t.Fatalf("expected research content with consent: %v", err)
		}
		if !contains(got, "consented content") {
			t.Fatalf("research log missing content: %s", got)
		}
		if !contains(got, "cmd-2") {
			t.Fatalf("research log missing command id: %s", got)
		}
	})
}

func TestLoggerTelemetryAlwaysOn(t *testing.T) {
	telemetryDir := t.TempDir()
	// No research dir configured; telemetry must still work.
	logger, err := New(fakeConsentChecker{active: true}, "", telemetryDir)
	if err != nil {
		t.Fatalf("new logger: %v", err)
	}
	if err := logger.LogCommand(context.Background(), "human-3", "agent-3", "cmd-3", 3, []byte("content-3")); err != nil {
		t.Fatalf("log command: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(telemetryDir, "commands.jsonl"))
	if err != nil {
		t.Fatalf("read telemetry: %v", err)
	}
	if !contains(got, "cmd-3") || contains(got, "content-3") {
		t.Fatalf("telemetry should have metadata without content: %s", got)
	}
}

func contains(haystack []byte, needle string) bool {
	return bytesContains(haystack, needle)
}

func bytesContains(haystack []byte, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == needle {
			return true
		}
	}
	return false
}
