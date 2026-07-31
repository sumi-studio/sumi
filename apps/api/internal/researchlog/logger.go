// Package researchlog implements the 研究協力-gated content logging pipeline
// (ADR 0009 §6). The default life-log is private — even administrators cannot
// read command content. Only Humans who opted into 研究協力 have their command
// content mirrored to a research log. Metadata-only operational telemetry
// (command id, sequence, agent, timestamp — never the content) is captured for
// all targets.
package researchlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ConsentChecker reports whether a Human has an active 研究協力 consent.
type ConsentChecker interface {
	ResearchConsentActive(ctx context.Context, humanID string) (bool, error)
}

// Logger writes consent-gated research content and always-on metadata telemetry.
// Both are best-effort: a logging failure never affects the command path.
type Logger struct {
	consent      ConsentChecker
	researchDir  string
	telemetryDir string
	researchMu   sync.Mutex
	telemetryMu  sync.Mutex
	now          func() time.Time
}

// New returns a Logger that writes research content to researchDir (only for
// consented Humans) and metadata telemetry to telemetryDir (always). Empty
// directories disable that stream.
func New(consent ConsentChecker, researchDir, telemetryDir string) (*Logger, error) {
	if consent == nil {
		return nil, errors.New("researchlog requires a consent checker")
	}
	for _, dir := range []string{researchDir, telemetryDir} {
		if dir == "" {
			continue
		}
		if info, err := os.Stat(dir); err != nil || !info.IsDir() {
			return nil, fmt.Errorf("researchlog directory %q is not a usable directory", dir)
		}
	}
	return &Logger{
		consent:      consent,
		researchDir:  researchDir,
		telemetryDir: telemetryDir,
		now:          time.Now,
	}, nil
}

// LogCommand records a command. Metadata telemetry is always written (when
// configured); the content is mirrored to the research log only when the Human
// has an active 研究協力 consent. Errors are returned for observation but the
// caller should not fail the command path on them.
func (l *Logger) LogCommand(
	ctx context.Context,
	humanID, agentID, commandID string,
	seq uint64,
	content []byte,
) error {
	if l.telemetryDir != "" {
		l.writeTelemetry(telemetryRecord{
			Timestamp:        l.now().UTC().Format(time.RFC3339Nano),
			HumanID:          humanID,
			AgentID:          agentID,
			CommandID:        commandID,
			Seq:              seq,
			ResearchConsented: l.consentActive(ctx, humanID),
		})
	}
	if l.researchDir != "" {
		if active, err := l.consent.ResearchConsentActive(ctx, humanID); err == nil && active {
			l.writeResearch(researchRecord{
				Timestamp: l.now().UTC().Format(time.RFC3339Nano),
				HumanID:   humanID,
				AgentID:   agentID,
				CommandID: commandID,
				Seq:       seq,
				Content:   content,
			})
		}
	}
	return nil
}

func (l *Logger) consentActive(ctx context.Context, humanID string) bool {
	active, err := l.consent.ResearchConsentActive(ctx, humanID)
	return err == nil && active
}

type telemetryRecord struct {
	Timestamp         string `json:"timestamp"`
	HumanID           string `json:"human_id"`
	AgentID           string `json:"agent_id"`
	CommandID         string `json:"command_id"`
	Seq               uint64 `json:"seq"`
	ResearchConsented bool   `json:"research_consented"`
}

type researchRecord struct {
	Timestamp string          `json:"timestamp"`
	HumanID   string          `json:"human_id"`
	AgentID   string          `json:"agent_id"`
	CommandID string          `json:"command_id"`
	Seq       uint64          `json:"seq"`
	Content   json.RawMessage `json:"content"`
}

func (l *Logger) writeTelemetry(record telemetryRecord) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return
	}
	encoded = append(encoded, '\n')
	l.telemetryMu.Lock()
	defer l.telemetryMu.Unlock()
	path := filepath.Join(l.telemetryDir, "commands.jsonl")
	appendLine(path, encoded)
}

func (l *Logger) writeResearch(record researchRecord) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return
	}
	encoded = append(encoded, '\n')
	l.researchMu.Lock()
	defer l.researchMu.Unlock()
	path := filepath.Join(l.researchDir, fmt.Sprintf("%s.jsonl", sanitizeFilename(record.AgentID)))
	appendLine(path, encoded)
}

func appendLine(path string, encoded []byte) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer file.Close()
	_, _ = file.Write(encoded)
}

func sanitizeFilename(name string) string {
	cleaned := filepath.Clean(name)
	if cleaned == "." || cleaned == ".." || filepath.IsAbs(cleaned) {
		return "agent"
	}
	return cleaned
}
