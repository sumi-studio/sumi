package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sumi-studio/sumi/apps/api/internal/agentevents"
	"github.com/sumi-studio/sumi/apps/api/internal/koseki"
)

// TestCloudReplacementProof drives an API that is already running elsewhere
// (for example the api-container Worker under `wrangler dev`, or an isolated
// deployment) through a Direct Chat exchange, has an operator command replace
// the API's host, and checks that the replacement serves the same
// acknowledged state:
//
//   - the conversation history is identical,
//   - a retried command (same Idempotency-Key) returns the original receipt,
//   - the next command takes the next sequence number.
//
// It is opt-in and needs, all in the environment:
//
//	SUMI_CLOUD_PROOF=1
//	SUMI_CLOUD_PROOF_URL            base URL of the API (through its Worker)
//	SUMI_CLOUD_PROOF_DB_URL         the same database, reachable from here
//	SUMI_CLOUD_PROOF_SESSION_SECRET the API's SUMI_BROWSER_SESSION_SECRET
//	SUMI_CLOUD_PROOF_AUDIENCE       the API's SUMI_BROWSER_SESSION_AUDIENCE
//	SUMI_CLOUD_PROOF_ORIGIN         one of SUMI_BROWSER_WS_ALLOWED_ORIGINS
//	SUMI_CLOUD_PROOF_REPLACE        shell command that replaces the API host
//	SUMI_CLOUD_PROOF_CORE_ONCE      optional shell command run with
//	                                SUMI_PERSONA_ID set, to let a secretary
//	                                answer before the replacement
//	SUMI_CLOUD_PROOF_EVIDENCE       optional path for a JSON summary
//
// The seeded Human and secretary are synthetic and identified as such.
func TestCloudReplacementProof(t *testing.T) {
	if os.Getenv("SUMI_CLOUD_PROOF") != "1" {
		t.Skip("SUMI_CLOUD_PROOF != 1")
	}
	env := func(name string) string {
		value := strings.TrimSpace(os.Getenv(name))
		if value == "" {
			t.Fatalf("%s is required", name)
		}
		return value
	}
	baseURL := strings.TrimRight(env("SUMI_CLOUD_PROOF_URL"), "/")
	origin := env("SUMI_CLOUD_PROOF_ORIGIN")
	secret, err := base64.StdEncoding.DecodeString(env("SUMI_CLOUD_PROOF_SESSION_SECRET"))
	if err != nil {
		t.Fatal("SUMI_CLOUD_PROOF_SESSION_SECRET must be standard base64")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, env("SUMI_CLOUD_PROOF_DB_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	evidence := map[string]any{"started_at": time.Now().UTC().Format(time.RFC3339Nano)}
	defer func() {
		path := strings.TrimSpace(os.Getenv("SUMI_CLOUD_PROOF_EVIDENCE"))
		if path == "" {
			return
		}
		evidence["passed"] = !t.Failed()
		raw, _ := json.MarshalIndent(evidence, "", "  ")
		_ = os.WriteFile(path, append(raw, '\n'), 0o600)
	}()

	started := time.Now()
	waitHealthy(t, baseURL, 3*time.Minute)
	evidence["initial_health_ms"] = time.Since(started).Milliseconds()

	run := uuid.NewString()[:8]
	reg, err := koseki.New(pool).AutoRegisterWithDisplayName(ctx, "firebase", "cloud-proof-"+run, "Cloud replacement proof "+run)
	if err != nil {
		t.Fatalf("register synthetic Human: %v", err)
	}
	var installationID string
	var epoch int64
	if err := pool.QueryRow(ctx, `
		SELECT installation_id::text, authority_epoch FROM app_installations
		WHERE owner_kind = 'human' AND owner_id = $1 AND app_id = 'direct-chat'`, reg.HumanID).Scan(&installationID, &epoch); err != nil {
		t.Fatalf("read Direct Chat installation: %v", err)
	}
	issuer, err := agentevents.NewHMACBrowserSessionIssuer(secret, env("SUMI_CLOUD_PROOF_AUDIENCE"))
	if err != nil {
		t.Fatal(err)
	}
	session, err := issuer.IssueSession(ctx, agentevents.UserSessionClaims{TenantID: "cloud-proof", UserID: reg.HumanID, PersonalityAgentID: reg.AgentID}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	evidence["human_id"], evidence["persona_id"] = reg.HumanID, reg.AgentID
	scope := fmt.Sprintf("installation_id=%s&authority_epoch=%d", installationID, epoch)

	send := func(key, text string) (int, map[string]any, time.Duration) {
		body, _ := json.Marshal(map[string]any{"type": "user_message", "text": text, "attachments": []any{}})
		req, _ := http.NewRequest(http.MethodPost, baseURL+"/direct-chat/commands?"+scope, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", origin)
		req.Header.Set("Idempotency-Key", key)
		req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: session})
		begin := time.Now()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("send %s: %v", key, err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var receipt map[string]any
		_ = json.Unmarshal(raw, &receipt)
		return resp.StatusCode, receipt, time.Since(begin)
	}
	history := func() map[string]any {
		req, _ := http.NewRequest(http.MethodGet, baseURL+"/direct-chat/history?"+scope, nil)
		req.Header.Set("Origin", origin)
		req.AddCookie(&http.Cookie{Name: agentevents.BrowserSessionCookie, Value: session})
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("history: %d %s", resp.StatusCode, raw)
		}
		var page map[string]any
		if err := json.Unmarshal(raw, &page); err != nil {
			t.Fatal(err)
		}
		return page
	}

	status, first, took := send("proof-"+run+"-1", "Please remember: the proof run is "+run)
	if status != http.StatusCreated {
		t.Fatalf("first command: %d %v", status, first)
	}
	evidence["first_command"], evidence["first_command_ms"] = first, took.Milliseconds()

	if command := strings.TrimSpace(os.Getenv("SUMI_CLOUD_PROOF_CORE_ONCE")); command != "" {
		begin := time.Now()
		out, err := shell(command, "SUMI_PERSONA_ID="+reg.AgentID)
		evidence["core_once_ms"] = time.Since(begin).Milliseconds()
		if err != nil {
			t.Fatalf("core once: %v\n%s", err, out)
		}
		// The API projects the committed reply into Direct Chat events.
		deadline := time.Now().Add(30 * time.Second)
		for len(eventTypes(history())) < 2 && time.Now().Before(deadline) {
			time.Sleep(250 * time.Millisecond)
		}
	}
	before := history()
	evidence["events_before"] = eventTypes(before)
	evidence["latest_seq_before"] = before["latest_seq"]

	begin := time.Now()
	if out, err := shell(env("SUMI_CLOUD_PROOF_REPLACE")); err != nil {
		t.Fatalf("replace API host: %v\n%s", err, out)
	}
	waitHealthy(t, baseURL, 3*time.Minute)
	evidence["replacement_to_healthy_ms"] = time.Since(begin).Milliseconds()

	after := history()
	evidence["events_after"] = eventTypes(after)
	if !jsonEqual(before["events"], after["events"]) || !jsonEqual(before["latest_seq"], after["latest_seq"]) {
		t.Fatalf("history changed across replacement:\nbefore %v\nafter  %v", eventTypes(before), eventTypes(after))
	}
	status, retried, _ := send("proof-"+run+"-1", "Please remember: the proof run is "+run)
	evidence["retried_command"] = retried
	if status != http.StatusCreated || retried["command_id"] != first["command_id"] || retried["seq"] != first["seq"] {
		t.Fatalf("retry after replacement: %d %v, want receipt %v", status, retried, first)
	}
	status, next, took := send("proof-"+run+"-2", "And a second message after the replacement")
	evidence["next_command"], evidence["next_command_ms"] = next, took.Milliseconds()
	firstSeq, _ := first["seq"].(float64)
	if status != http.StatusCreated || next["seq"] != firstSeq+1 {
		t.Fatalf("next command after replacement: %d %v", status, next)
	}
}

func waitHealthy(t *testing.T, baseURL string, limit time.Duration) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for {
		resp, err := http.Get(baseURL + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("API at %s not healthy within %s (last: %v)", baseURL, limit, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func shell(command string, extraEnv ...string) ([]byte, error) {
	cmd := exec.Command("sh", "-c", command)
	cmd.Env = append(os.Environ(), extraEnv...)
	return cmd.CombinedOutput()
}

func eventTypes(page map[string]any) []string {
	events, _ := page["events"].([]any)
	types := make([]string, 0, len(events))
	for _, raw := range events {
		event, _ := raw.(map[string]any)
		inner, _ := event["event"].(map[string]any)
		kind, _ := inner["type"].(string)
		if kind == "" {
			kind, _ = event["type"].(string)
		}
		types = append(types, kind)
	}
	return types
}

func jsonEqual(a, b any) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}
