package authemail

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRenderChallengeLeadsWithCodeAndKeepsLinkAlternative(t *testing.T) {
	sent := time.Date(2026, 9, 15, 3, 4, 0, 0, time.UTC)
	message := RenderChallenge(Challenge{
		To: "person@example.com", Code: "012345", LinkURL: "https://sumi.example/email-sign-in#challenge=c&token=t",
		ExpiresAt: sent.Add(10 * time.Minute), SentAt: sent,
	})
	if message.To != "person@example.com" || !strings.Contains(message.Subject, "012345") {
		t.Fatalf("message header: %+v", message)
	}
	codeAt, linkAt := strings.Index(message.Text, "012345"), strings.Index(message.Text, "https://sumi.example/email-sign-in#")
	if codeAt < 0 || linkAt < 0 || codeAt > linkAt {
		t.Fatalf("code must precede the link alternative:\n%s", message.Text)
	}
	if !strings.Contains(message.Text, "12:14") || !strings.Contains(message.Text, "12:04") {
		t.Fatalf("expiry and send time are not shown in JST:\n%s", message.Text)
	}
}

func TestDevMailboxWritesPrivateLocalFilesOnly(t *testing.T) {
	if err := (&DevMailbox{Dir: "relative"}).Send(context.Background(), Message{To: "a@example.com"}); err == nil {
		t.Fatal("relative development mailbox accepted")
	}
	dir := filepath.Join(t.TempDir(), "mailbox")
	mailbox := &DevMailbox{Dir: dir}
	if err := mailbox.Send(context.Background(), Message{To: "a@example.com", Subject: "subject", Text: "body"}); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("mailbox entries: %v %v", entries, err)
	}
	info, err := entries[0].Info()
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mailbox file mode: %v %v", info.Mode(), err)
	}
	dirInfo, err := os.Stat(dir)
	if err != nil || dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("mailbox dir mode: %v %v", dirInfo.Mode(), err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var stored map[string]any
	if err := json.Unmarshal(raw, &stored); err != nil || stored["to"] != "a@example.com" || stored["text"] != "body" {
		t.Fatalf("stored message: %s %v", raw, err)
	}
}

func TestPermanentSendErrorsAreClassified(t *testing.T) {
	base := errors.New("mailbox rejected")
	if IsPermanent(base) || !IsPermanent(Permanent(base)) || !errors.Is(Permanent(base), base) || IsPermanent(nil) {
		t.Fatal("permanent classification")
	}
}
