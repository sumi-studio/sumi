package main

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sumi-studio/sumi/apps/api/internal/authemail"
)

// emailAuthEnv clears and sets the email-auth environment group so each case
// starts from a known configuration.
func emailAuthEnv(t *testing.T, sender string) {
	t.Helper()
	for _, name := range []string{
		"SUMI_AUTH_EMAIL_CHALLENGE_KEY", "SUMI_AUTH_EMAIL_CHALLENGE_KEY_ID",
		"SUMI_AUTH_EMAIL_SENDER", "SUMI_AUTH_EMAIL_LINK_ORIGIN",
		"SUMI_AUTH_EMAIL_DEV_MAILBOX_DIR",
		"SUMI_AUTH_EMAIL_SMTP_HOST", "SUMI_AUTH_EMAIL_SMTP_PORT",
		"SUMI_AUTH_EMAIL_SMTP_TLS", "SUMI_AUTH_EMAIL_SMTP_USERNAME",
		"SUMI_AUTH_EMAIL_SMTP_PASSWORD", "SUMI_AUTH_EMAIL_SMTP_PASSWORD_FILE",
		"SUMI_AUTH_EMAIL_SMTP_FROM", "FIREBASE_AUTH_EMULATOR_HOST",
	} {
		t.Setenv(name, "")
	}
	if sender != "" {
		t.Setenv("SUMI_AUTH_EMAIL_CHALLENGE_KEY", base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")))
		t.Setenv("SUMI_AUTH_EMAIL_CHALLENGE_KEY_ID", "test-mail-2026-09")
		t.Setenv("SUMI_AUTH_EMAIL_SENDER", sender)
		t.Setenv("SUMI_AUTH_EMAIL_LINK_ORIGIN", "https://app.example.test")
	}
}

func smtpEnv(t *testing.T) {
	t.Helper()
	emailAuthEnv(t, "smtp")
	t.Setenv("SUMI_AUTH_EMAIL_SMTP_HOST", "smtp.example.test")
	t.Setenv("SUMI_AUTH_EMAIL_SMTP_USERNAME", "resend")
	t.Setenv("SUMI_AUTH_EMAIL_SMTP_PASSWORD", "re_placeholder_key")
	t.Setenv("SUMI_AUTH_EMAIL_SMTP_FROM", "Sumi <login@example.test>")
}

func TestSMTPConfigBuildsSenderWithDefaults(t *testing.T) {
	smtpEnv(t)
	config, err := emailAuthConfigFromEnv([]string{"https://app.example.test"}, true)
	if err != nil || config == nil {
		t.Fatalf("smtp config: %+v %v", config, err)
	}
	sender, ok := config.sender.(*authemail.SMTPSender)
	if !ok {
		t.Fatalf("sender type: %T", config.sender)
	}
	if sender.Host != "smtp.example.test" || sender.Port != 587 || sender.ImplicitTLS ||
		sender.Username != "resend" || sender.Password != "re_placeholder_key" || sender.From != "Sumi <login@example.test>" {
		t.Fatalf("sender fields: %+v", sender)
	}
	// The secure-cookie production posture must not reject the real sender.
	if sender.TLSConfig != nil {
		t.Fatal("production sender must use the system roots, not an injected pool")
	}
}

func TestSMTPConfigTLSAndPortResolution(t *testing.T) {
	for _, tc := range []struct {
		name     string
		tls      string
		port     string
		wantTLS  bool
		wantPort int
	}{
		{"explicit implicit", "implicit", "", true, 465},
		{"explicit starttls", "starttls", "", false, 587},
		{"port 465 implies smtps", "", "465", true, 465},
		{"explicit port with starttls", "starttls", "2465", false, 2465},
		{"explicit implicit on 587", "implicit", "587", true, 587},
	} {
		t.Run(tc.name, func(t *testing.T) {
			smtpEnv(t)
			t.Setenv("SUMI_AUTH_EMAIL_SMTP_TLS", tc.tls)
			t.Setenv("SUMI_AUTH_EMAIL_SMTP_PORT", tc.port)
			sender, err := smtpSenderFromEnv()
			if err != nil {
				t.Fatal(err)
			}
			if sender.ImplicitTLS != tc.wantTLS || sender.Port != tc.wantPort {
				t.Fatalf("tls=%v port=%d, want implicit=%v port=%d", sender.ImplicitTLS, sender.Port, tc.wantTLS, tc.wantPort)
			}
		})
	}
}

func TestSMTPConfigRejectsIncompleteOrContradictory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T)
		want   string
	}{
		{"missing host", func(t *testing.T) { t.Setenv("SUMI_AUTH_EMAIL_SMTP_HOST", "") }, "SMTP_HOST"},
		{"missing username", func(t *testing.T) { t.Setenv("SUMI_AUTH_EMAIL_SMTP_USERNAME", "") }, "SMTP_USERNAME"},
		{"missing password", func(t *testing.T) { t.Setenv("SUMI_AUTH_EMAIL_SMTP_PASSWORD", "") }, "SMTP_PASSWORD"},
		{"both password sources", func(t *testing.T) {
			t.Setenv("SUMI_AUTH_EMAIL_SMTP_PASSWORD_FILE", "/nonexistent")
		}, "only one"},
		{"missing from", func(t *testing.T) { t.Setenv("SUMI_AUTH_EMAIL_SMTP_FROM", "") }, "SMTP_FROM"},
		{"invalid from", func(t *testing.T) { t.Setenv("SUMI_AUTH_EMAIL_SMTP_FROM", "not a mailbox") }, "configuration"},
		{"bad tls mode", func(t *testing.T) { t.Setenv("SUMI_AUTH_EMAIL_SMTP_TLS", "off") }, "SMTP_TLS"},
		{"bad port", func(t *testing.T) { t.Setenv("SUMI_AUTH_EMAIL_SMTP_PORT", "smtp") }, "SMTP_PORT"},
		{"host with port", func(t *testing.T) { t.Setenv("SUMI_AUTH_EMAIL_SMTP_HOST", "smtp.example.test:465") }, "configuration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			smtpEnv(t)
			tc.mutate(t)
			_, err := smtpSenderFromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q error, got %v", tc.want, err)
			}
		})
	}
}

func TestSMTPConfigPasswordFile(t *testing.T) {
	smtpEnv(t)
	secret := filepath.Join(t.TempDir(), "smtp-password")
	if err := os.WriteFile(secret, []byte("re_file_secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUMI_AUTH_EMAIL_SMTP_PASSWORD", "")
	t.Setenv("SUMI_AUTH_EMAIL_SMTP_PASSWORD_FILE", secret)
	sender, err := smtpSenderFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if sender.Password != "re_file_secret" {
		t.Fatalf("password file content: %q", sender.Password)
	}
}

func TestEmailAuthConfigStillBoundedWithoutSMTP(t *testing.T) {
	emailAuthEnv(t, "")
	if config, err := emailAuthConfigFromEnv([]string{"https://app.example.test"}, true); config != nil || err != nil {
		t.Fatalf("unconfigured email auth must stay off: %+v %v", config, err)
	}
	// dev-mailbox remains the local-only sender: rejected under secure cookies.
	emailAuthEnv(t, "dev-mailbox")
	t.Setenv("SUMI_AUTH_EMAIL_DEV_MAILBOX_DIR", t.TempDir())
	if _, err := emailAuthConfigFromEnv([]string{"https://app.example.test"}, true); err == nil {
		t.Fatal("dev-mailbox accepted with secure cookies and no emulator")
	}
	emailAuthEnv(t, "pigeon")
	if _, err := emailAuthConfigFromEnv([]string{"https://app.example.test"}, true); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("unknown sender: %v", err)
	}
	// The four enabling variables still have to be configured together.
	emailAuthEnv(t, "")
	t.Setenv("SUMI_AUTH_EMAIL_SENDER", "smtp")
	if _, err := emailAuthConfigFromEnv([]string{"https://app.example.test"}, true); err == nil || !strings.Contains(err.Error(), "together") {
		t.Fatalf("partial group: %v", err)
	}
}
