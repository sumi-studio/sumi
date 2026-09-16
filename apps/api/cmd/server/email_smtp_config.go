package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/sumi-studio/sumi/apps/api/internal/authemail"
)

// smtpSenderFromEnv builds the production SMTP sender. TLS mode is
// "starttls" (the submission standard, default port 587) or "implicit"
// (SMTPS, default port 465); when unset it defaults to implicit on port 465
// and STARTTLS otherwise. The password comes from exactly one of
// SUMI_AUTH_EMAIL_SMTP_PASSWORD or SUMI_AUTH_EMAIL_SMTP_PASSWORD_FILE so
// deployments can keep the secret out of the environment. No variable here
// may carry a credential onto a cleartext connection: the sender requires
// TLS and refuses to authenticate without it.
func smtpSenderFromEnv() (*authemail.SMTPSender, error) {
	host := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_SMTP_HOST"))
	if host == "" {
		return nil, errors.New("SUMI_AUTH_EMAIL_SMTP_HOST is required for the smtp sender")
	}
	tlsMode := strings.ToLower(strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_SMTP_TLS")))
	portRaw := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_SMTP_PORT"))
	if tlsMode == "" {
		if portRaw == "465" {
			tlsMode = "implicit"
		} else {
			tlsMode = "starttls"
		}
	}
	var implicit bool
	switch tlsMode {
	case "implicit":
		implicit = true
	case "starttls":
	default:
		return nil, fmt.Errorf("SUMI_AUTH_EMAIL_SMTP_TLS must be \"implicit\" or \"starttls\", not %q", tlsMode)
	}
	port := 587
	if implicit {
		port = 465
	}
	if portRaw != "" {
		parsed, err := strconv.Atoi(portRaw)
		if err != nil || parsed < 1 || parsed > 65535 {
			return nil, errors.New("SUMI_AUTH_EMAIL_SMTP_PORT must be a port number")
		}
		port = parsed
	}
	username := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_SMTP_USERNAME"))
	if username == "" {
		return nil, errors.New("SUMI_AUTH_EMAIL_SMTP_USERNAME is required for the smtp sender")
	}
	password := os.Getenv("SUMI_AUTH_EMAIL_SMTP_PASSWORD")
	passwordFile := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_SMTP_PASSWORD_FILE"))
	if password != "" && passwordFile != "" {
		return nil, errors.New("set only one of SUMI_AUTH_EMAIL_SMTP_PASSWORD and SUMI_AUTH_EMAIL_SMTP_PASSWORD_FILE")
	}
	if passwordFile != "" {
		raw, err := os.ReadFile(passwordFile)
		if err != nil {
			return nil, fmt.Errorf("SUMI_AUTH_EMAIL_SMTP_PASSWORD_FILE: %w", err)
		}
		password = strings.TrimRight(string(raw), "\r\n")
	}
	if password == "" {
		return nil, errors.New("SUMI_AUTH_EMAIL_SMTP_PASSWORD or SUMI_AUTH_EMAIL_SMTP_PASSWORD_FILE is required for the smtp sender")
	}
	from := strings.TrimSpace(os.Getenv("SUMI_AUTH_EMAIL_SMTP_FROM"))
	if from == "" {
		return nil, errors.New("SUMI_AUTH_EMAIL_SMTP_FROM is required for the smtp sender")
	}
	sender := &authemail.SMTPSender{
		Host: host, Port: port, ImplicitTLS: implicit,
		Username: username, Password: password, From: from,
	}
	// Surfacing an invalid address or host at startup keeps an operator error
	// from appearing later as per-delivery permanent failures.
	if err := sender.Validate(); err != nil {
		return nil, fmt.Errorf("smtp sender configuration: %w", err)
	}
	log.Printf("auth email uses smtp sender host %s port %d tls %s", host, port, tlsMode)
	return sender, nil
}
