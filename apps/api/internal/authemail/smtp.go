package authemail

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"strconv"
	"strings"
	"time"
)

// smtpDefaultTimeout bounds one send when the caller's context has no earlier
// deadline. The delivery worker already bounds Send to 30 seconds, inside the
// one-minute queue lease.
const smtpDefaultTimeout = 30 * time.Second

// SMTPSender delivers challenge messages through an authenticated SMTP
// submission endpoint. TLS is mandatory: ImplicitTLS connects with TLS
// immediately (SMTPS, typically port 465); otherwise STARTTLS is required
// before authentication (typically port 587). A server that offers neither
// fails the delivery permanently rather than sending credentials in
// cleartext. Certificate verification is never disabled: TLSConfig defaults
// to the system roots with the host name verified.
type SMTPSender struct {
	Host        string // DNS name or IP, without a port suffix
	Port        int
	ImplicitTLS bool
	Username    string
	Password    string
	From        string // RFC 5322 mailbox, for example "Sumi <login@example.com>"
	LocalName   string // EHLO identifier; "localhost" when empty
	Timeout     time.Duration
	// TLSConfig optionally overrides the TLS client configuration (tests).
	// When nil, the system roots authenticate Host.
	TLSConfig *tls.Config
}

var _ Sender = (*SMTPSender)(nil)

// Validate checks the static configuration without connecting. Send runs the
// same check and reports it as a permanent failure.
func (s *SMTPSender) Validate() error {
	_, err := s.validated()
	return err
}

func (s *SMTPSender) validated() (*mail.Address, error) {
	if s == nil {
		return nil, errors.New("smtp sender is not configured")
	}
	if strings.TrimSpace(s.Host) == "" || strings.ContainsAny(s.Host, ": \t\r\n/") {
		return nil, errors.New("smtp host must be a host name or IP without a port")
	}
	if s.Port < 1 || s.Port > 65535 {
		return nil, errors.New("smtp port must be between 1 and 65535")
	}
	if s.Username == "" || s.Password == "" {
		return nil, errors.New("smtp username and password are required")
	}
	from, err := mail.ParseAddress(s.From)
	if err != nil || from == nil || from.Address == "" {
		return nil, errors.New("smtp from must be a valid mailbox such as \"Sumi <login@example.com>\"")
	}
	if strings.ContainsAny(from.Name+from.Address, "\r\n") {
		return nil, errors.New("smtp from must not contain line breaks")
	}
	return from, nil
}

func (s *SMTPSender) tlsConfig() *tls.Config {
	if s.TLSConfig != nil {
		return s.TLSConfig
	}
	return &tls.Config{ServerName: s.Host, MinVersion: tls.VersionTLS12}
}

// Send performs one complete SMTP submission: connect, TLS, AUTH PLAIN, then
// MAIL/RCPT/DATA for the single recipient. A server rejection (5xx) is
// permanent; a deferral (4xx) or a network failure is retried by the delivery
// queue. When the connection drops after the message bytes were sent but
// before the server's reply, the outcome is genuinely unknown: the error is
// left retryable, and the queue's at-least-once retry may deliver a duplicate
// of the same challenge — harmless here because resending never mints a new
// code. Send never logs the message, code, link, or credentials.
func (s *SMTPSender) Send(ctx context.Context, message Message) error {
	from, err := s.validated()
	if err != nil {
		return Permanent(err)
	}
	to, err := mail.ParseAddress(message.To)
	if err != nil || to == nil || to.Address == "" {
		return Permanent(fmt.Errorf("recipient address %q is not a valid mailbox", message.To))
	}
	if strings.ContainsAny(to.Address, "\r\n") || strings.ContainsAny(message.Subject, "\r\n") {
		return Permanent(errors.New("recipient or subject contains a line break"))
	}
	raw := smtpMessageBytes(from, to.Address, message)

	timeout := s.Timeout
	if timeout <= 0 {
		timeout = smtpDefaultTimeout
	}
	sendCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var dialer net.Dialer
	conn, err := dialer.DialContext(sendCtx, "tcp", net.JoinHostPort(s.Host, strconv.Itoa(s.Port)))
	if err != nil {
		return smtpError(sendCtx, err)
	}
	defer conn.Close()
	if deadline, ok := sendCtx.Deadline(); ok {
		// The single deadline covers the banner, STARTTLS negotiation,
		// authentication, and the message body, keeping the whole
		// submission inside the delivery lease.
		_ = conn.SetDeadline(deadline)
	}
	cancelled := make(chan struct{})
	go func(rawConn net.Conn) {
		select {
		case <-sendCtx.Done():
			_ = rawConn.Close()
		case <-cancelled:
		}
	}(conn)
	defer close(cancelled)

	tlsConfig := s.tlsConfig()
	if s.ImplicitTLS {
		tlsConn := tls.Client(conn, tlsConfig)
		if err := tlsConn.HandshakeContext(sendCtx); err != nil {
			return smtpError(sendCtx, err)
		}
		conn = tlsConn
	}
	client, err := smtp.NewClient(conn, s.Host)
	if err != nil {
		return smtpError(sendCtx, err)
	}
	defer client.Close()
	if s.LocalName != "" {
		if err := client.Hello(s.LocalName); err != nil {
			return smtpError(sendCtx, err)
		}
	}
	if !s.ImplicitTLS {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return Permanent(errors.New("smtp server does not offer STARTTLS; refusing to authenticate on a cleartext connection"))
		}
		if err := client.StartTLS(tlsConfig); err != nil {
			return smtpError(sendCtx, err)
		}
	}
	ok, authParams := client.Extension("AUTH")
	if !ok {
		return Permanent(errors.New("smtp server does not offer AUTH"))
	}
	if !strings.Contains(" "+authParams+" ", " PLAIN") {
		return Permanent(fmt.Errorf("smtp server does not offer AUTH PLAIN (offers %q)", authParams))
	}
	if err := client.Auth(smtp.PlainAuth("", s.Username, s.Password, s.Host)); err != nil {
		return smtpError(sendCtx, err)
	}
	if err := client.Mail(from.Address); err != nil {
		return smtpError(sendCtx, err)
	}
	if err := client.Rcpt(to.Address); err != nil {
		return smtpError(sendCtx, err)
	}
	writer, err := client.Data()
	if err != nil {
		return smtpError(sendCtx, err)
	}
	if _, err := writer.Write(raw); err != nil {
		_ = writer.Close()
		return smtpError(sendCtx, err)
	}
	if err := writer.Close(); err != nil {
		var protoErr *textproto.Error
		if errors.As(err, &protoErr) {
			return smtpError(sendCtx, err)
		}
		if bound := sendBound(sendCtx, err); bound != nil {
			return bound
		}
		return fmt.Errorf("smtp server closed the connection without a delivery verdict; the message may still have been accepted: %w", err)
	}
	_ = client.Quit()
	return nil
}

// sendBound reports the context bound when it, or the connection deadline
// derived from it, stopped the send.
func sendBound(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return context.DeadlineExceeded
	}
	return nil
}

// smtpError maps context and protocol failures onto the Sender contract:
// SMTP 5xx replies are permanent; everything else stays retryable. Errors
// after the message body was accepted for transmission are handled by the
// caller's acknowledgement check.
func smtpError(ctx context.Context, err error) error {
	if bound := sendBound(ctx, err); bound != nil {
		return bound
	}
	var protoErr *textproto.Error
	if errors.As(err, &protoErr) && protoErr.Code >= 500 && protoErr.Code <= 599 {
		return Permanent(fmt.Errorf("smtp server refused the request: %w", err))
	}
	return err
}

// smtpMessageBytes renders a minimal RFC 5322 message. Header values are
// already validated free of line breaks; the body is normalized to CRLF and
// base64-encoded so UTF-8 Japanese text survives every relay. The smtp
// client's data writer performs dot-stuffing.
func smtpMessageBytes(from *mail.Address, to string, message Message) []byte {
	var b bytes.Buffer
	b.WriteString("From: " + from.String() + "\r\n")
	b.WriteString("To: <" + to + ">\r\n")
	b.WriteString("Subject: " + mime.QEncoding.Encode("utf-8", message.Subject) + "\r\n")
	b.WriteString("Date: " + time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 +0000") + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=utf-8\r\n")
	b.WriteString("Content-Transfer-Encoding: base64\r\n")
	b.WriteString("\r\n")
	body := strings.ReplaceAll(message.Text, "\r\n", "\n")
	body = strings.ReplaceAll(body, "\r", "\n")
	encoded := base64.StdEncoding.EncodeToString([]byte(body))
	for len(encoded) > 76 {
		b.WriteString(encoded[:76] + "\r\n")
		encoded = encoded[76:]
	}
	b.WriteString(encoded)
	b.WriteString("\r\n")
	return b.Bytes()
}
