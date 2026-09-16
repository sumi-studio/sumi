package authemail

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// smtpFixtureServer is a disposable, in-process SMTP submission peer used to
// validate SMTPSender against the real wire protocol: TLS handshakes, STARTTLS
// negotiation, AUTH PLAIN, and the DATA transcript. It never touches a real
// network beyond a loopback listener on a kernel-assigned port.
type smtpFixtureServer struct {
	ln        net.Listener
	serverTLS *tls.Config
	implicit  bool
	starttls  bool
	user      string
	pass      string

	mu            sync.Mutex
	rcptCode      int
	mailCode      int
	dataFinalCode int
	dropAfterData bool
	stall         bool
	done          chan struct{}

	commands      []string
	mailFrom      []string
	rcptTo        []string
	data          [][]string
	authUser      string
	authPass      string
	authOnTLS     bool
	plaintextAuth bool
}

func newSMTPFixtureServer(t *testing.T, implicit, starttls bool) (*smtpFixtureServer, *x509.CertPool) {
	t.Helper()
	serverTLS, pool := smtpFixtureTLS(t)
	f := &smtpFixtureServer{
		serverTLS: serverTLS,
		implicit:  implicit,
		starttls:  starttls,
		user:      "fixture-user",
		pass:      "fixture-password",
		done:      make(chan struct{}),
	}
	var err error
	f.ln, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		close(f.done)
		_ = f.ln.Close()
	})
	go func() {
		for {
			conn, err := f.ln.Accept()
			if err != nil {
				return
			}
			go f.serve(conn)
		}
	}()
	return f, pool
}

// smtpFixtureTLS issues a self-signed loopback certificate. The sender's
// default TLS configuration (system roots) must reject it; tests that expect
// a handshake pass the returned pool explicitly.
func smtpFixtureTLS(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(20260917),
		Subject:               pkix.Name{CommonName: "sumi-auth-smtp-20260917 fixture"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}},
		MinVersion:   tls.VersionTLS12,
	}, pool
}

func (f *smtpFixtureServer) serve(conn net.Conn) {
	defer conn.Close()
	f.mu.Lock()
	stall := f.stall
	f.mu.Unlock()
	if stall {
		<-f.done
		return
	}
	tlsActive := false
	if f.implicit {
		conn = tls.Server(conn, f.serverTLS)
		tlsActive = true
	}
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	reply := func(format string, args ...any) {
		fmt.Fprintf(writer, format+"\r\n", args...)
		_ = writer.Flush()
	}
	reply("220 fixture.test ESMTP sumi-auth-smtp-20260917")
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		verb, arg, _ := strings.Cut(line, " ")
		verb = strings.ToUpper(verb)
		f.mu.Lock()
		f.commands = append(f.commands, verb)
		f.mu.Unlock()
		switch verb {
		case "EHLO", "HELO":
			f.mu.Lock()
			features := []string{"fixture.test"}
			if f.starttls && !tlsActive {
				features = append(features, "STARTTLS")
			}
			if tlsActive {
				features = append(features, "AUTH PLAIN")
			}
			features = append(features, "8BITMIME")
			f.mu.Unlock()
			for i, feature := range features {
				separator := "-"
				if i == len(features)-1 {
					separator = " "
				}
				fmt.Fprintf(writer, "250%s%s\r\n", separator, feature)
			}
			_ = writer.Flush()
		case "STARTTLS":
			if !f.starttls || tlsActive {
				reply("454 TLS not available")
				continue
			}
			reply("220 Ready to start TLS")
			tlsConn := tls.Server(conn, f.serverTLS)
			if err := tlsConn.Handshake(); err != nil {
				return
			}
			conn = tlsConn
			reader = bufio.NewReader(conn)
			writer = bufio.NewWriter(conn)
			tlsActive = true
		case "AUTH":
			fields := strings.Fields(arg)
			if len(fields) == 0 || !strings.EqualFold(fields[0], "PLAIN") {
				reply("504 5.5.4 unsupported mechanism")
				continue
			}
			initial := ""
			if len(fields) > 1 {
				initial = fields[1]
			} else {
				reply("334 ")
				response, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				initial = strings.TrimRight(response, "\r\n")
			}
			decoded, err := base64.StdEncoding.DecodeString(initial)
			parts := strings.Split(string(decoded), "\x00")
			f.mu.Lock()
			f.authOnTLS = tlsActive
			f.plaintextAuth = f.plaintextAuth || !tlsActive
			if err == nil && len(parts) == 3 {
				f.authUser, f.authPass = parts[1], parts[2]
			}
			ok := err == nil && len(parts) == 3 && parts[1] == f.user && parts[2] == f.pass
			f.mu.Unlock()
			if ok {
				reply("235 2.7.0 authentication succeeded")
			} else {
				reply("535 5.7.8 authentication credentials invalid")
			}
		case "MAIL":
			f.mu.Lock()
			f.mailFrom = append(f.mailFrom, arg)
			code := f.mailCode
			f.mu.Unlock()
			if code == 0 {
				code = 250
			}
			reply("%d fixture reply", code)
		case "RCPT":
			f.mu.Lock()
			f.rcptTo = append(f.rcptTo, arg)
			code := f.rcptCode
			f.mu.Unlock()
			if code == 0 {
				code = 250
			}
			reply("%d fixture reply", code)
		case "DATA":
			reply("354 end with <CR><LF>.<CR><LF>")
			var body []string
			for {
				dataLine, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if strings.HasPrefix(dataLine, "..") {
					dataLine = dataLine[1:]
				}
				if strings.TrimRight(dataLine, "\r\n") == "." {
					break
				}
				body = append(body, dataLine)
			}
			f.mu.Lock()
			f.data = append(f.data, body)
			drop := f.dropAfterData
			code := f.dataFinalCode
			f.mu.Unlock()
			if drop {
				return
			}
			if code == 0 {
				code = 250
			}
			reply("%d fixture reply", code)
		case "RSET", "NOOP":
			reply("250 ok")
		case "QUIT":
			reply("221 bye")
			return
		default:
			reply("502 5.5.2 command not recognized")
		}
	}
}

// script mutates the scripted failure knobs under the capture mutex so
// test writes stay ordered with the serving goroutines under -race.
func (f *smtpFixtureServer) script(fn func(*smtpFixtureServer)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func (f *smtpFixtureServer) addr() string { return f.ln.Addr().String() }

func (f *smtpFixtureServer) hostPort() (string, int) {
	host, port, _ := net.SplitHostPort(f.addr())
	var portNum int
	fmt.Sscanf(port, "%d", &portNum)
	return host, portNum
}

func (f *smtpFixtureServer) snapshot() (commands, mailFrom, rcptTo []string, data [][]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.commands...), append([]string(nil), f.mailFrom...),
		append([]string(nil), f.rcptTo...), append([][]string(nil), f.data...)
}

func (f *smtpFixtureServer) auth() (user, pass string, onTLS, plaintext bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.authUser, f.authPass, f.authOnTLS, f.plaintextAuth
}

func fixtureSender(f *smtpFixtureServer, pool *x509.CertPool) *SMTPSender {
	host, port := f.hostPort()
	return &SMTPSender{
		Host:        host,
		Port:        port,
		ImplicitTLS: f.implicit,
		Username:    f.user,
		Password:    f.pass,
		From:        "\"Sumi ログイン\" <login@example.test>",
		TLSConfig:   &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12},
	}
}

func TestSMTPSenderImplicitTLSMessageBytes(t *testing.T) {
	server, pool := newSMTPFixtureServer(t, true, false)
	sender := fixtureSender(server, pool)
	message := RenderChallenge(Challenge{
		To: "person@example.test", Code: "654321",
		LinkURL:   "https://sumi.example/email-sign-in#challenge=c&token=t",
		ExpiresAt: time.Now().Add(10 * time.Minute), SentAt: time.Now(),
	})
	if err := sender.Send(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	commands, mailFrom, rcptTo, data := server.snapshot()
	user, pass, onTLS, plaintext := server.auth()
	if user != "fixture-user" || pass != "fixture-password" || !onTLS || plaintext {
		t.Fatalf("auth: user=%q tls=%v plaintext=%v", user, onTLS, plaintext)
	}
	if len(mailFrom) != 1 || !strings.Contains(mailFrom[0], "FROM:<login@example.test>") {
		t.Fatalf("mail from: %v", mailFrom)
	}
	if len(rcptTo) != 1 || !strings.Contains(rcptTo[0], "TO:<person@example.test>") {
		t.Fatalf("rcpt to: %v", rcptTo)
	}
	if len(data) != 1 {
		t.Fatalf("data payloads: %d", len(data))
	}
	joined := strings.Join(data[0], "")
	for _, want := range []string{
		"To: <person@example.test>",
		"MIME-Version: 1.0",
		"Content-Type: text/plain; charset=utf-8",
		"Content-Transfer-Encoding: base64",
		"Date: ",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("message missing %q:\n%s", want, joined)
		}
	}
	// Japanese display name and subject must travel as RFC 2047 encoded words.
	if !strings.Contains(joined, "=?utf-8?") || !strings.Contains(joined, "From: =?utf-8?") {
		t.Fatalf("headers are not encoded-word safe:\n%s", joined)
	}
	headerEnd := strings.Index(joined, "\r\n\r\n")
	if headerEnd < 0 {
		t.Fatalf("message has no header/body separator:\n%s", joined)
	}
	body, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(joined[headerEnd+4:], "\r\n", ""))
	if err != nil {
		t.Fatalf("body is not valid base64: %v", err)
	}
	if !strings.Contains(string(body), "確認コード: 654321") || !strings.Contains(string(body), "https://sumi.example/email-sign-in#challenge=") {
		t.Fatalf("decoded body lost the code or link:\n%s", body)
	}
	sequence := strings.Join(commands, ",")
	if !strings.Contains(sequence, "EHLO,AUTH,MAIL,RCPT,DATA") {
		t.Fatalf("unexpected command sequence: %s", sequence)
	}
}

func TestSMTPSenderStartTLSNegotiatesBeforeAuth(t *testing.T) {
	server, pool := newSMTPFixtureServer(t, false, true)
	sender := fixtureSender(server, pool)
	err := sender.Send(context.Background(), Message{To: "person@example.test", Subject: "s", Text: "body"})
	if err != nil {
		t.Fatal(err)
	}
	commands, _, _, _ := server.snapshot()
	sequence := strings.Join(commands, ",")
	startAt := strings.Index(sequence, "STARTTLS")
	authAt := strings.Index(sequence, "AUTH")
	if startAt < 0 || authAt < 0 || startAt > authAt {
		t.Fatalf("STARTTLS must precede AUTH: %s", sequence)
	}
	_, _, onTLS, plaintext := server.auth()
	if !onTLS || plaintext {
		t.Fatalf("credentials outside TLS: onTLS=%v plaintext=%v", onTLS, plaintext)
	}
}

func TestSMTPSenderRefusesCleartextServer(t *testing.T) {
	server, pool := newSMTPFixtureServer(t, false, false)
	sender := fixtureSender(server, pool)
	err := sender.Send(context.Background(), Message{To: "person@example.test", Subject: "s", Text: "body"})
	if err == nil || !IsPermanent(err) || !strings.Contains(err.Error(), "STARTTLS") {
		t.Fatalf("expected permanent STARTTLS refusal: %v", err)
	}
	_, pass, _, plaintext := server.auth()
	if plaintext || pass == "fixture-password" {
		t.Fatal("credentials were offered on a cleartext connection")
	}
	commands, _, _, _ := server.snapshot()
	for _, verb := range commands {
		if verb == "AUTH" || verb == "MAIL" {
			t.Fatalf("client proceeded without TLS: %v", commands)
		}
	}
}

func TestSMTPSenderRejectsUntrustedCertificate(t *testing.T) {
	server, _ := newSMTPFixtureServer(t, true, false)
	host, port := server.hostPort()
	// No TLSConfig: the sender must verify against the system roots and fail.
	sender := &SMTPSender{
		Host: host, Port: port, ImplicitTLS: true,
		Username: "fixture-user", Password: "fixture-password", From: "login@example.test",
	}
	err := sender.Send(context.Background(), Message{To: "person@example.test", Subject: "s", Text: "body"})
	if err == nil {
		t.Fatal("self-signed server certificate was trusted")
	}
}

func TestSMTPSenderSMTPResponseClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*smtpFixtureServer)
		permanent bool
	}{
		{"recipient rejected permanently", func(f *smtpFixtureServer) { f.rcptCode = 550 }, true},
		{"recipient deferred", func(f *smtpFixtureServer) { f.rcptCode = 451 }, false},
		{"sender address deferred", func(f *smtpFixtureServer) { f.mailCode = 421 }, false},
		{"content rejected permanently", func(f *smtpFixtureServer) { f.dataFinalCode = 554 }, true},
		{"content deferred", func(f *smtpFixtureServer) { f.dataFinalCode = 451 }, false},
		{"lost acknowledgement", func(f *smtpFixtureServer) { f.dropAfterData = true }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, pool := newSMTPFixtureServer(t, true, false)
			server.script(tc.configure)
			sender := fixtureSender(server, pool)
			err := sender.Send(context.Background(), Message{To: "person@example.test", Subject: "s", Text: "body"})
			if err == nil {
				t.Fatal("expected a send error")
			}
			if IsPermanent(err) != tc.permanent {
				t.Fatalf("permanent=%v for error %v", IsPermanent(err), err)
			}
			if tc.name == "lost acknowledgement" && !strings.Contains(err.Error(), "verdict") {
				t.Fatalf("lost acknowledgement is not diagnosed honestly: %v", err)
			}
		})
	}
}

func TestSMTPSenderAuthFailureIsPermanent(t *testing.T) {
	server, pool := newSMTPFixtureServer(t, true, false)
	sender := fixtureSender(server, pool)
	sender.Password = "wrong-password"
	err := sender.Send(context.Background(), Message{To: "person@example.test", Subject: "s", Text: "body"})
	if err == nil || !IsPermanent(err) {
		t.Fatalf("rejected credentials must fail permanently: %v", err)
	}
	_, pass, onTLS, _ := server.auth()
	if pass != "wrong-password" || !onTLS {
		t.Fatalf("auth attempt not observed over TLS: pass=%q onTLS=%v", pass, onTLS)
	}
}

func TestSMTPSenderHonorsContextDeadline(t *testing.T) {
	server, pool := newSMTPFixtureServer(t, true, false)
	server.script(func(f *smtpFixtureServer) { f.stall = true })
	sender := fixtureSender(server, pool)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := sender.Send(ctx, Message{To: "person@example.test", Subject: "s", Text: "body"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled peer must surface the context deadline: %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("stalled send was not bounded by the context")
	}
}

func TestSMTPSenderValidatesConfiguration(t *testing.T) {
	valid := func() *SMTPSender {
		return &SMTPSender{Host: "smtp.example.test", Port: 465, ImplicitTLS: true,
			Username: "u", Password: "p", From: "Sumi <login@example.test>"}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*SMTPSender)
	}{
		{"nil sender", func(s *SMTPSender) {}},
		{"empty host", func(s *SMTPSender) { s.Host = " " }},
		{"host with port", func(s *SMTPSender) { s.Host = "smtp.example.test:465" }},
		{"port zero", func(s *SMTPSender) { s.Port = 0 }},
		{"port too large", func(s *SMTPSender) { s.Port = 70000 }},
		{"missing username", func(s *SMTPSender) { s.Username = "" }},
		{"missing password", func(s *SMTPSender) { s.Password = "" }},
		{"unparsable from", func(s *SMTPSender) { s.From = "not-an-address" }},
		{"from line break", func(s *SMTPSender) { s.From = "Sumi\nBcc: x@y <login@example.test>" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sender *SMTPSender
			if tc.name != "nil sender" {
				sender = valid()
				tc.mutate(sender)
			}
			err := sender.Send(context.Background(), Message{To: "a@example.test", Subject: "s", Text: "b"})
			if err == nil || !IsPermanent(err) {
				t.Fatalf("invalid configuration must fail permanently without dialing: %v", err)
			}
		})
	}
}

func TestSMTPSenderRejectsInjectedHeaders(t *testing.T) {
	server, pool := newSMTPFixtureServer(t, true, false)
	sender := fixtureSender(server, pool)
	for _, tc := range []struct {
		name    string
		message Message
	}{
		{"recipient with line break", Message{To: "a@example.test\r\nBcc: victim@example.test", Subject: "s", Text: "b"}},
		{"subject with line break", Message{To: "a@example.test", Subject: "s\r\nBcc: victim@example.test", Text: "b"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sender.Send(context.Background(), tc.message)
			if err == nil || !IsPermanent(err) {
				t.Fatalf("header injection must fail permanently: %v", err)
			}
		})
	}
	commands, _, _, _ := server.snapshot()
	if len(commands) != 0 {
		t.Fatalf("injected messages reached the wire: %v", commands)
	}
}
