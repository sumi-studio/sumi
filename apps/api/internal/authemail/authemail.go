// Package authemail renders and sends Sumi sign-in challenge email. Delivery
// providers implement Sender; the deployment selects one. Messages carry
// one-time secrets and must never be logged.
package authemail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Message struct {
	To      string
	Subject string
	Text    string
}

// Sender delivers one message. Return Permanent for a failure a retry cannot
// fix (for example a rejected recipient); other errors are retried.
type Sender interface {
	Send(ctx context.Context, message Message) error
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

func IsPermanent(err error) bool {
	var permanent permanentError
	return errors.As(err, &permanent)
}

type Challenge struct {
	To        string
	Code      string
	LinkURL   string
	ExpiresAt time.Time
	SentAt    time.Time
}

var japanTime = time.FixedZone("JST", 9*60*60)

// RenderChallenge puts the code first. The send time lets a person pick the
// newest message when several arrive out of order.
func RenderChallenge(challenge Challenge) Message {
	format := func(t time.Time) string { return t.In(japanTime).Format("2006-01-02 15:04") }
	var b strings.Builder
	b.WriteString("Sumiにログインするための確認コードです。\n\n")
	fmt.Fprintf(&b, "確認コード: %s\n\n", challenge.Code)
	b.WriteString("ログイン画面に戻って、このコードを入力してください。\n")
	b.WriteString("このメールを開いた端末でそのまま続ける場合は、次のリンクを開いてください。\n")
	fmt.Fprintf(&b, "%s\n\n", challenge.LinkURL)
	fmt.Fprintf(&b, "有効期限: %s（日本時間）\n", format(challenge.ExpiresAt))
	fmt.Fprintf(&b, "送信日時: %s（日本時間）\n\n", format(challenge.SentAt))
	b.WriteString("複数のメールが届いた場合は、送信日時がいちばん新しいメールを使ってください。\n")
	b.WriteString("心当たりがない場合は、このメールを無視してください。コードとリンクは他の人に伝えないでください。\n")
	return Message{
		To:      challenge.To,
		Subject: fmt.Sprintf("Sumiの確認コード: %s", challenge.Code),
		Text:    b.String(),
	}
}

// DevMailbox writes each message as an owner-only JSON file in an explicit
// local directory. It is a development and test driver, never a provider.
type DevMailbox struct {
	Dir string
}

func (m *DevMailbox) Send(ctx context.Context, message Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil || !filepath.IsAbs(m.Dir) {
		return Permanent(errors.New("development mailbox directory must be absolute"))
	}
	if err := os.MkdirAll(m.Dir, 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(struct {
		To        string    `json:"to"`
		Subject   string    `json:"subject"`
		Text      string    `json:"text"`
		WrittenAt time.Time `json:"written_at"`
	}{message.To, message.Subject, message.Text, time.Now().UTC()})
	if err != nil {
		return Permanent(err)
	}
	file, err := os.CreateTemp(m.Dir, time.Now().UTC().Format("20060102T150405.000000000")+"-*.json")
	if err != nil {
		return err
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
