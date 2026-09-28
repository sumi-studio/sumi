package modelconnections

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// LoginView is the browser-visible state of one device-code login. The
// user code is shown only while the login is pending.
type LoginView struct {
	LoginID         string      `json:"loginId"`
	Status          string      `json:"status"`
	VerificationURL string      `json:"verificationUrl,omitempty"`
	UserCode        string      `json:"userCode,omitempty"`
	ExpiresAt       time.Time   `json:"expiresAt"`
	IntervalMs      int64       `json:"intervalMs"`
	Error           string      `json:"error,omitempty"`
	Connection      *Connection `json:"connection,omitempty"`
}

// Bounded login failure codes (the HTTP layer renders them).
const (
	loginErrFailed      = "login_failed"
	loginErrUnavailable = "device_login_unavailable"
	loginErrSave        = "save_failed"
)

func loginAAD(human, login string) []byte {
	b, _ := json.Marshal([]string{"sumi.chatgpt-login.v1", human, login})
	return b
}

func (s *Store) sealDevice(human, login, deviceAuthID string) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, ErrUnavailable
	}
	return s.aead.Seal(nonce, nonce, []byte(deviceAuthID), loginAAD(human, login)), nil
}

func (s *Store) openDevice(human, login string, b []byte) (string, error) {
	n := s.aead.NonceSize()
	if len(b) < n {
		return "", ErrUnavailable
	}
	v, err := s.aead.Open(nil, b[:n], b[n:], loginAAD(human, login))
	if err != nil || len(v) == 0 {
		return "", ErrUnavailable
	}
	return string(v), nil
}

// BeginChatGPTLogin starts or recovers one browser-chosen login ID. Retries
// with the same ID return that attempt, including its completed connection.
// A different ID is an intentional new login and replaces any pending one.
// The browser persists only this opaque ID, never a code or credential.
func (s *Store) BeginChatGPTLogin(ctx context.Context, human, session, target, loginID string) (LoginView, error) {
	if !s.ChatGPTEnabled() {
		return LoginView{}, ErrChatGPTDisabled
	}
	id, err := uuid.Parse(loginID)
	if err != nil || session == "" {
		return LoginView{}, ErrInvalid
	}
	loginID = id.String()
	var targetID *string
	if target != "" {
		parsed, err := uuid.Parse(target)
		if err != nil {
			return LoginView{}, ErrNotFound
		}
		target = parsed.String()
		targetID = &target
	}
	ctx, cancelLock := chatGPTPhase(ctx, chatGPTLockTimeout)
	defer cancelLock()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return LoginView{}, err
	}
	defer rollbackChatGPT(tx)
	// All login operations use human -> login -> connection, including
	// rechecking the attempt ID and replacement after acquiring the human.
	if err = lockHuman(ctx, tx, human); err != nil {
		return LoginView{}, err
	}
	r, err := readLogin(ctx, tx, human, session, loginID)
	if err == nil {
		if (r.target == nil) != (targetID == nil) || (r.target != nil && *r.target != target) {
			return LoginView{}, ErrInvalid
		}
		cancelLock()
		ctx, cancelSave := chatGPTPhase(ctx, chatGPTSaveTimeout)
		defer cancelSave()
		if r.status == "pending" && !s.oauth.now().Before(r.expires) {
			r.status = "expired"
			if _, err = tx.Exec(ctx, `UPDATE model_chatgpt_logins SET status='expired' WHERE login_id=$1`, loginID); err != nil {
				return LoginView{}, err
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return LoginView{}, err
		}
		return s.loginView(ctx, human, loginID, r), nil
	}
	if !errors.Is(err, ErrNotFound) {
		return LoginView{}, err
	}
	// A login ID is immutable and cannot be rebound to another session or
	// person. Do not issue a device code for a colliding ID.
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM model_chatgpt_logins WHERE login_id=$1)`, loginID).Scan(&exists); err != nil {
		return LoginView{}, err
	}
	if exists {
		return LoginView{}, ErrNotFound
	}
	if targetID != nil {
		if err = lockChatGPTTarget(ctx, tx, human, target); err != nil {
			return LoginView{}, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE model_chatgpt_logins SET status='cancelled' WHERE human_id=$1 AND status='pending'`, human); err != nil {
		return LoginView{}, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM model_chatgpt_logins WHERE human_id=$1 AND created_at < $2`, human, s.oauth.now().Add(-24*time.Hour)); err != nil {
		return LoginView{}, err
	}
	cancelLock()
	issuerCtx, cancelIssuer := chatGPTPhase(ctx, chatGPTRefreshTimeout)
	device, err := s.oauth.BeginDevice(issuerCtx)
	cancelIssuer()
	if err != nil {
		return LoginView{}, err
	}
	ctx, cancelSave := chatGPTPhase(ctx, chatGPTSaveTimeout)
	defer cancelSave()
	now := s.oauth.now()
	sealed, err := s.sealDevice(human, loginID, device.DeviceAuthID)
	if err != nil {
		return LoginView{}, err
	}
	v := LoginView{LoginID: loginID, Status: "pending", VerificationURL: device.VerificationURL, UserCode: device.UserCode,
		ExpiresAt: now.Add(deviceLoginLifetime), IntervalMs: device.Interval.Milliseconds()}
	_, err = tx.Exec(ctx, `INSERT INTO model_chatgpt_logins(login_id,human_id,session_id,connection_id,device_ciphertext,user_code,verification_url,interval_seconds,expires_at,next_poll_at,status,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11)`, loginID, human, session, targetID, sealed, device.UserCode, device.VerificationURL,
		int(device.Interval/time.Second), v.ExpiresAt, now.Add(device.Interval), now)
	if err != nil {
		return LoginView{}, err
	}
	return v, tx.Commit(ctx)
}

type loginRow struct {
	target     *string
	sealed     []byte
	userCode   string
	url        string
	interval   int
	expires    time.Time
	nextPoll   time.Time
	status     string
	errorCode  *string
	connection *string
}

func readLogin(ctx context.Context, tx pgx.Tx, human, session, id string) (loginRow, error) {
	var r loginRow
	err := tx.QueryRow(ctx, `SELECT connection_id::text,device_ciphertext,user_code,verification_url,interval_seconds,expires_at,next_poll_at,status,error_code,result_connection_id::text
 FROM model_chatgpt_logins WHERE login_id=$1 AND human_id=$2 AND session_id=$3 FOR UPDATE`, id, human, session).Scan(
		&r.target, &r.sealed, &r.userCode, &r.url, &r.interval, &r.expires, &r.nextPoll, &r.status, &r.errorCode, &r.connection)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return r, err
}

func lockChatGPTTarget(ctx context.Context, tx pgx.Tx, human, target string) error {
	var id string
	err := tx.QueryRow(ctx, `SELECT connection_id::text FROM model_api_connections WHERE human_id=$1 AND connection_id=$2 AND preset=$3 FOR UPDATE`, human, target, ChatGPTPreset).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func (r loginRow) view(id string) LoginView {
	v := LoginView{LoginID: id, Status: r.status, ExpiresAt: r.expires, IntervalMs: int64(r.interval) * 1000}
	if r.status == "pending" {
		v.VerificationURL, v.UserCode = r.url, r.userCode
	}
	if r.errorCode != nil {
		v.Error = *r.errorCode
	}
	return v
}

func (s *Store) loginView(ctx context.Context, human, id string, r loginRow) LoginView {
	v := r.view(id)
	if r.status == "completed" && r.connection != nil {
		if a, err := s.Describe(ctx, human, *r.connection); err == nil {
			v.Connection = &a.Connection
		}
	}
	return v
}

// PollChatGPTLogin exchanges a code at most once under human -> login ->
// connection locks. Acquire every contended lock BEFORE contacting the
// issuer. Waiting for another login or refresh cannot consume the issuer
// or persistence budgets, and a dropped browser request cannot discard a
// successfully returned grant. A new process reads the same durable row.
func (s *Store) PollChatGPTLogin(ctx context.Context, human, session, loginID string) (LoginView, error) {
	if !s.ChatGPTEnabled() {
		return LoginView{}, ErrChatGPTDisabled
	}
	parsed, err := uuid.Parse(loginID)
	if err != nil {
		return LoginView{}, ErrNotFound
	}
	loginID = parsed.String()
	ctx, cancelLock := chatGPTPhase(ctx, chatGPTLockTimeout)
	defer cancelLock()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return LoginView{}, err
	}
	defer rollbackChatGPT(tx)
	if err = lockHuman(ctx, tx, human); err != nil {
		return LoginView{}, err
	}
	r, err := readLogin(ctx, tx, human, session, loginID)
	if err != nil {
		return LoginView{}, err
	}
	if r.status == "pending" && r.target != nil {
		if err = lockChatGPTTarget(ctx, tx, human, *r.target); err != nil {
			return LoginView{}, err
		}
	}
	cancelLock()
	now := s.oauth.now()
	var tokens OAuthTokens
	var pending bool
	var pollErr error
	poll := r.status == "pending" && now.Before(r.expires) && !now.Before(r.nextPoll)
	if poll {
		device, err := s.openDevice(human, loginID, r.sealed)
		if err != nil {
			pollErr = err
		} else {
			issuerCtx, cancelIssuer := chatGPTPhase(ctx, chatGPTPollTimeout)
			tokens, pending, pollErr = s.oauth.PollDevice(issuerCtx, device, r.userCode)
			cancelIssuer()
		}
	}
	// The write phase has a fresh budget even if the issuer used all of its
	// own. All rows it needs are already locked, including the reconnect target.
	ctx, cancelSave := chatGPTPhase(ctx, chatGPTSaveTimeout)
	defer cancelSave()
	var connected *Connection
	switch {
	case r.status != "pending":
	case !now.Before(r.expires):
		r.status = "expired"
	case !poll:
	case pending, errors.Is(pollErr, errIssuerTransient):
		r.nextPoll = s.oauth.now().Add(time.Duration(r.interval) * time.Second)
	case pollErr != nil:
		r.status = "failed"
		code := loginErrFailed
		r.errorCode = &code
	default:
		target := ""
		if r.target != nil {
			target = *r.target
		}
		c, err := s.connectChatGPT(ctx, tx, human, target, tokens)
		if err != nil {
			rollbackChatGPT(tx)
			failureCtx, cancel := chatGPTPhase(ctx, chatGPTSaveTimeout)
			defer cancel()
			_, _ = s.pool.Exec(failureCtx, `UPDATE model_chatgpt_logins SET status='failed',error_code=$2 WHERE login_id=$1 AND status='pending'`, loginID, loginErrSave)
			return LoginView{LoginID: loginID, Status: "failed", ExpiresAt: r.expires, IntervalMs: int64(r.interval) * 1000, Error: loginErrSave}, nil
		}
		r.status, r.connection, connected = "completed", &c.ID, &c
	}
	if _, err = tx.Exec(ctx, `UPDATE model_chatgpt_logins SET status=$2,error_code=$3,result_connection_id=$4,next_poll_at=$5 WHERE login_id=$1`, loginID, r.status, r.errorCode, r.connection, r.nextPoll); err != nil {
		return LoginView{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return LoginView{}, err
	}
	if connected != nil {
		v := r.view(loginID)
		v.Connection = connected
		return v, nil
	}
	return s.loginView(ctx, human, loginID, r), nil
}

// CancelChatGPTLogin waits out a consuming poll on a detached, bounded
// context. An already-completed grant stays completed. Cancellation does
// not depend on the browser remaining connected while it waits.
func (s *Store) CancelChatGPTLogin(ctx context.Context, human, session, loginID string) (LoginView, error) {
	parsed, err := uuid.Parse(loginID)
	if err != nil {
		return LoginView{}, ErrNotFound
	}
	loginID = parsed.String()
	ctx, cancelLock := chatGPTPhase(ctx, chatGPTCancelWait)
	defer cancelLock()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return LoginView{}, err
	}
	defer rollbackChatGPT(tx)
	if err = lockHuman(ctx, tx, human); err != nil {
		return LoginView{}, err
	}
	r, err := readLogin(ctx, tx, human, session, loginID)
	if err != nil {
		return LoginView{}, err
	}
	cancelLock()
	ctx, cancelSave := chatGPTPhase(ctx, chatGPTSaveTimeout)
	defer cancelSave()
	if r.status == "pending" {
		r.status = "cancelled"
		if _, err = tx.Exec(ctx, `UPDATE model_chatgpt_logins SET status='cancelled' WHERE login_id=$1`, loginID); err != nil {
			return LoginView{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return LoginView{}, err
	}
	return s.loginView(ctx, human, loginID, r), nil
}
