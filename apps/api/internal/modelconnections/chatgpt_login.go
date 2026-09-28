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

// pollTimeout bounds one poll: the row lock wait, the issuer poll, the code
// exchange (each bounded by the OAuth client's HTTP timeout) and the save.
const pollTimeout = 45 * time.Second

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

// resumeMargin: a pending login closer than this to expiry is not resumed;
// there would be too little time left to enter its code.
const resumeMargin = time.Minute

// BeginChatGPTLogin starts a device-code login for the signed-in person
// (bound to their browser session). target, when set, names the
// subscription connection the login reconnects.
//
// When the same browser session already has a pending login for the same
// target with time left, that login is returned instead (same code): a
// reload, a discarded mobile tab or a reopened settings sheet continues
// the sign-in the person may already have authorized, rather than
// replacing it. Otherwise a new login starts and any earlier pending
// login of the same person is cancelled.
func (s *Store) BeginChatGPTLogin(ctx context.Context, human, session, target string) (LoginView, error) {
	if !s.ChatGPTEnabled() {
		return LoginView{}, ErrChatGPTDisabled
	}
	if session == "" {
		return LoginView{}, ErrInvalid
	}
	var targetID *string
	if target != "" {
		parsed, err := uuid.Parse(target)
		if err != nil {
			return LoginView{}, ErrNotFound
		}
		var found bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM model_api_connections WHERE human_id=$1 AND connection_id=$2 AND preset=$3)`, human, parsed.String(), ChatGPTPreset).Scan(&found); err != nil {
			return LoginView{}, err
		}
		if !found {
			return LoginView{}, ErrNotFound
		}
		t := parsed.String()
		targetID = &t
	}
	if v, ok, err := s.resumableLogin(ctx, human, session, targetID); err != nil || ok {
		return v, err
	}
	device, err := s.oauth.BeginDevice(ctx)
	if err != nil {
		return LoginView{}, err
	}
	now := s.oauth.now()
	id := uuid.NewString()
	sealed, err := s.sealDevice(human, id, device.DeviceAuthID)
	if err != nil {
		return LoginView{}, err
	}
	view := LoginView{
		LoginID: id, Status: "pending", VerificationURL: device.VerificationURL, UserCode: device.UserCode,
		ExpiresAt: now.Add(deviceLoginLifetime), IntervalMs: device.Interval.Milliseconds(),
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return LoginView{}, err
	}
	defer tx.Rollback(context.Background())
	// Lock order is login rows, then the person (see PollChatGPTLogin). A
	// login whose poll is completing right now is waited for, not
	// cancelled: its one-time code has been exchanged and its grant is
	// saved. A pending login created and locked by another request after
	// this point is skipped rather than waited on while holding the person.
	if _, err = tx.Exec(ctx, `SELECT 1 FROM model_chatgpt_logins WHERE human_id=$1 AND status='pending' FOR UPDATE`, human); err != nil {
		return LoginView{}, err
	}
	if err = lockHuman(ctx, tx, human); err != nil {
		return LoginView{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE model_chatgpt_logins SET status='cancelled' WHERE login_id IN
 (SELECT login_id FROM model_chatgpt_logins WHERE human_id=$1 AND status='pending' FOR UPDATE SKIP LOCKED)`, human); err != nil {
		return LoginView{}, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM model_chatgpt_logins WHERE human_id=$1 AND created_at < $2`, human, now.Add(-24*time.Hour)); err != nil {
		return LoginView{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO model_chatgpt_logins(login_id,human_id,session_id,connection_id,device_ciphertext,user_code,verification_url,interval_seconds,expires_at,next_poll_at,status,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11)`,
		id, human, session, targetID, sealed, device.UserCode, device.VerificationURL, int(device.Interval/time.Second), view.ExpiresAt, now.Add(device.Interval), now)
	if err != nil {
		return LoginView{}, err
	}
	return view, tx.Commit(ctx)
}

// resumableLogin finds the session's pending login for the same target
// that has at least resumeMargin left.
func (s *Store) resumableLogin(ctx context.Context, human, session string, target *string) (LoginView, bool, error) {
	var id string
	var r loginRow
	err := s.pool.QueryRow(ctx, `SELECT login_id::text,connection_id::text,user_code,verification_url,interval_seconds,expires_at,next_poll_at,status
 FROM model_chatgpt_logins
 WHERE human_id=$1 AND session_id=$2 AND status='pending' AND connection_id IS NOT DISTINCT FROM $3::uuid AND expires_at > $4
 ORDER BY created_at DESC LIMIT 1`, human, session, target, s.oauth.now().Add(resumeMargin)).Scan(
		&id, &r.target, &r.userCode, &r.url, &r.interval, &r.expires, &r.nextPoll, &r.status)
	if errors.Is(err, pgx.ErrNoRows) {
		return LoginView{}, false, nil
	}
	if err != nil {
		return LoginView{}, false, err
	}
	return r.view(id), true, nil
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

// PollChatGPTLogin reports a login's state and, when it is pending and its
// poll interval has elapsed, polls the issuer once. Polling happens on the
// browser's reads under the login row's lock, so any API process can serve
// it, concurrent reads never poll twice, and a restart loses nothing. On
// authorization the grant is sealed and stored in the same transaction
// that completes the login.
//
// Locks are taken in the order login row → person → connection row, the
// same order BeginChatGPTLogin and CancelChatGPTLogin use. The person is
// locked only after the issuer has answered, so a slow issuer never holds
// the person's row. The poll runs on its own bounded context: once the
// issuer may have consumed the one-time code, its result is stored even if
// the browser request that triggered the poll has gone away.
func (s *Store) PollChatGPTLogin(ctx context.Context, human, session, loginID string) (LoginView, error) {
	if !s.ChatGPTEnabled() {
		return LoginView{}, ErrChatGPTDisabled
	}
	parsed, err := uuid.Parse(loginID)
	if err != nil {
		return LoginView{}, ErrNotFound
	}
	loginID = parsed.String()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), pollTimeout)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return LoginView{}, err
	}
	defer tx.Rollback(context.Background())
	var r loginRow
	err = tx.QueryRow(ctx, `SELECT connection_id::text,device_ciphertext,user_code,verification_url,interval_seconds,expires_at,next_poll_at,status,error_code,result_connection_id::text
 FROM model_chatgpt_logins WHERE login_id=$1 AND human_id=$2 AND session_id=$3 FOR UPDATE`, loginID, human, session).Scan(
		&r.target, &r.sealed, &r.userCode, &r.url, &r.interval, &r.expires, &r.nextPoll, &r.status, &r.errorCode, &r.connection)
	if errors.Is(err, pgx.ErrNoRows) {
		return LoginView{}, ErrNotFound
	}
	if err != nil {
		return LoginView{}, err
	}
	finish := func(status, code string) error {
		r.status = status
		if code != "" {
			r.errorCode = &code
		}
		_, err := tx.Exec(ctx, `UPDATE model_chatgpt_logins SET status=$2,error_code=NULLIF($3,'') WHERE login_id=$1`, loginID, status, code)
		return err
	}
	now := s.oauth.now()
	var connected *Connection
	switch {
	case r.status != "pending":
	case !now.Before(r.expires):
		if err = finish("expired", ""); err != nil {
			return LoginView{}, err
		}
	case now.Before(r.nextPoll):
	default:
		device, err := s.openDevice(human, loginID, r.sealed)
		if err != nil {
			if err = finish("failed", loginErrFailed); err != nil {
				return LoginView{}, err
			}
			break
		}
		tokens, pending, pollErr := s.oauth.PollDevice(ctx, device, r.userCode)
		switch {
		case pending, errors.Is(pollErr, errIssuerTransient):
			r.nextPoll = now.Add(time.Duration(r.interval) * time.Second)
			if _, err = tx.Exec(ctx, `UPDATE model_chatgpt_logins SET next_poll_at=$2 WHERE login_id=$1`, loginID, r.nextPoll); err != nil {
				return LoginView{}, err
			}
		case pollErr != nil:
			if ctx.Err() != nil {
				return LoginView{}, ctx.Err()
			}
			if err = finish("failed", loginErrFailed); err != nil {
				return LoginView{}, err
			}
		default:
			target := ""
			if r.target != nil {
				target = *r.target
			}
			c, err := s.connectChatGPT(ctx, tx, human, target, tokens)
			if err != nil {
				// The one-time code is spent; a new login is the only way on.
				// Record the failure in a fresh transaction so the rollback
				// of the partial save cannot lose it.
				_ = tx.Rollback(context.Background())
				_, _ = s.pool.Exec(context.Background(), `UPDATE model_chatgpt_logins SET status='failed',error_code=$2 WHERE login_id=$1 AND status='pending'`, loginID, loginErrSave)
				return LoginView{LoginID: loginID, Status: "failed", ExpiresAt: r.expires, IntervalMs: int64(r.interval) * 1000, Error: loginErrSave}, nil
			}
			if _, err = tx.Exec(ctx, `UPDATE model_chatgpt_logins SET status='completed',result_connection_id=$2 WHERE login_id=$1`, loginID, c.ID); err != nil {
				return LoginView{}, err
			}
			r.status = "completed"
			connected = &c
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return LoginView{}, err
	}
	v := r.view(loginID)
	if connected != nil {
		v.Connection = connected
	} else if r.status == "completed" && r.connection != nil {
		if a, err := s.Describe(ctx, human, *r.connection); err == nil {
			v.Connection = &a.Connection
		}
	}
	return v, nil
}

// CancelChatGPTLogin cancels the person's pending login (idempotent). It
// locks only the login row, so it waits for a poll in progress; a login
// that completed meanwhile stays completed and is reported as such.
func (s *Store) CancelChatGPTLogin(ctx context.Context, human, session, loginID string) (LoginView, error) {
	parsed, err := uuid.Parse(loginID)
	if err != nil {
		return LoginView{}, ErrNotFound
	}
	var r loginRow
	err = s.pool.QueryRow(ctx, `UPDATE model_chatgpt_logins SET status=CASE WHEN status='pending' THEN 'cancelled' ELSE status END
 WHERE login_id=$1 AND human_id=$2 AND session_id=$3
 RETURNING connection_id::text,device_ciphertext,user_code,verification_url,interval_seconds,expires_at,next_poll_at,status,error_code,result_connection_id::text`,
		parsed.String(), human, session).Scan(&r.target, &r.sealed, &r.userCode, &r.url, &r.interval, &r.expires, &r.nextPoll, &r.status, &r.errorCode, &r.connection)
	if errors.Is(err, pgx.ErrNoRows) {
		return LoginView{}, ErrNotFound
	}
	if err != nil {
		return LoginView{}, err
	}
	return r.view(parsed.String()), nil
}
