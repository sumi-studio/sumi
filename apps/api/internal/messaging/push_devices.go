package messaging

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
)

// RefreshBrowserPushDevice is called only after a successful actual login. It
// renews delivery consent without extending the browser's HTTP authorization.
func (s *Store) RefreshBrowserPushDevice(ctx context.Context, existingID, newID, humanID string, expiresAt time.Time) (string, error) {
	if len(newID) != 43 || humanID == "" || !expiresAt.After(time.Now()) {
		return "", ErrInvalidPushSubscription
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin push device refresh: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	chosen := newID
	reused := false
	if existingID != "" {
		var owner string
		var expiry time.Time
		err := tx.QueryRow(ctx, `SELECT human_id, expires_at FROM push_devices WHERE device_id=$1 FOR UPDATE`, existingID).Scan(&owner, &expiry)
		var active bool
		if err == nil {
			if checkErr := tx.QueryRow(ctx, `SELECT $1::timestamptz > clock_timestamp()`, expiry).Scan(&active); checkErr != nil {
				return "", fmt.Errorf("check device refresh expiry: %w", checkErr)
			}
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("lock push device refresh: %w", err)
		}
		if err == nil && owner == humanID && active {
			chosen = existingID
			reused = true
			if _, err := tx.Exec(ctx, `UPDATE push_devices SET expires_at=$2 WHERE device_id=$1`, chosen, expiresAt); err != nil {
				return "", fmt.Errorf("renew push device: %w", err)
			}
		} else if err == nil {
			if _, err := tx.Exec(ctx, `DELETE FROM push_devices WHERE device_id=$1`, existingID); err != nil {
				return "", fmt.Errorf("retire push device: %w", err)
			}
		}
	}
	if !reused {
		if _, err := tx.Exec(ctx, `INSERT INTO push_devices(device_id,human_id,expires_at) VALUES($1,$2,$3)`, newID, humanID, expiresAt); err != nil {
			return "", fmt.Errorf("create push device: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit push device refresh: %w", err)
	}
	// Housekeeping runs after the refresh transaction releases its locks. It
	// must not turn a successful login into a failure or hold locks that another
	// concurrent refresh needs. A contended database can still leave expired
	// rows behind — the pass can exhaust its deadline or find every expired row
	// locked — so an unfinished purge retries detached instead of waiting for
	// the human's next login.
	if err := s.purgeExpiredPushDevices(ctx, humanID); err != nil {
		s.schedulePushDevicePurgeRetry(humanID)
	}
	return chosen, nil
}

// purgeExpiredPushDevices deletes expired device rows for one human inside a
// short bounded pass. It returns an error whenever expired rows remain —
// whether the pass failed or concurrent refreshes still hold their locks — so
// the caller knows the purge is unfinished rather than silently skipped.
func (s *Store) purgeExpiredPushDevices(ctx context.Context, humanID string) error {
	cleanupCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	for {
		res, err := s.pool.Exec(cleanupCtx, `
			WITH expired AS (
			  SELECT device_id FROM push_devices
			  WHERE human_id=$1 AND expires_at <= clock_timestamp()
			  ORDER BY device_id LIMIT 32 FOR UPDATE SKIP LOCKED
			)
			DELETE FROM push_devices WHERE device_id IN (SELECT device_id FROM expired)`, humanID)
		if err != nil {
			return err
		}
		var remaining int
		if err := s.pool.QueryRow(cleanupCtx, `
			SELECT count(*) FROM push_devices
			WHERE human_id=$1 AND expires_at <= clock_timestamp()`, humanID).Scan(&remaining); err != nil {
			return err
		}
		if remaining == 0 {
			return nil
		}
		if res.RowsAffected() == 0 || cleanupCtx.Err() != nil {
			return fmt.Errorf("expired push devices remain: %d", remaining)
		}
	}
}

// schedulePushDevicePurgeRetry records a human whose bounded purge pass did
// not finish and starts the shared drainer when none is running.
func (s *Store) schedulePushDevicePurgeRetry(humanID string) {
	s.pushDevicePurge.Lock()
	if s.pushDevicePurge.pending == nil {
		s.pushDevicePurge.pending = make(map[string]struct{})
	}
	s.pushDevicePurge.pending[humanID] = struct{}{}
	if s.pushDevicePurge.running {
		s.pushDevicePurge.Unlock()
		return
	}
	s.pushDevicePurge.running = true
	s.pushDevicePurge.Unlock()
	go s.runPushDevicePurgeDrain()
}

func (s *Store) runPushDevicePurgeDrain() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s.drainExpiredPushDevicePurges(ctx)
}

// drainExpiredPushDevicePurges is the single retry loop for purge passes that
// lost to contention: at most one runs per Store, it drains the pending set
// within one bounded context, then exits. Anything still pending at the
// deadline stays in the set for the next refresh to reschedule, so a busy or
// closed database never turns logins into unbounded background work.
func (s *Store) drainExpiredPushDevicePurges(ctx context.Context) {
	defer func() {
		s.pushDevicePurge.Lock()
		defer s.pushDevicePurge.Unlock()
		s.pushDevicePurge.running = false
		// A pending entry can arrive between the empty check and this exit
		// path; respawn once while the drain context is still alive.
		if ctx.Err() == nil && len(s.pushDevicePurge.pending) > 0 {
			s.pushDevicePurge.running = true
			go s.runPushDevicePurgeDrain()
		}
	}()
	for {
		s.pushDevicePurge.Lock()
		var humanID string
		for h := range s.pushDevicePurge.pending {
			humanID = h
			break
		}
		if humanID == "" {
			s.pushDevicePurge.Unlock()
			return
		}
		delete(s.pushDevicePurge.pending, humanID)
		s.pushDevicePurge.Unlock()
		if err := s.purgeExpiredPushDevices(ctx, humanID); err != nil {
			// Every unfinished pass — including one cut short by the drain
			// deadline — returns the entry so it survives for the next
			// refresh to reschedule.
			s.pushDevicePurge.Lock()
			s.pushDevicePurge.pending[humanID] = struct{}{}
			s.pushDevicePurge.Unlock()
			if ctx.Err() != nil {
				log.Printf("push: expired device cleanup unfinished for human %s: %v", humanID, err)
				return
			}
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}
}

// Revocation waits for in-flight sends holding a shared device lease. Once it
// succeeds, neither queued delivery nor a stale registration can send again.
func (s *Store) RevokeBrowserPushDevice(ctx context.Context, deviceID string) error {
	if deviceID == "" {
		return nil
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM push_devices WHERE device_id=$1`, deviceID); err != nil {
		return fmt.Errorf("revoke push device: %w", err)
	}
	return nil
}

func lockPushDevice(ctx context.Context, tx pgx.Tx, deviceID, humanID string) (bool, error) {
	var expiresAt time.Time
	err := tx.QueryRow(ctx, `SELECT expires_at FROM push_devices WHERE device_id=$1 AND human_id=$2 FOR SHARE`, deviceID, humanID).Scan(&expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("lock push device: %w", err)
	}
	// Ask the database after the lock wait: transaction-start time or queued
	// subscription state could admit an already expired device.
	var active bool
	if err := tx.QueryRow(ctx, `SELECT $1::timestamptz > clock_timestamp()`, expiresAt).Scan(&active); err != nil {
		return false, fmt.Errorf("check push device expiry: %w", err)
	}
	return active, nil
}
