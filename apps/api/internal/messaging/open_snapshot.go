package messaging

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// OpenSnapshot is one authorized view of a place. Place metadata, history,
// member profiles, and the viewer's private cursor all come from the same
// PostgreSQL snapshot so no caller receives a cursor newer than the
// messages/latest_seq it was shown.
type OpenSnapshot struct {
	Place       Place
	Messages    []Message
	Members     []MemberProfile
	LastReadSeq int64
}

// OpenSnapshot loads one read-only screen at REPEATABLE READ. READ COMMITTED
// is not sufficient: each statement could otherwise observe a different
// concurrent append or cursor advance.
func (s *Store) OpenSnapshot(
	ctx context.Context,
	placeID string,
	viewer ParticipantRef,
	opt HistoryOptions,
) (OpenSnapshot, error) {
	tx, err := s.beginOpenSnapshot(ctx)
	if err != nil {
		return OpenSnapshot{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	place, err := s.placeFor(ctx, tx, placeID, viewer)
	if err != nil {
		return OpenSnapshot{}, err
	}
	snapshot, err := s.openSnapshotFromPlace(ctx, tx, place, viewer, opt)
	if err != nil {
		return OpenSnapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OpenSnapshot{}, fmt.Errorf("commit open snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *Store) beginOpenSnapshot(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("begin open snapshot: %w", err)
	}
	return tx, nil
}

// openSnapshotFromPlace finishes a screen after placeFor has authorized the
// viewer. Its querier must be the same snapshot that produced place.
func (s *Store) openSnapshotFromPlace(
	ctx context.Context,
	q querier,
	place Place,
	viewer ParticipantRef,
	opt HistoryOptions,
) (OpenSnapshot, error) {
	messages, err := s.history(ctx, q, place.PlaceID, opt)
	if err != nil {
		return OpenSnapshot{}, err
	}
	members, err := s.activeMembers(ctx, q, place)
	if err != nil {
		return OpenSnapshot{}, err
	}
	lastRead, err := s.readMarker(ctx, q, place.PlaceID, viewer)
	if err != nil {
		return OpenSnapshot{}, err
	}
	if lastRead > place.LastSeq {
		return OpenSnapshot{}, fmt.Errorf(
			"open snapshot last_read_seq %d exceeds latest_seq %d",
			lastRead,
			place.LastSeq,
		)
	}
	return OpenSnapshot{
		Place:       place,
		Messages:    messages,
		Members:     members,
		LastReadSeq: lastRead,
	}, nil
}
