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
func (s *ScopedStore) OpenSnapshot(
	ctx context.Context,
	placeID string,
	opt HistoryOptions,
) (OpenSnapshot, error) {
	tx, err := s.Store.beginOpenSnapshot(ctx)
	if err != nil {
		return OpenSnapshot{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Scope authority is part of the screen snapshot: the exact enabled
	// installation and active Workspace tenure cannot change between
	// authorization and any response projection.
	if _, err := s.authorizeSnapshotInTx(ctx, tx); err != nil {
		return OpenSnapshot{}, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return OpenSnapshot{}, err
	}
	access, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor)
	if err != nil {
		return OpenSnapshot{}, err
	}
	snapshot, err := s.openSnapshotFromPlace(ctx, tx, place, access, opt)
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

// openSnapshotFromPlace finishes a screen after the exact app scope and place
// tenure have been authorized. Its querier must be the same snapshot that
// produced place and access.
func (s *ScopedStore) openSnapshotFromPlace(
	ctx context.Context,
	q querier,
	place Place,
	access PlaceAccess,
	opt HistoryOptions,
) (OpenSnapshot, error) {
	messages, err := s.historyAfterAuthorization(ctx, q, place, access, opt)
	if err != nil {
		return OpenSnapshot{}, err
	}
	members, err := s.activeMembersScoped(ctx, q, place)
	if err != nil {
		return OpenSnapshot{}, err
	}
	lastRead, err := s.readMarkerAfterAuthorization(ctx, q, place, access.WorkspaceMemberID)
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
