package messaging

import (
	"context"
	"fmt"
)

// ReadMarker returns the participant's private cursor, or zero before the
// place has ever been read.
func (s *Store) ReadMarker(ctx context.Context, placeID string, participant ParticipantRef) (int64, error) {
	if _, err := s.PlaceFor(ctx, placeID, participant); err != nil {
		return 0, err
	}
	var seq int64
	err := s.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT last_read_seq FROM read_markers
		 WHERE place_id = $1 AND member_kind = $2 AND member_id = $3), 0)`,
		placeID, participant.Kind, participant.ID).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("read marker: %w", err)
	}
	return seq, nil
}
