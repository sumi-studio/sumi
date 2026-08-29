package messaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	workspacecontrol "github.com/sumi-studio/sumi/apps/api/internal/workspace"
)

type PlaceAccess struct {
	WorkspaceMemberID string
	PlaceMemberID     string
	VisibleFromSeq    int64
}

func (s *ScopedStore) Workspace(ctx context.Context) (Workspace, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Workspace{}, fmt.Errorf("begin scoped Workspace read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return Workspace{}, err
	}
	var workspace Workspace
	if err := tx.QueryRow(ctx, `
		SELECT workspace_id, name FROM workspaces WHERE workspace_id = $1`,
		s.Scope.WorkspaceID).Scan(&workspace.WorkspaceID, &workspace.Name); err != nil {
		return Workspace{}, fmt.Errorf("load scoped Workspace: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Workspace{}, fmt.Errorf("commit scoped Workspace read: %w", err)
	}
	return workspace, nil
}

func (s *ScopedStore) WorkspaceMembers(ctx context.Context) ([]MemberProfile, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin scoped Workspace-members read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return nil, err
	}
	members, err := s.activeMembersScoped(ctx, tx, Place{
		Kind: PlaceChannel, WorkspaceID: s.Scope.WorkspaceID,
	})
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit scoped Workspace-members read: %w", err)
	}
	return members, nil
}

// MaxChannelNameChars matches the schema CHECK on places.name. The rule lives
// in the Store rather than in one route, so REST and local-control obey the
// same bound (PostgreSQL length() counts characters, so this counts runes).
const MaxChannelNameChars = 200

func validChannelName(name string) bool {
	return name != "" && utf8.RuneCountInString(name) <= MaxChannelNameChars
}

const (
	placeCreationChannel   = "create_channel"
	placeCreationDuplicate = "duplicate_channel"
	placeCreationGroupDM   = "create_group_dm"
)

// A place creation can commit before an HTTP peer receives its response. Keep
// a receipt under the authenticated scoped actor, just as message sends keep
// their client nonce, so retrying that exact operation returns the first
// place rather than minting another one.
func (s *ScopedStore) replayPlaceCreation(
	ctx context.Context,
	tx pgx.Tx,
	operation, nonce string,
	digest []byte,
	workspaceMemberID string,
) (Place, bool, error) {
	if nonce == "" {
		return Place{}, false, nil
	}
	if len(nonce) > 128 {
		return Place{}, false, ErrIdempotencyConflict
	}
	receiptNonce := s.placeCreationReceiptNonce(nonce)
	key := fmt.Sprintf("place-creation/%s/%s/%s/%s", s.Scope.WorkspaceID, s.Scope.Actor.Key(), operation, receiptNonce)
	if workspaceMemberID != "" {
		key += "/" + workspaceMemberID
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return Place{}, false, fmt.Errorf("lock place creation nonce: %w", err)
	}
	var storedDigest []byte
	var placeID string
	receiptQuery := `
		SELECT request_digest, place_id
		FROM messaging_place_creation_receipts
		WHERE workspace_id = $1 AND member_kind = $2 AND member_id = $3
		  AND operation = $4 AND client_nonce = $5`
	args := []any{s.Scope.WorkspaceID, s.Scope.Actor.Kind, s.Scope.Actor.ID, operation, receiptNonce}
	if workspaceMemberID != "" {
		receiptQuery += ` AND workspace_member_id = $6`
		args = append(args, workspaceMemberID)
	}
	err := tx.QueryRow(ctx, receiptQuery, args...).Scan(&storedDigest, &placeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Place{}, false, nil
	}
	if err != nil {
		return Place{}, false, fmt.Errorf("load place creation receipt: %w", err)
	}
	if !bytes.Equal(storedDigest, digest) {
		return Place{}, false, ErrIdempotencyConflict
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return Place{}, false, fmt.Errorf("load idempotent place creation: %w", err)
	}
	return place, true, nil
}

func (s *ScopedStore) recordPlaceCreation(
	ctx context.Context,
	tx pgx.Tx,
	operation, nonce string,
	digest []byte,
	placeID string,
	workspaceMemberID string,
) error {
	if nonce == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO messaging_place_creation_receipts
			(workspace_id, workspace_member_id, member_kind, member_id,
			 operation, client_nonce, request_digest, place_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		s.Scope.WorkspaceID, workspaceMemberID, s.Scope.Actor.Kind, s.Scope.Actor.ID,
		operation, s.placeCreationReceiptNonce(nonce), digest, placeID,
	)
	if err != nil {
		return fmt.Errorf("record place creation receipt: %w", err)
	}
	return nil
}

// A client nonce is only a receipt identity inside one exact installation
// lifecycle. Hashing that sealed address into the stored key keeps a later
// disable/re-enable session, or another installation, from reconciling with a
// creation committed under stale authority. Workspace and actor remain
// explicit relational key columns for auditability and isolation.
func (s *ScopedStore) placeCreationReceiptNonce(nonce string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("sumi.messaging.place-creation-receipt/v1\x00"))
	for _, value := range []string{s.Scope.InstallationID, fmt.Sprint(s.Scope.AuthorityEpoch), nonce} {
		_, _ = hash.Write([]byte(fmt.Sprintf("%d:", len(value))))
		_, _ = hash.Write([]byte(value))
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func placeCreationDigest(operation string, request any) []byte {
	encoded, err := json.Marshal(struct {
		Operation string `json:"operation"`
		Request   any    `json:"request"`
	}{operation, request})
	if err != nil {
		panic("place creation request must be JSON serializable")
	}
	sum := sha256.Sum256(encoded)
	return sum[:]
}

func (s *ScopedStore) CreateChannel(ctx context.Context, name, topic string, voice bool) (Place, error) {
	place, _, err := s.createChannel(ctx, name, topic, voice, "")
	return place, err
}

func (s *ScopedStore) CreateChannelOnce(ctx context.Context, name, topic string, voice bool, nonce string) (Place, bool, error) {
	return s.createChannel(ctx, name, topic, voice, nonce)
}

func (s *ScopedStore) createChannel(ctx context.Context, name, topic string, voice bool, nonce string) (Place, bool, error) {
	if !validChannelName(name) {
		return Place{}, false, ErrInvalidChannelName
	}
	digest := placeCreationDigest(placeCreationChannel, struct {
		Name  string `json:"name"`
		Topic string `json:"topic"`
		Voice bool   `json:"voice"`
	}{name, topic, voice})
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, false, fmt.Errorf("begin create channel: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeManageChannelsInTx(ctx, tx)
	if err != nil {
		return Place{}, false, err
	}
	if place, replayed, err := s.replayPlaceCreation(ctx, tx, placeCreationChannel, nonce, digest, membership.WorkspaceMemberID); err != nil || replayed {
		if err != nil {
			return Place{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Place{}, false, fmt.Errorf("commit replay create channel: %w", err)
		}
		return place, false, nil
	}
	place, err := s.insertChannelInTx(ctx, tx, name, topic, voice)
	if err != nil {
		return Place{}, false, fmt.Errorf("insert channel: %w", err)
	}
	if err := s.recordPlaceCreation(ctx, tx, placeCreationChannel, nonce, digest, place.PlaceID, membership.WorkspaceMemberID); err != nil {
		return Place{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, false, fmt.Errorf("commit create channel: %w", err)
	}
	return place, true, nil
}

// insertChannelInTx writes the new channel and reads back the row the database
// actually stored, so the columns this code does not name — visibility, the
// sequence a place starts at — are reported as the schema decides them rather
// than as a second copy of that decision kept in Go.
func (s *ScopedStore) insertChannelInTx(ctx context.Context, tx pgx.Tx, name, topic string, voice bool) (Place, error) {
	var place Place
	var storedName *string
	if err := tx.QueryRow(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, name, topic, voice)
		VALUES ($1, 'channel', $2, $3, $4, $5)
		RETURNING place_id, kind, workspace_id, revision, name, topic, visibility, last_seq, voice`,
		newUUIDv7(), s.Scope.WorkspaceID, name, topic, voice,
	).Scan(&place.PlaceID, &place.Kind, &place.WorkspaceID, &place.Revision, &storedName,
		&place.Topic, &place.Visibility, &place.LastSeq, &place.Voice); err != nil {
		return Place{}, err
	}
	if storedName != nil {
		place.Name = *storedName
	}
	return place, nil
}

// UpdateChannelTopic keeps the topic-only shape for callers that only ever
// retopic. It delegates, so there is one implementation of what an edit is.
func (s *ScopedStore) UpdateChannelTopic(ctx context.Context, placeID, topic string) (Place, error) {
	return s.UpdateChannel(ctx, placeID, nil, &topic)
}

// UpdateChannel rewrites a channel's mutable identity. A nil field is left
// alone: renaming a channel can never be the reason its topic disappeared.
// Naming neither is not an edit — a silent no-op reads to its caller as a
// successful rename, so it is refused.
func (s *ScopedStore) UpdateChannel(ctx context.Context, placeID string, name, topic *string) (Place, error) {
	if name == nil && topic == nil {
		return Place{}, ErrEmptyChannelUpdate
	}
	if name != nil && !validChannelName(*name) {
		return Place{}, ErrInvalidChannelName
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, fmt.Errorf("begin update channel: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeManageChannelsInTx(ctx, tx); err != nil {
		return Place{}, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return Place{}, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return Place{}, err
	}
	if place.Kind != PlaceChannel {
		return Place{}, ErrNotAChannel
	}
	// COALESCE keeps the omitted column exactly as it was, in the database
	// rather than in the caller's memory of it — and RETURNING reports the row
	// the statement left behind, not the one this transaction read on its way
	// in. Two people editing different fields of the same channel would
	// otherwise each answer with the other's field as they last saw it, and
	// the place_updated built from that answer would put the stale value on
	// everyone's screen.
	var updated Place
	var updatedName *string
	if err := tx.QueryRow(ctx, `
		UPDATE places SET name = COALESCE($1, name), topic = COALESCE($2, topic)
		WHERE workspace_id = $3 AND place_id = $4
		RETURNING place_id, kind, workspace_id, revision, name, topic, visibility, last_seq, voice`,
		name, topic, s.Scope.WorkspaceID, placeID,
	).Scan(&updated.PlaceID, &updated.Kind, &updated.WorkspaceID, &updated.Revision, &updatedName,
		&updated.Topic, &updated.Visibility, &updated.LastSeq, &updated.Voice); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Place{}, ErrPlaceNotFound
		}
		return Place{}, fmt.Errorf("update channel: %w", err)
	}
	if updatedName != nil {
		updated.Name = *updatedName
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, fmt.Errorf("commit update channel: %w", err)
	}
	return updated, nil
}

// DuplicateChannel opens a new, empty channel shaped like an existing one. It
// carries the name and topic and nothing else: messages, read state and
// per-place notification settings belong to the original, and the copy is born
// empty. An empty name takes the derived default, so the human menu and the
// agent tool cannot disagree about what a copy is called.
func (s *ScopedStore) DuplicateChannel(ctx context.Context, placeID, name string) (Place, error) {
	place, _, err := s.duplicateChannel(ctx, placeID, name, "")
	return place, err
}

func (s *ScopedStore) DuplicateChannelOnce(ctx context.Context, placeID, name, nonce string) (Place, bool, error) {
	return s.duplicateChannel(ctx, placeID, name, nonce)
}

func (s *ScopedStore) duplicateChannel(ctx context.Context, placeID, name, nonce string) (Place, bool, error) {
	if name != "" && !validChannelName(name) {
		return Place{}, false, ErrInvalidChannelName
	}
	digest := placeCreationDigest(placeCreationDuplicate, struct {
		PlaceID string `json:"place_id"`
		Name    string `json:"name"`
	}{placeID, name})
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, false, fmt.Errorf("begin duplicate channel: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	membership, err := s.authorizeManageChannelsInTx(ctx, tx)
	if err != nil {
		return Place{}, false, err
	}
	if place, replayed, err := s.replayPlaceCreation(ctx, tx, placeCreationDuplicate, nonce, digest, membership.WorkspaceMemberID); err != nil || replayed {
		if err != nil {
			return Place{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Place{}, false, fmt.Errorf("commit replay duplicate channel: %w", err)
		}
		return place, false, nil
	}
	source, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return Place{}, false, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, source, s.Scope.Actor); err != nil {
		return Place{}, false, err
	}
	if source.Kind != PlaceChannel {
		return Place{}, false, ErrNotAChannel
	}
	if name == "" {
		name = copyChannelName(source.Name)
	}
	place, err := s.insertChannelInTx(ctx, tx, name, source.Topic, source.Voice)
	if err != nil {
		return Place{}, false, fmt.Errorf("insert duplicated channel: %w", err)
	}
	if err := s.recordPlaceCreation(ctx, tx, placeCreationDuplicate, nonce, digest, place.PlaceID, membership.WorkspaceMemberID); err != nil {
		return Place{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, false, fmt.Errorf("commit duplicate channel: %w", err)
	}
	return place, true, nil
}

// copyChannelName derives the default name of a copy. The server owns it so
// that the human menu and the agent tool never disagree about what「コピー」is
// called. When the result would pass the schema bound the base is shortened
// rather than the suffix dropped: what the name has to say is that this is a
// copy.
// copyChannelName derives the name of a copy. Copying a copy does not stack
// the suffix: 「general のコピー のコピー」names nothing the shorter name did
// not, and the reader has to count words to find the original.
func copyChannelName(source string) string {
	const suffix = " のコピー"
	base := strings.TrimSuffix(source, suffix)
	if base == "" {
		base = source
	}
	runes := []rune(base)
	room := MaxChannelNameChars - utf8.RuneCountInString(suffix)
	if len(runes) > room {
		runes = runes[:room]
	}
	return string(runes) + suffix
}

// normalizeDMOthers is the one entrance for a requested DM member set. The
// actor is never an "other", and each remaining participant occurs once in
// canonical ParticipantRef order. A group DM is a set: REST, agent-local,
// digesting, receipt replay, and membership insertion must not disagree merely
// because the same participants arrived in another order.
func normalizeDMOthers(actor ParticipantRef, requested []ParticipantRef) ([]ParticipantRef, error) {
	seen := map[string]bool{actor.Key(): true}
	others := make([]ParticipantRef, 0, len(requested))
	for _, ref := range requested {
		if err := ref.Validate(); err != nil {
			return nil, err
		}
		if seen[ref.Key()] {
			continue
		}
		seen[ref.Key()] = true
		others = append(others, ref)
	}
	sort.Slice(others, func(i, j int) bool { return others[i].Key() < others[j].Key() })
	return others, nil
}

func (s *ScopedStore) EnsureDM(ctx context.Context, other ParticipantRef) (Place, bool, error) {
	others, err := normalizeDMOthers(s.Scope.Actor, []ParticipantRef{other})
	if err != nil {
		return Place{}, false, err
	}
	if len(others) != 1 {
		return Place{}, false, errors.New("a dm needs two distinct participants")
	}
	other = others[0]
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, false, fmt.Errorf("begin ensure dm: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	actorMembership, err := s.authorizeMutationInTx(ctx, tx)
	if err != nil {
		return Place{}, false, err
	}
	otherMembership, err := s.workspaces.ActiveMembershipInTx(ctx, tx, s.Scope.WorkspaceID, other)
	if err != nil {
		return Place{}, false, ErrNotReachable
	}
	dmKey := dmPairKey(s.Scope.Actor, other)
	placeID := newUUIDv7()
	var inserted string
	err = tx.QueryRow(ctx, `
		INSERT INTO places (place_id, kind, workspace_id, dm_key)
		VALUES ($1, 'dm', $2, $3)
		ON CONFLICT (workspace_id, dm_key) DO NOTHING
		RETURNING place_id`, placeID, s.Scope.WorkspaceID, dmKey).Scan(&inserted)
	created := true
	if errors.Is(err, pgx.ErrNoRows) {
		created = false
		// Existing private-place tenure changes use the place row as their
		// audience fence. Append locks the same row while allocating seq before
		// it snapshots recipients, so re-admission either commits first and is
		// included or waits until the message transaction commits.
		if err := tx.QueryRow(ctx, `
			SELECT place_id FROM places
			WHERE workspace_id = $1 AND dm_key = $2 FOR UPDATE`,
			s.Scope.WorkspaceID, dmKey).Scan(&placeID); err != nil {
			return Place{}, false, fmt.Errorf("load existing dm: %w", err)
		}
	} else if err != nil {
		return Place{}, false, fmt.Errorf("insert dm place: %w", err)
	}
	for _, membership := range []workspacecontrol.Membership{actorMembership, otherMembership} {
		if err := admitPlaceTenure(ctx, tx, placeID, membership, 1); err != nil {
			return Place{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, false, fmt.Errorf("commit ensure dm: %w", err)
	}
	return Place{PlaceID: placeID, Kind: PlaceDM, WorkspaceID: s.Scope.WorkspaceID, Visibility: "private"}, created, nil
}

func (s *ScopedStore) CreateGroupDM(ctx context.Context, others []ParticipantRef) (Place, error) {
	place, _, err := s.createGroupDM(ctx, others, "")
	return place, err
}

func (s *ScopedStore) CreateGroupDMOnce(ctx context.Context, others []ParticipantRef, nonce string) (Place, bool, error) {
	return s.createGroupDM(ctx, others, nonce)
}

func (s *ScopedStore) createGroupDM(ctx context.Context, others []ParticipantRef, nonce string) (Place, bool, error) {
	others, err := normalizeDMOthers(s.Scope.Actor, others)
	if err != nil {
		return Place{}, false, err
	}
	members := append([]ParticipantRef{s.Scope.Actor}, others...)
	if len(members) < 3 {
		return Place{}, false, errors.New("a group dm needs at least three distinct participants")
	}
	digest := placeCreationDigest(placeCreationGroupDM, struct {
		Participants []ParticipantRef `json:"participants"`
	}{others})
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, false, fmt.Errorf("begin create group dm: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	actorMembership, err := s.authorizeMutationInTx(ctx, tx)
	if err != nil {
		return Place{}, false, err
	}
	if place, replayed, err := s.replayPlaceCreation(ctx, tx, placeCreationGroupDM, nonce, digest, actorMembership.WorkspaceMemberID); err != nil || replayed {
		if err != nil {
			return Place{}, false, err
		}
		if err := s.validateGroupDMReplayTenures(ctx, tx, place, others); err != nil {
			return Place{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Place{}, false, fmt.Errorf("commit replay create group dm: %w", err)
		}
		return place, false, nil
	}
	memberships := []workspacecontrol.Membership{actorMembership}
	for _, ref := range members[1:] {
		membership, err := s.workspaces.ActiveMembershipInTx(ctx, tx, s.Scope.WorkspaceID, ref)
		if err != nil {
			return Place{}, false, ErrNotReachable
		}
		memberships = append(memberships, membership)
	}
	place := Place{PlaceID: newUUIDv7(), Kind: PlaceGroupDM, WorkspaceID: s.Scope.WorkspaceID, Visibility: "private"}
	if _, err := tx.Exec(ctx, `
		INSERT INTO places (place_id, kind, workspace_id)
		VALUES ($1, 'group_dm', $2)`, place.PlaceID, place.WorkspaceID); err != nil {
		return Place{}, false, fmt.Errorf("insert group dm: %w", err)
	}
	for _, membership := range memberships {
		if err := admitPlaceTenure(ctx, tx, place.PlaceID, membership, 1); err != nil {
			return Place{}, false, err
		}
	}
	if err := s.recordPlaceCreation(ctx, tx, placeCreationGroupDM, nonce, digest, place.PlaceID, actorMembership.WorkspaceMemberID); err != nil {
		return Place{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, false, fmt.Errorf("commit create group dm: %w", err)
	}
	return place, true, nil
}

// A group-DM receipt remembers a historical creation, not current access.
// Reconciliation may return its place only while the actor and every exact
// requested participant still hold both their current Workspace tenure and a
// private-place tenure bound to that same membership. Removal/rejoin creates a
// new Workspace tenure and therefore cannot revive the old private audience.
func (s *ScopedStore) validateGroupDMReplayTenures(
	ctx context.Context,
	tx pgx.Tx,
	place Place,
	requested []ParticipantRef,
) error {
	if place.Kind != PlaceGroupDM {
		return ErrPlaceNotFound
	}
	participants := append([]ParticipantRef{s.Scope.Actor}, requested...)
	for _, participant := range participants {
		membership, err := s.workspaces.ActiveMembershipInTx(
			ctx, tx, s.Scope.WorkspaceID, participant,
		)
		if err != nil {
			return ErrPlaceNotFound
		}
		access, err := s.placeAccessAfterAuthorization(ctx, tx, place, participant)
		if err != nil || access.WorkspaceMemberID != membership.WorkspaceMemberID || access.PlaceMemberID == "" {
			return ErrPlaceNotFound
		}
	}
	return nil
}

func admitPlaceTenure(ctx context.Context, tx pgx.Tx, placeID string, membership workspacecontrol.Membership, visibleFrom int64) error {
	if visibleFrom < 1 {
		visibleFrom = 1
	}
	var currentWorkspaceMemberID string
	err := tx.QueryRow(ctx, `
		SELECT workspace_member_id FROM place_members
		WHERE place_id = $1 AND member_kind = $2 AND member_id = $3 AND left_at IS NULL`,
		placeID, membership.Participant.Kind, membership.Participant.ID).Scan(&currentWorkspaceMemberID)
	if err == nil {
		if currentWorkspaceMemberID != membership.WorkspaceMemberID {
			return errors.New("active place tenure is bound to a stale Workspace tenure")
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("load active place tenure: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO place_members
			(place_member_id, workspace_id, place_id, workspace_member_id,
			 member_kind, member_id, visible_from_seq)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		newUUIDv7(), membership.WorkspaceID, placeID, membership.WorkspaceMemberID,
		membership.Participant.Kind, membership.Participant.ID, visibleFrom); err != nil {
		return fmt.Errorf("admit place tenure: %w", err)
	}
	return nil
}

func (s *ScopedStore) PlaceFor(ctx context.Context, placeID string) (Place, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Place{}, fmt.Errorf("begin scoped place read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return Place{}, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return Place{}, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return Place{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Place{}, fmt.Errorf("commit scoped place read: %w", err)
	}
	return place, nil
}

func (s *ScopedStore) loadScopedPlace(ctx context.Context, q querier, placeID string) (Place, error) {
	return s.loadScopedPlaceWithClause(ctx, q, placeID, "")
}

// lockScopedPlace is the place-level half of the live authority fence. The
// Workspace fence is always acquired first. Place admission/closure takes the
// conflicting row lock, so audience and exact access cannot change during the
// protected effect.
func (s *ScopedStore) lockScopedPlace(ctx context.Context, q querier, placeID string) (Place, error) {
	return s.loadScopedPlaceWithClause(ctx, q, placeID, " FOR SHARE")
}

func (s *ScopedStore) loadScopedPlaceWithClause(
	ctx context.Context,
	q querier,
	placeID string,
	lockClause string,
) (Place, error) {
	if lockClause != "" && lockClause != " FOR SHARE" {
		return Place{}, errors.New("invalid place lock mode")
	}
	var place Place
	var name *string
	err := q.QueryRow(ctx, `
		SELECT place_id, kind, workspace_id, revision, name, topic, visibility, last_seq, voice
		FROM places WHERE workspace_id = $1 AND place_id = $2`+lockClause,
		s.Scope.WorkspaceID, placeID).Scan(&place.PlaceID, &place.Kind, &place.WorkspaceID,
		&place.Revision, &name, &place.Topic, &place.Visibility, &place.LastSeq, &place.Voice)
	if errors.Is(err, pgx.ErrNoRows) {
		return Place{}, ErrPlaceNotFound
	}
	if err != nil {
		return Place{}, fmt.Errorf("load scoped place: %w", err)
	}
	if name != nil {
		place.Name = *name
	}
	return place, nil
}

func (s *ScopedStore) placeAccessAfterAuthorization(ctx context.Context, q querier, place Place, actor ParticipantRef) (PlaceAccess, error) {
	var access PlaceAccess
	if place.Kind == PlaceChannel {
		err := q.QueryRow(ctx, `
			SELECT wm.workspace_member_id,
			       COALESCE(pm.place_member_id::text, ''), COALESCE(pm.visible_from_seq, 1)
			FROM workspace_members wm
			LEFT JOIN place_members pm
			  ON pm.workspace_id = wm.workspace_id
			 AND pm.workspace_member_id = wm.workspace_member_id
			 AND pm.place_id = $4 AND pm.left_at IS NULL
			WHERE wm.workspace_id = $1 AND wm.member_kind = $2
			  AND wm.member_id = $3 AND wm.left_at IS NULL`,
			s.Scope.WorkspaceID, actor.Kind, actor.ID, place.PlaceID).Scan(
			&access.WorkspaceMemberID, &access.PlaceMemberID, &access.VisibleFromSeq)
		if errors.Is(err, pgx.ErrNoRows) {
			return PlaceAccess{}, ErrPlaceNotFound
		}
		if err != nil {
			return PlaceAccess{}, fmt.Errorf("load channel access: %w", err)
		}
		return access, nil
	}
	err := q.QueryRow(ctx, `
		SELECT pm.workspace_member_id, pm.place_member_id, pm.visible_from_seq
		FROM place_members pm
		JOIN workspace_members wm
		  ON wm.workspace_id = pm.workspace_id
		 AND wm.workspace_member_id = pm.workspace_member_id
		 AND wm.member_kind = pm.member_kind AND wm.member_id = pm.member_id
		WHERE pm.workspace_id = $1 AND pm.place_id = $2
		  AND pm.member_kind = $3 AND pm.member_id = $4
		  AND pm.left_at IS NULL AND wm.left_at IS NULL`,
		s.Scope.WorkspaceID, place.PlaceID, actor.Kind, actor.ID).Scan(
		&access.WorkspaceMemberID, &access.PlaceMemberID, &access.VisibleFromSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return PlaceAccess{}, ErrPlaceNotFound
	}
	if err != nil {
		return PlaceAccess{}, fmt.Errorf("load private-place access: %w", err)
	}
	return access, nil
}

func (s *ScopedStore) ActiveMembers(ctx context.Context, placeID string) ([]MemberProfile, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin active-members read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := s.authorizeInTx(ctx, tx); err != nil {
		return nil, err
	}
	place, err := s.loadScopedPlace(ctx, tx, placeID)
	if err != nil {
		return nil, err
	}
	if _, err := s.placeAccessAfterAuthorization(ctx, tx, place, s.Scope.Actor); err != nil {
		return nil, err
	}
	members, err := s.activeMembersScoped(ctx, tx, place)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit active-members read: %w", err)
	}
	return members, nil
}

func (s *ScopedStore) activeMembersScoped(ctx context.Context, q querier, place Place) ([]MemberProfile, error) {
	condition := `pm.workspace_id = $1 AND pm.place_id = $2
		AND pm.left_at IS NULL AND wm.left_at IS NULL`
	args := []any{s.Scope.WorkspaceID, place.PlaceID}
	if place.Kind == PlaceChannel {
		// A channel admits every Workspace member; the place-scoped join above
		// still needs its place argument, so both branches pass the same args.
		condition = `wm.workspace_id = $1 AND wm.left_at IS NULL`
	}
	rows, err := q.Query(ctx, `
		SELECT wm.workspace_member_id,
		       COALESCE(pm.place_member_id::text, ''),
		       wm.member_kind, wm.member_id,
		       COALESCE(h.display_name, a.display_name, '') AS display_name
		FROM workspace_members wm
		-- Bound to the exact place: without pm.place_id the join multiplies a
		-- member by every other place they are in, which for a channel (whose
		-- condition does not constrain pm) returns the same participant once
		-- per place and made notification-intent issuance insert duplicates.
		LEFT JOIN place_members pm
		  ON pm.workspace_id = wm.workspace_id
		 AND pm.place_id = $2
		 AND pm.workspace_member_id = wm.workspace_member_id AND pm.left_at IS NULL
		LEFT JOIN humans h ON wm.member_kind = 'human' AND h.human_id = wm.member_id
		LEFT JOIN agents a ON wm.member_kind = 'personality_agent'
		                  AND a.personality_agent_id = wm.member_id
		WHERE `+condition+`
		ORDER BY wm.workspace_member_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("query scoped active members: %w", err)
	}
	defer rows.Close()
	var members []MemberProfile
	for rows.Next() {
		var member MemberProfile
		if err := rows.Scan(
			&member.workspaceMemberID,
			&member.placeMemberID,
			&member.Participant.Kind,
			&member.Participant.ID,
			&member.DisplayName,
		); err != nil {
			return nil, fmt.Errorf("scan scoped active member: %w", err)
		}
		if place.Kind == PlaceChannel {
			member.placeMemberID = ""
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scoped active members: %w", err)
	}
	return members, nil
}
