package messaging

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Place kinds.
const (
	PlaceChannel = "channel"
	PlaceDM      = "dm"
	PlaceGroupDM = "group_dm"
)

// Sentinel errors. The transport layer maps these to status codes; the store
// never reveals whether a place exists to a caller who cannot see it
// (ErrPlaceNotFound doubles as the authorization failure for reads).
var (
	ErrWorkspaceNotFound   = errors.New("workspace not found")
	ErrPlaceNotFound       = errors.New("place not found")
	ErrParticipantNotFound = errors.New("participant not found in the 戸籍")
	ErrNotAMember          = errors.New("participant is not an active member of the place")
	ErrNotReachable        = errors.New("participants share no active workspace membership")
	ErrMessageNotFound     = errors.New("message not found")
	ErrNotAuthor           = errors.New("only the author may do this")
	ErrNotAChannel         = errors.New("place is not a channel")
	ErrInvalidChannelName  = errors.New("channel name must be 1..200 characters")
	ErrEmptyChannelUpdate  = errors.New("a channel edit must name something to change")
	ErrForbidden           = errors.New("participant lacks the required role")
	ErrMessageDeleted      = errors.New("message is deleted")
	ErrIdempotencyConflict = errors.New("idempotency key was already used for another reaction mutation")
	ErrSeqBeyondLatest     = errors.New("seq is beyond the place's latest seq")
)

// Store persists the messaging surface. All authorization decisions the
// contract assigns to the service — membership, roles, reachability — are made
// here so REST, WS, and the agent tool path cannot diverge (凍結契約 v1 §4:
// 人間がUIから行うのと同じ経路・同じ権限モデル).
type Store struct {
	pool       *pgxpool.Pool
	workspaces WorkspaceAuthority
	apps       AppAuthority
	// blobs and attachmentPolicy are set by ConfigureAttachments. A nil blobs
	// keeps every attachment operation failing closed.
	blobs            AttachmentBlobs
	attachmentPolicy AttachmentPolicy
	missingBlobScan  attachmentMissingBlobScan
}

type attachmentMissingBlobScan struct {
	sync.Mutex
	createdAt    time.Time
	attachmentID string
}

// New returns a Store backed by the given pool. The pool must be connected to
// a database with migrations applied (0002 for the 戸籍, 0008 for messaging).
func New(pool *pgxpool.Pool, workspaces WorkspaceAuthority, apps AppAuthority) *Store {
	return &Store{pool: pool, workspaces: workspaces, apps: apps}
}

// Workspace is the Discord-shaped server: channels live directly under it.
type Workspace struct {
	WorkspaceID string
	Name        string
}

// Place is where messages flow. WorkspaceID and Name are empty for dm and
// group_dm places.
type Place struct {
	PlaceID     string
	Kind        string
	WorkspaceID string
	Name        string
	Topic       string
	Visibility  string
	LastSeq     int64
	// Voice is an attribute rather than a place kind because conversation text
	// and call presence belong to the same place (ADR 0012).
	Voice bool
}

// MemberProfile is a participant with their scope-resolved display name.
// IDs are never used as display names (ADR 0008 §1).
type MemberProfile struct {
	Participant             ParticipantRef
	DisplayName             string
	SecretaryForDisplayName string
	Role                    string // workspace role; empty for dm/group_dm members
}

// ProjectedDisplayName is the temporary v1 wire compromise for multiple
// Secretaries canonically named Sumi. The composite is presentation only: the
// agent registry continues to store "Sumi", while its stable Human relation
// supplies the qualifier.
func (m MemberProfile) ProjectedDisplayName() string {
	if m.Participant.Kind == KindPersonalityAgent && m.SecretaryForDisplayName != "" {
		return m.DisplayName + "（" + m.SecretaryForDisplayName + "）"
	}
	return m.DisplayName
}

// querier lets the same helpers run on the pool or inside a transaction.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// dmPairKey builds the canonical sorted participant key for a dm pair, the
// database-level guarantee that one pair has exactly one dm.
func dmPairKey(a, b ParticipantRef) string {
	keys := []string{a.Key(), b.Key()}
	sort.Strings(keys)
	return strings.Join(keys, "|")
}

// newUUIDv7 returns a canonical lowercase hyphenated UUIDv7 string.
func newUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		// uuid.NewV7 only fails when the crypto/rand source fails, which is a
		// fatal process condition. Panic so the caller surfaces it immediately.
		panic(fmt.Sprintf("generate uuidv7: %v", err))
	}
	return id.String()
}
