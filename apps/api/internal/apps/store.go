package apps

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const workspaceManageAppsPermission = "manage_apps"

// WorkspaceAuthorizer keeps app lifecycle dependent on Workspace's canonical
// commit-time authorization without moving app vocabulary into Workspace.
type WorkspaceAuthorizer interface {
	LockAndRequirePermission(context.Context, pgx.Tx, string, participant.Ref, string) error
	RequireMembershipInTx(context.Context, pgx.Tx, string, participant.Ref) error
}

type Store struct {
	pool       *pgxpool.Pool
	workspaces WorkspaceAuthorizer
	now        func() time.Time
}

func New(pool *pgxpool.Pool, workspaces WorkspaceAuthorizer) *Store {
	return &Store{pool: pool, workspaces: workspaces, now: time.Now}
}

func (s *Store) Catalog(ctx context.Context) ([]Descriptor, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ac.app_id, ac.display_name, ac.workspace_owner_allowed,
		       ac.participant_owner_allowed, cap.capability_ref, cap.label
		FROM app_catalog ac
		LEFT JOIN app_workspace_role_capabilities cap
		  ON cap.app_id = ac.app_id AND cap.retired_at IS NULL
		ORDER BY ac.display_name, ac.app_id, cap.capability_ref`)
	if err != nil {
		return nil, fmt.Errorf("list app catalog: %w", err)
	}
	defer rows.Close()
	descriptors := []Descriptor{}
	var current *Descriptor
	for rows.Next() {
		var descriptor Descriptor
		var capabilityRef, capabilityLabel *string
		if err := rows.Scan(&descriptor.AppID, &descriptor.DisplayName,
			&descriptor.WorkspaceOwnerAllowed, &descriptor.ParticipantOwnerAllowed,
			&capabilityRef, &capabilityLabel); err != nil {
			return nil, fmt.Errorf("scan app descriptor: %w", err)
		}
		if current == nil || current.AppID != descriptor.AppID {
			descriptor.WorkspaceRoleCapabilities = []WorkspaceRoleCapability{}
			descriptors = append(descriptors, descriptor)
			current = &descriptors[len(descriptors)-1]
		}
		if capabilityRef != nil && capabilityLabel != nil {
			current.WorkspaceRoleCapabilities = append(current.WorkspaceRoleCapabilities,
				WorkspaceRoleCapability{Ref: *capabilityRef, Label: *capabilityLabel})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate app catalog: %w", err)
	}
	return descriptors, nil
}

// RequireEnabledInstallationInTx is the app-consumer admission seam for a
// commit. The caller must present the exact installation identity it entered
// through; matching only owner+app would let a stale pre-uninstall capability
// authorize against a later reinstall. The SHARE lock binds lifecycle
// admission and the app-owned write to one transaction ordering.
func (s *Store) RequireEnabledInstallationInTx(
	ctx context.Context,
	tx pgx.Tx,
	installationID string,
	owner OwnerRef,
	appID string,
) (Installation, error) {
	if !isCanonicalUUIDv7(installationID) {
		return Installation{}, ErrInstallationNotFound
	}
	if err := owner.Validate(); err != nil {
		return Installation{}, err
	}
	storageKind, storageID := ownerStorageKey(owner)
	row := tx.QueryRow(ctx, `
		SELECT installation_id, owner_kind, owner_id, app_id, enabled,
		       installed_at, updated_at
		FROM app_installations
		WHERE installation_id = $1
		  AND owner_kind = $2 AND owner_id = $3 AND app_id = $4
		FOR SHARE`, installationID, storageKind, storageID, appID)
	installation, err := scanInstallation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Installation{}, ErrInstallationNotFound
	}
	if err != nil {
		return Installation{}, fmt.Errorf("load exact app lifecycle admission: %w", err)
	}
	if installation.State != StateEnabled {
		return Installation{}, ErrAppDisabled
	}
	return installation, nil
}

// RequireEnabledInstallationInSnapshot is the read-only transaction variant
// used when an app response must bind exact lifecycle admission and every
// projected row to one database snapshot. It intentionally does not lock:
// REPEATABLE READ supplies a coherent historical view, while mutating app
// operations use RequireEnabledInstallationInTx for commit-time ordering.
func (s *Store) RequireEnabledInstallationInSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	installationID string,
	owner OwnerRef,
	appID string,
) (Installation, error) {
	if !isCanonicalUUIDv7(installationID) {
		return Installation{}, ErrInstallationNotFound
	}
	if err := owner.Validate(); err != nil {
		return Installation{}, err
	}
	storageKind, storageID := ownerStorageKey(owner)
	row := tx.QueryRow(ctx, `
		SELECT installation_id, owner_kind, owner_id, app_id, enabled,
		       installed_at, updated_at
		FROM app_installations
		WHERE installation_id = $1
		  AND owner_kind = $2 AND owner_id = $3 AND app_id = $4`,
		installationID, storageKind, storageID, appID)
	installation, err := scanInstallation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Installation{}, ErrInstallationNotFound
	}
	if err != nil {
		return Installation{}, fmt.Errorf("load exact app snapshot admission: %w", err)
	}
	if installation.State != StateEnabled {
		return Installation{}, ErrAppDisabled
	}
	return installation, nil
}

// RequireEnabledInstallation is the read-side equivalent used by entry
// surfaces and delivery workers. It deliberately preserves exact installation
// identity instead of collapsing missing and disabled bindings into a boolean.
func (s *Store) RequireEnabledInstallation(
	ctx context.Context,
	installationID string,
	owner OwnerRef,
	appID string,
) (Installation, error) {
	if !isCanonicalUUIDv7(installationID) {
		return Installation{}, ErrInstallationNotFound
	}
	if err := owner.Validate(); err != nil {
		return Installation{}, err
	}
	storageKind, storageID := ownerStorageKey(owner)
	row := s.pool.QueryRow(ctx, `
		SELECT installation_id, owner_kind, owner_id, app_id, enabled,
		       installed_at, updated_at
		FROM app_installations
		WHERE installation_id = $1
		  AND owner_kind = $2 AND owner_id = $3 AND app_id = $4`,
		installationID, storageKind, storageID, appID)
	installation, err := scanInstallation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Installation{}, ErrInstallationNotFound
	}
	if err != nil {
		return Installation{}, fmt.Errorf("read exact app lifecycle state: %w", err)
	}
	if installation.State != StateEnabled {
		return Installation{}, ErrAppDisabled
	}
	return installation, nil
}

// ResolveEnabledInstallation turns an authenticated, model-selected app owner
// address into the exact current installation identity used at bind time. It
// never supplies a default Workspace and never accepts an installation id from
// the model. Callers must still seal the returned id into the invocation and
// use RequireEnabledInstallationInTx again at commit.
func (s *Store) ResolveEnabledInstallation(
	ctx context.Context,
	owner OwnerRef,
	actor participant.Ref,
	appID string,
) (Installation, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return Installation{}, fmt.Errorf("begin app-installation resolution: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := s.authorizeRead(ctx, tx, owner, actor); err != nil {
		return Installation{}, err
	}
	storageKind, storageID := ownerStorageKey(owner)
	row := tx.QueryRow(ctx, `
		SELECT installation_id, owner_kind, owner_id, app_id, enabled,
		       installed_at, updated_at
		FROM app_installations
		WHERE owner_kind = $1 AND owner_id = $2 AND app_id = $3`,
		storageKind, storageID, appID)
	installation, err := scanInstallation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Installation{}, ErrInstallationNotFound
	}
	if err != nil {
		return Installation{}, fmt.Errorf("resolve app installation: %w", err)
	}
	if installation.State != StateEnabled {
		return Installation{}, ErrAppDisabled
	}
	if err := tx.Commit(ctx); err != nil {
		return Installation{}, fmt.Errorf("commit app-installation resolution: %w", err)
	}
	return installation, nil
}

func (s *Store) Installations(ctx context.Context, owner OwnerRef, actor participant.Ref) ([]Installation, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("begin app-installation read: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := s.authorizeRead(ctx, tx, owner, actor); err != nil {
		return nil, err
	}
	storageKind, storageID := ownerStorageKey(owner)
	rows, err := tx.Query(ctx, `
		SELECT installation_id, owner_kind, owner_id, app_id, enabled,
		       installed_at, updated_at
		FROM app_installations
		WHERE owner_kind = $1 AND owner_id = $2
		ORDER BY installed_at, app_id`, storageKind, storageID)
	if err != nil {
		return nil, fmt.Errorf("list app installations: %w", err)
	}
	installations := []Installation{}
	for rows.Next() {
		installation, err := scanInstallation(rows)
		if err != nil {
			return nil, err
		}
		installations = append(installations, installation)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate app installations: %w", err)
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit app-installation read: %w", err)
	}
	return installations, nil
}

func (s *Store) Install(ctx context.Context, owner OwnerRef, actor participant.Ref, appID string) (Installation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Installation{}, fmt.Errorf("begin install app: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := s.authorizeMutation(ctx, tx, owner, actor); err != nil {
		return Installation{}, err
	}
	descriptor, err := descriptorByID(ctx, tx, appID)
	if err != nil {
		return Installation{}, err
	}
	if !descriptorAllowsOwner(descriptor, owner) {
		return Installation{}, ErrOwnerKindUnsupported
	}
	now := s.now().UTC()
	installation := Installation{
		InstallationID: newUUIDv7(), Owner: owner, AppID: appID,
		State: StateEnabled, InstalledAt: now, UpdatedAt: now,
	}
	storageKind, storageID := ownerStorageKey(owner)
	_, err = tx.Exec(ctx, `
		INSERT INTO app_installations
			(installation_id, owner_kind, owner_id, app_id, enabled,
			 installed_at, updated_at)
		VALUES ($1, $2, $3, $4, true, $5, $5)`,
		installation.InstallationID, storageKind, storageID, appID, now)
	if err != nil {
		if isUniqueViolation(err) {
			return Installation{}, ErrAlreadyInstalled
		}
		return Installation{}, fmt.Errorf("insert app installation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Installation{}, fmt.Errorf("commit install app: %w", err)
	}
	return installation, nil
}

func (s *Store) SetEnabled(ctx context.Context, owner OwnerRef, actor participant.Ref, appID string, enabled bool) (Installation, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Installation{}, fmt.Errorf("begin change app state: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := s.authorizeMutation(ctx, tx, owner, actor); err != nil {
		return Installation{}, err
	}
	storageKind, storageID := ownerStorageKey(owner)
	now := s.now().UTC()
	row := tx.QueryRow(ctx, `
		UPDATE app_installations SET enabled = $4, updated_at = $5
		WHERE owner_kind = $1 AND owner_id = $2 AND app_id = $3
		RETURNING installation_id, owner_kind, owner_id, app_id, enabled,
		          installed_at, updated_at`, storageKind, storageID, appID, enabled, now)
	installation, err := scanInstallation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Installation{}, ErrInstallationNotFound
	}
	if err != nil {
		return Installation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Installation{}, fmt.Errorf("commit app state: %w", err)
	}
	return installation, nil
}

// SetEnabledByID is the transport-facing lifecycle mutation. The canonical
// installation id addresses the binding; its owner is loaded server-side and
// then authorized by the same owner-domain rule used at installation.
func (s *Store) SetEnabledByID(ctx context.Context, installationID string, actor participant.Ref, enabled bool) (Installation, error) {
	owner, appID, err := s.installationAddress(ctx, installationID)
	if err != nil {
		return Installation{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Installation{}, fmt.Errorf("begin change app state: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := s.authorizeMutation(ctx, tx, owner, actor); err != nil {
		return Installation{}, err
	}
	storageKind, storageID := ownerStorageKey(owner)
	now := s.now().UTC()
	row := tx.QueryRow(ctx, `
		UPDATE app_installations SET enabled = $2, updated_at = $3
		WHERE installation_id = $1 AND owner_kind = $4 AND owner_id = $5 AND app_id = $6
		RETURNING installation_id, owner_kind, owner_id, app_id, enabled,
		          installed_at, updated_at`, installationID, enabled, now,
		storageKind, storageID, appID)
	installation, err := scanInstallation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Installation{}, ErrInstallationNotFound
	}
	if err != nil {
		return Installation{}, fmt.Errorf("change app state: %w", err)
	}
	if installation.AppID != appID {
		return Installation{}, errors.New("app installation address changed during mutation")
	}
	if err := tx.Commit(ctx); err != nil {
		return Installation{}, fmt.Errorf("commit app state: %w", err)
	}
	return installation, nil
}

func (s *Store) Uninstall(ctx context.Context, owner OwnerRef, actor participant.Ref, appID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin uninstall app: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := s.authorizeMutation(ctx, tx, owner, actor); err != nil {
		return err
	}
	storageKind, storageID := ownerStorageKey(owner)
	tag, err := tx.Exec(ctx, `
		DELETE FROM app_installations
		WHERE owner_kind = $1 AND owner_id = $2 AND app_id = $3`,
		storageKind, storageID, appID)
	if err != nil {
		return fmt.Errorf("delete app installation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrInstallationNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit uninstall app: %w", err)
	}
	return nil
}

func (s *Store) UninstallByID(ctx context.Context, installationID string, actor participant.Ref) error {
	owner, appID, err := s.installationAddress(ctx, installationID)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin uninstall app: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := s.authorizeMutation(ctx, tx, owner, actor); err != nil {
		return err
	}
	storageKind, storageID := ownerStorageKey(owner)
	tag, err := tx.Exec(ctx, `
		DELETE FROM app_installations
		WHERE installation_id = $1 AND owner_kind = $2 AND owner_id = $3 AND app_id = $4`,
		installationID, storageKind, storageID, appID)
	if err != nil {
		return fmt.Errorf("delete app installation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrInstallationNotFound
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit uninstall app: %w", err)
	}
	return nil
}

func (s *Store) authorizeRead(ctx context.Context, tx pgx.Tx, owner OwnerRef, actor participant.Ref) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	if err := actor.Validate(); err != nil {
		return err
	}
	if owner.Kind == OwnerWorkspace {
		if s.workspaces == nil {
			return ErrForbidden
		}
		return s.workspaces.RequireMembershipInTx(ctx, tx, owner.WorkspaceID, actor)
	}
	ref, ok := owner.Participant()
	if !ok || ref != actor {
		return ErrForbidden
	}
	exists, err := participant.Exists(ctx, tx, actor)
	if err != nil {
		return err
	}
	if !exists {
		return ErrForbidden
	}
	return nil
}

func (s *Store) authorizeMutation(ctx context.Context, tx pgx.Tx, owner OwnerRef, actor participant.Ref) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	if err := actor.Validate(); err != nil {
		return err
	}
	if owner.Kind == OwnerWorkspace {
		if s.workspaces == nil {
			return ErrForbidden
		}
		return s.workspaces.LockAndRequirePermission(
			ctx, tx, owner.WorkspaceID, actor, workspaceManageAppsPermission,
		)
	}
	ref, ok := owner.Participant()
	if !ok || ref != actor {
		return ErrForbidden
	}
	if err := participant.LockOwnIdentity(ctx, tx, ref); err != nil {
		return ErrForbidden
	}
	return nil
}

func descriptorByID(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, appID string) (Descriptor, error) {
	var descriptor Descriptor
	err := q.QueryRow(ctx, `
		SELECT app_id, display_name, workspace_owner_allowed, participant_owner_allowed
		FROM app_catalog WHERE app_id = $1`, appID,
	).Scan(&descriptor.AppID, &descriptor.DisplayName,
		&descriptor.WorkspaceOwnerAllowed, &descriptor.ParticipantOwnerAllowed)
	if errors.Is(err, pgx.ErrNoRows) {
		return Descriptor{}, ErrAppNotFound
	}
	if err != nil {
		return Descriptor{}, fmt.Errorf("load app descriptor: %w", err)
	}
	return descriptor, nil
}

func descriptorAllowsOwner(descriptor Descriptor, owner OwnerRef) bool {
	if owner.Kind == OwnerWorkspace {
		return descriptor.WorkspaceOwnerAllowed
	}
	return descriptor.ParticipantOwnerAllowed
}

// ownerStorageKey is a private relational encoding of the canonical
// Workspace | Participant(ParticipantRef) sum. Human/PersonalityAgent never
// become app-owner variants in the domain or wire contract.
func ownerStorageKey(owner OwnerRef) (string, string) {
	if owner.Kind == OwnerWorkspace {
		return string(OwnerWorkspace), owner.WorkspaceID
	}
	return string(owner.ParticipantRef.Kind), owner.ParticipantRef.ID
}

func ownerFromStorage(kind, id string) (OwnerRef, error) {
	switch participant.Kind(kind) {
	case participant.KindHuman:
		owner := ParticipantOwner(participant.Human(id))
		return owner, owner.Validate()
	case participant.KindPersonalityAgent:
		owner := ParticipantOwner(participant.PersonalityAgent(id))
		return owner, owner.Validate()
	default:
		if kind != string(OwnerWorkspace) {
			return OwnerRef{}, fmt.Errorf("unknown app installation storage owner kind %q", kind)
		}
		owner := WorkspaceOwner(id)
		return owner, owner.Validate()
	}
}

func (s *Store) installationAddress(ctx context.Context, installationID string) (OwnerRef, string, error) {
	if !isCanonicalUUIDv7(installationID) {
		return OwnerRef{}, "", ErrInstallationNotFound
	}
	var storageKind, storageID, appID string
	err := s.pool.QueryRow(ctx, `
		SELECT owner_kind, owner_id, app_id
		FROM app_installations WHERE installation_id = $1`, installationID,
	).Scan(&storageKind, &storageID, &appID)
	if errors.Is(err, pgx.ErrNoRows) {
		return OwnerRef{}, "", ErrInstallationNotFound
	}
	if err != nil {
		return OwnerRef{}, "", fmt.Errorf("load app installation address: %w", err)
	}
	owner, err := ownerFromStorage(storageKind, storageID)
	if err != nil {
		return OwnerRef{}, "", err
	}
	return owner, appID, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanInstallation(row rowScanner) (Installation, error) {
	var installation Installation
	var ownerKind, ownerID string
	var enabled bool
	err := row.Scan(&installation.InstallationID, &ownerKind, &ownerID,
		&installation.AppID, &enabled, &installation.InstalledAt, &installation.UpdatedAt)
	if err != nil {
		return Installation{}, err
	}
	installation.Owner, err = ownerFromStorage(ownerKind, ownerID)
	if err != nil {
		return Installation{}, err
	}
	if enabled {
		installation.State = StateEnabled
	} else {
		installation.State = StateDisabled
	}
	return installation, nil
}

func newUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		panic(fmt.Sprintf("generate UUIDv7: %v", err))
	}
	return id.String()
}

func isCanonicalUUIDv7(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value && id.Version() == 7 && id.Variant() == uuid.RFC4122
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
