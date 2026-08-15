package apps

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sumi-studio/sumi/apps/api/internal/canonicalid"
	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
	"github.com/sumi-studio/sumi/apps/api/internal/participant"
)

const (
	workspaceManageAppsPermission     = "manage_apps"
	appInstallationOwnerAppConstraint = "app_installations_owner_kind_owner_id_app_id_key"
)

// WorkspaceAuthorizer keeps app lifecycle dependent on Workspace's canonical
// commit-time authorization without moving app vocabulary into Workspace.
type WorkspaceAuthorizer interface {
	LockAndRequirePermission(context.Context, pgx.Tx, string, participant.Ref, string) error
	RequireMembershipInTx(context.Context, pgx.Tx, string, participant.Ref) error
}

type Store struct {
	pool                *pgxpool.Pool
	workspaces          WorkspaceAuthorizer
	directChatLifecycle *directchat.LifecycleFence
	now                 func() time.Time
}

func New(
	pool *pgxpool.Pool,
	workspaces WorkspaceAuthorizer,
	directChatLifecycle ...*directchat.LifecycleFence,
) *Store {
	var lifecycle *directchat.LifecycleFence
	if len(directChatLifecycle) > 0 {
		lifecycle = directChatLifecycle[0]
	}
	return &Store{
		pool:                pool,
		workspaces:          workspaces,
		directChatLifecycle: lifecycle,
		now:                 time.Now,
	}
}

func (s *Store) acquireLifecycleMutation(ctx context.Context, appID string) (func(), error) {
	if appID != directchat.AppID {
		return func() {}, nil
	}
	if s == nil || s.directChatLifecycle == nil {
		return nil, directchat.ErrLifecycleFenceUnavailable
	}
	return s.directChatLifecycle.AcquireMutation(ctx)
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
	return s.requireEnabledInstallationInTx(ctx, tx, installationID, nil, owner, appID)
}

// RequireEnabledInstallationEpochInTx additionally seals a lifecycle authority
// epoch so a disable -> enable cycle cannot revive an app operation, socket, or
// retry bound before the disable.
func (s *Store) RequireEnabledInstallationEpochInTx(
	ctx context.Context,
	tx pgx.Tx,
	installationID string,
	authorityEpoch int64,
	owner OwnerRef,
	appID string,
) (Installation, error) {
	if authorityEpoch < 1 {
		return Installation{}, ErrInstallationNotFound
	}
	return s.requireEnabledInstallationInTx(
		ctx, tx, installationID, &authorityEpoch, owner, appID,
	)
}

func (s *Store) requireEnabledInstallationInTx(
	ctx context.Context,
	tx pgx.Tx,
	installationID string,
	authorityEpoch *int64,
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
	query := `
		SELECT installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
		       installed_at, updated_at
		FROM app_installations
		WHERE installation_id = $1
		  AND owner_kind = $2 AND owner_id = $3 AND app_id = $4
	`
	args := []any{installationID, storageKind, storageID, appID}
	if authorityEpoch != nil {
		query += " AND authority_epoch = $5"
		args = append(args, *authorityEpoch)
	}
	query += " FOR SHARE"
	row := tx.QueryRow(ctx, query, args...)
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
	return requireEnabledInstallationRead(
		ctx, tx, installationID, nil, owner, appID, "load exact app snapshot admission",
	)
}

// RequireEnabledInstallationEpochInSnapshot is RequireEnabledInstallationInSnapshot
// additionally sealed to one lifecycle authority epoch, for app read screens
// whose caller bound that epoch when it resolved the installation.
func (s *Store) RequireEnabledInstallationEpochInSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	installationID string,
	authorityEpoch int64,
	owner OwnerRef,
	appID string,
) (Installation, error) {
	if authorityEpoch < 1 {
		return Installation{}, ErrInstallationNotFound
	}
	return requireEnabledInstallationRead(
		ctx, tx, installationID, &authorityEpoch, owner, appID,
		"load exact app snapshot admission",
	)
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
	return requireEnabledInstallationRead(
		ctx, s.pool, installationID, nil, owner, appID, "read exact app lifecycle state",
	)
}

// RequireEnabledInstallationEpoch is RequireEnabledInstallation additionally
// sealed to one lifecycle authority epoch. An epoch that no longer matches
// reads as ErrInstallationNotFound: the caller's binding predates a disable
// and must be resolved again rather than revived.
func (s *Store) RequireEnabledInstallationEpoch(
	ctx context.Context,
	installationID string,
	authorityEpoch int64,
	owner OwnerRef,
	appID string,
) (Installation, error) {
	if authorityEpoch < 1 {
		return Installation{}, ErrInstallationNotFound
	}
	return requireEnabledInstallationRead(
		ctx, s.pool, installationID, &authorityEpoch, owner, appID,
		"read exact app lifecycle state",
	)
}

type installationRowReader interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func requireEnabledInstallationRead(
	ctx context.Context,
	reader installationRowReader,
	installationID string,
	authorityEpoch *int64,
	owner OwnerRef,
	appID string,
	failure string,
) (Installation, error) {
	if !isCanonicalUUIDv7(installationID) {
		return Installation{}, ErrInstallationNotFound
	}
	if err := owner.Validate(); err != nil {
		return Installation{}, err
	}
	storageKind, storageID := ownerStorageKey(owner)
	query := `
		SELECT installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
		       installed_at, updated_at
		FROM app_installations
		WHERE installation_id = $1
		  AND owner_kind = $2 AND owner_id = $3 AND app_id = $4`
	args := []any{installationID, storageKind, storageID, appID}
	if authorityEpoch != nil {
		query += " AND authority_epoch = $5"
		args = append(args, *authorityEpoch)
	}
	installation, err := scanInstallation(reader.QueryRow(ctx, query, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return Installation{}, ErrInstallationNotFound
	}
	if err != nil {
		return Installation{}, fmt.Errorf("%s: %w", failure, err)
	}
	if installation.State != StateEnabled {
		return Installation{}, ErrAppDisabled
	}
	return installation, nil
}

// ResolveEnabledInstallation turns an authenticated, model-selected app owner
// address into the exact current installation identity used at bind time. It
// never supplies a default Workspace and never accepts an installation id from
// the model. Callers must seal the returned id and authority epoch into the
// invocation and use RequireEnabledInstallationEpochInTx again at commit.
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
		SELECT installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
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
		SELECT installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
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
	return s.install(ctx, owner, actor, appID, "")
}

// InstallAtOperation applies one durable browser install intent. Its receipt
// survives uninstall, so a delayed request with the same operation id can
// return only the historical terminal outcome and can never recreate a
// removed installation.
func (s *Store) InstallAtOperation(ctx context.Context, owner OwnerRef, actor participant.Ref, appID, operationID string) (Installation, error) {
	if err := ValidateInstallOperationID(operationID); err != nil {
		return Installation{}, err
	}
	return s.install(ctx, owner, actor, appID, operationID)
}

func (s *Store) install(ctx context.Context, owner OwnerRef, actor participant.Ref, appID, operationID string) (Installation, error) {
	releaseLifecycle, err := s.acquireLifecycleMutation(ctx, appID)
	if err != nil {
		return Installation{}, err
	}
	defer releaseLifecycle()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Installation{}, fmt.Errorf("begin install app: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := s.authorizeMutation(ctx, tx, owner, actor); err != nil {
		return Installation{}, err
	}
	if operationID != "" {
		// A terminal receipt is the historical truth for this exact operation,
		// even if the catalog has changed since the original request. Read it
		// before current descriptor validation. If no receipt exists, the later
		// atomic claim still resolves a concurrent first request.
		historical, receiptErr := readInstallOperationReceipt(
			ctx, tx, owner, appID, operationID,
		)
		if receiptErr == nil {
			return historical, nil
		}
		if !errors.Is(receiptErr, pgx.ErrNoRows) {
			return Installation{}, receiptErr
		}
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
		State: StateEnabled, AuthorityEpoch: 1, InstalledAt: now, UpdatedAt: now,
	}
	storageKind, storageID := ownerStorageKey(owner)
	if operationID == "" {
		_, err = tx.Exec(ctx, `
		INSERT INTO app_installations
			(installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
			 installed_at, updated_at)
		VALUES ($1, $2, $3, $4, true, 1, $5, $5)`,
			installation.InstallationID, storageKind, storageID, appID, now)
		if err != nil {
			if isUniqueConstraint(err, appInstallationOwnerAppConstraint) {
				return Installation{}, ErrAlreadyInstalled
			}
			return Installation{}, fmt.Errorf("insert app installation: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Installation{}, fmt.Errorf("commit install app: %w", err)
		}
		return installation, nil
	}

	claimed, historical, err := claimInstallOperation(ctx, tx, owner, appID, operationID, now)
	if err != nil {
		return Installation{}, err
	}
	if !claimed {
		return historical, nil
	}

	row := tx.QueryRow(ctx, `
		INSERT INTO app_installations
			(installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
			 installed_at, updated_at)
		VALUES ($1, $2, $3, $4, true, 1, $5, $5)
		ON CONFLICT ON CONSTRAINT app_installations_owner_kind_owner_id_app_id_key
		DO NOTHING
		RETURNING installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
		          installed_at, updated_at`,
		installation.InstallationID, storageKind, storageID, appID, now)
	installation, err = scanInstallation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := completeInstallOperationAlreadyInstalled(
			ctx, tx, storageKind, storageID, operationID, now,
		); err != nil {
			return Installation{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return Installation{}, fmt.Errorf("commit existing app install intent: %w", err)
		}
		return Installation{}, ErrInstallIntentAlreadyInstalled
	}
	if err != nil {
		// In particular, an installation_id primary-key collision is not the
		// owner/app convergence condition and rolls the receipt back with this
		// transaction instead of being mislabeled AlreadyInstalled.
		return Installation{}, fmt.Errorf("insert exact app installation: %w", err)
	}
	if err := completeInstallOperationInstalled(
		ctx, tx, storageKind, storageID, operationID, installation, now,
	); err != nil {
		return Installation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Installation{}, fmt.Errorf("commit exact app install intent: %w", err)
	}
	return installation, nil
}

func claimInstallOperation(
	ctx context.Context,
	tx pgx.Tx,
	owner OwnerRef,
	appID string,
	operationID string,
	now time.Time,
) (bool, Installation, error) {
	storageKind, storageID := ownerStorageKey(owner)
	tag, err := tx.Exec(ctx, `
		INSERT INTO app_install_operation_receipts
			(owner_kind, owner_id, operation_id, app_id, status, created_at)
		VALUES ($1, $2, $3, $4, 'pending', $5)
		ON CONFLICT (owner_kind, owner_id, operation_id) DO NOTHING`,
		storageKind, storageID, operationID, appID, now)
	if err != nil {
		return false, Installation{}, fmt.Errorf("claim app install operation: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, Installation{}, nil
	}
	installation, err := readInstallOperationReceipt(
		ctx, tx, owner, appID, operationID,
	)
	return false, installation, err
}

func readInstallOperationReceipt(
	ctx context.Context,
	tx pgx.Tx,
	owner OwnerRef,
	appID string,
	operationID string,
) (Installation, error) {
	storageKind, storageID := ownerStorageKey(owner)
	var (
		receiptAppID   string
		status         string
		installationID pgtype.Text
		enabled        pgtype.Bool
		authorityEpoch pgtype.Int8
		installedAt    pgtype.Timestamptz
		updatedAt      pgtype.Timestamptz
		completedAt    pgtype.Timestamptz
	)
	err := tx.QueryRow(ctx, `
		SELECT app_id, status, installation_id::text, enabled, authority_epoch,
		       installed_at, updated_at, completed_at
		FROM app_install_operation_receipts
		WHERE owner_kind = $1 AND owner_id = $2 AND operation_id = $3`,
		storageKind, storageID, operationID,
	).Scan(
		&receiptAppID, &status, &installationID, &enabled, &authorityEpoch,
		&installedAt, &updatedAt, &completedAt,
	)
	if err != nil {
		return Installation{}, fmt.Errorf("read app install operation receipt: %w", err)
	}
	if receiptAppID != appID {
		return Installation{}, ErrInstallIntentMismatch
	}
	resultPresent := installationID.Valid || enabled.Valid || authorityEpoch.Valid ||
		installedAt.Valid || updatedAt.Valid
	switch status {
	case "pending":
		if resultPresent || completedAt.Valid {
			return Installation{}, fmt.Errorf("%w: malformed pending receipt", ErrInstallIntentIncomplete)
		}
		return Installation{}, ErrInstallIntentIncomplete
	case "already_installed":
		if resultPresent || !completedAt.Valid {
			return Installation{}, fmt.Errorf("%w: malformed existing receipt", ErrInstallIntentIncomplete)
		}
		return Installation{}, ErrInstallIntentAlreadyInstalled
	case "installed":
		if !installationID.Valid || !isCanonicalUUIDv7(installationID.String) ||
			!enabled.Valid || !enabled.Bool ||
			!authorityEpoch.Valid || authorityEpoch.Int64 != 1 ||
			!installedAt.Valid || !updatedAt.Valid || !completedAt.Valid ||
			!updatedAt.Time.Equal(installedAt.Time) {
			return Installation{}, fmt.Errorf("%w: malformed installed receipt", ErrInstallIntentIncomplete)
		}
		return Installation{
			InstallationID: installationID.String,
			Owner:          owner,
			AppID:          appID,
			State:          StateEnabled,
			AuthorityEpoch: authorityEpoch.Int64,
			InstalledAt:    installedAt.Time,
			UpdatedAt:      updatedAt.Time,
		}, nil
	default:
		return Installation{}, fmt.Errorf("%w: unknown receipt status", ErrInstallIntentIncomplete)
	}
}

func completeInstallOperationAlreadyInstalled(
	ctx context.Context,
	tx pgx.Tx,
	storageKind string,
	storageID string,
	operationID string,
	now time.Time,
) error {
	tag, err := tx.Exec(ctx, `
		UPDATE app_install_operation_receipts
		SET status = 'already_installed', completed_at = $4
		WHERE owner_kind = $1 AND owner_id = $2 AND operation_id = $3
		  AND status = 'pending'`,
		storageKind, storageID, operationID, now)
	if err != nil {
		return fmt.Errorf("complete existing app install operation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: pending receipt disappeared", ErrInstallIntentIncomplete)
	}
	return nil
}

func completeInstallOperationInstalled(
	ctx context.Context,
	tx pgx.Tx,
	storageKind string,
	storageID string,
	operationID string,
	installation Installation,
	completedAt time.Time,
) error {
	tag, err := tx.Exec(ctx, `
		UPDATE app_install_operation_receipts
		SET status = 'installed', installation_id = $4, enabled = $5,
		    authority_epoch = $6, installed_at = $7, updated_at = $8,
		    completed_at = $9
		WHERE owner_kind = $1 AND owner_id = $2 AND operation_id = $3
		  AND status = 'pending'`,
		storageKind, storageID, operationID,
		installation.InstallationID, installation.State == StateEnabled,
		installation.AuthorityEpoch, installation.InstalledAt, installation.UpdatedAt,
		completedAt)
	if err != nil {
		return fmt.Errorf("complete app install operation: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: pending receipt disappeared", ErrInstallIntentIncomplete)
	}
	return nil
}

func (s *Store) SetEnabled(ctx context.Context, owner OwnerRef, actor participant.Ref, appID string, enabled bool) (Installation, error) {
	releaseLifecycle, err := s.acquireLifecycleMutation(ctx, appID)
	if err != nil {
		return Installation{}, err
	}
	defer releaseLifecycle()
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
		UPDATE app_installations
		SET authority_epoch = CASE
		        WHEN enabled AND NOT $4 THEN authority_epoch + 1
		        ELSE authority_epoch
		    END,
		    enabled = $4,
		    updated_at = $5
		WHERE owner_kind = $1 AND owner_id = $2 AND app_id = $3
		RETURNING installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
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
	return s.setEnabledByID(ctx, installationID, actor, enabled, nil)
}

// SetEnabledByIDAtEpoch applies an exact desired state only while the
// installation still has the authority epoch observed by the caller. This
// makes a replay of an interrupted browser intent converge without allowing a
// stale intent to overwrite a later disable/re-enable lifecycle.
func (s *Store) SetEnabledByIDAtEpoch(ctx context.Context, installationID string, actor participant.Ref, enabled bool, expectedAuthorityEpoch int64) (Installation, error) {
	if expectedAuthorityEpoch < 1 {
		return Installation{}, ErrAuthorityEpochStale
	}
	return s.setEnabledByID(ctx, installationID, actor, enabled, &expectedAuthorityEpoch)
}

func (s *Store) setEnabledByID(ctx context.Context, installationID string, actor participant.Ref, enabled bool, expectedAuthorityEpoch *int64) (Installation, error) {
	owner, appID, err := s.installationAddress(ctx, installationID)
	if err != nil {
		return Installation{}, err
	}
	// Address classification completes before the process fence is acquired and
	// holds no database lock. The exact row is revalidated in the transaction,
	// whose lock order remains lifecycle fence then PostgreSQL.
	releaseLifecycle, err := s.acquireLifecycleMutation(ctx, appID)
	if err != nil {
		return Installation{}, err
	}
	defer releaseLifecycle()
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
		UPDATE app_installations
		SET authority_epoch = CASE
		        WHEN enabled AND NOT $2 THEN authority_epoch + 1
		        ELSE authority_epoch
		    END,
		    enabled = $2,
		    updated_at = CASE
		        WHEN $7::bigint IS NOT NULL AND enabled = $2 THEN updated_at
		        ELSE $3
		    END
		WHERE installation_id = $1 AND owner_kind = $4 AND owner_id = $5 AND app_id = $6
		  AND ($7::bigint IS NULL OR authority_epoch = $7)
		RETURNING installation_id, owner_kind, owner_id, app_id, enabled, authority_epoch,
		          installed_at, updated_at`, installationID, enabled, now,
		storageKind, storageID, appID, expectedAuthorityEpoch)
	installation, err := scanInstallation(row)
	if errors.Is(err, pgx.ErrNoRows) {
		if expectedAuthorityEpoch == nil {
			return Installation{}, ErrInstallationNotFound
		}
		var currentEpoch int64
		err = tx.QueryRow(ctx, `
			SELECT authority_epoch
			FROM app_installations
			WHERE installation_id = $1 AND owner_kind = $2 AND owner_id = $3 AND app_id = $4`,
			installationID, storageKind, storageID, appID,
		).Scan(&currentEpoch)
		if errors.Is(err, pgx.ErrNoRows) {
			return Installation{}, ErrInstallationNotFound
		}
		if err != nil {
			return Installation{}, fmt.Errorf("classify stale app authority: %w", err)
		}
		return Installation{}, ErrAuthorityEpochStale
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
	releaseLifecycle, err := s.acquireLifecycleMutation(ctx, appID)
	if err != nil {
		return err
	}
	defer releaseLifecycle()
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
	// See SetEnabledByID: no database lock is held while acquiring the process
	// fence, and the addressed row is checked again under the mutation tx.
	releaseLifecycle, err := s.acquireLifecycleMutation(ctx, appID)
	if err != nil {
		return err
	}
	defer releaseLifecycle()
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
		&installation.AppID, &enabled, &installation.AuthorityEpoch,
		&installation.InstalledAt, &installation.UpdatedAt)
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
	return canonicalid.IsUUIDv7(value)
}

func isUniqueConstraint(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505" &&
		pgErr.ConstraintName == constraint
}
