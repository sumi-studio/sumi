package apps

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestAppInstallationUniqueConstraintClassificationIsExact(t *testing.T) {
	ownerApp := &pgconn.PgError{
		Code: "23505", ConstraintName: appInstallationOwnerAppConstraint,
	}
	primaryKey := &pgconn.PgError{
		Code: "23505", ConstraintName: "app_installations_pkey",
	}
	if !isUniqueConstraint(ownerApp, appInstallationOwnerAppConstraint) {
		t.Fatal("owner/app constraint was not classified")
	}
	if isUniqueConstraint(primaryKey, appInstallationOwnerAppConstraint) {
		t.Fatal("installation_id primary-key collision was classified as owner/app convergence")
	}
}

func TestInstallOperationIDRequiresCanonicalUUIDv4(t *testing.T) {
	if err := ValidateInstallOperationID("00000000-0000-4000-8000-000000000101"); err != nil {
		t.Fatalf("canonical UUIDv4 rejected: %v", err)
	}
	for _, value := range []string{
		"00000000-0000-7000-8000-000000000101",
		"00000000-0000-4000-8000-0000000001AB",
		"00000000000040008000000000000101",
		"not-a-uuid",
	} {
		if err := ValidateInstallOperationID(value); err != ErrInstallOperationInvalid {
			t.Errorf("ValidateInstallOperationID(%q) = %v", value, err)
		}
	}
}

func TestDirectChatProvisionOperationIDIsCanonicalUUIDv4(t *testing.T) {
	operationID := directChatProvisionOperationID("0198f0f4-9b72-7000-8000-000000000202")
	if err := ValidateInstallOperationID(operationID); err != nil {
		t.Fatalf("direct-chat provision operation id %q rejected: %v", operationID, err)
	}
}
