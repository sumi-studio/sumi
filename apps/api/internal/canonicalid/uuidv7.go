// Package canonicalid owns the shared wire-shape checks for canonical IDs.
package canonicalid

import "github.com/google/uuid"

// IsUUIDv7 accepts only the lowercase RFC 4122 rendering of a UUIDv7.
func IsUUIDv7(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.String() == value && id.Version() == 7 && id.Variant() == uuid.RFC4122
}
