// Package runtimeprovision defines the privileged host provisioner boundary.
//
// The API and agent roles speak this typed protocol over a root-managed Unix
// socket. Only a Backend implementation behind the daemon may reach a machine
// runtime such as Docker or, in the future, Firecracker.
package runtimeprovision

import (
	"errors"
	"regexp"
)

var canonicalPAID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func ValidatePersonalityAgentID(value string) error {
	if !canonicalPAID.MatchString(value) {
		return errors.New("personality_agent_id must be a canonical lowercase UUIDv7")
	}
	return nil
}
