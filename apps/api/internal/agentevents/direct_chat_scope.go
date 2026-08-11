package agentevents

import (
	"errors"
	"net/http"
)

var errDirectChatInvalidScope = errors.New("invalid direct-chat installation scope")

// directChatInstallationID reads the exact app installation capability from
// transport metadata. It deliberately rejects absent, empty, and duplicated
// identities before any runtime spawn, upgrade, or command allocation.
func directChatInstallationID(r *http.Request) (string, error) {
	values, ok := r.URL.Query()["installation_id"]
	if !ok || len(values) != 1 || values[0] == "" {
		return "", errDirectChatInvalidScope
	}
	return values[0], nil
}
