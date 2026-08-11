package agentevents

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/sumi-studio/sumi/apps/api/internal/canonicalid"
)

var errDirectChatInvalidScope = errors.New("invalid direct-chat transport scope")

type directChatScope struct {
	InstallationID string
	AuthorityEpoch int64
}

// directChatScopeFromRequest reads the exact installation identity and its
// durable lifecycle epoch from transport metadata. It deliberately rejects
// absent, empty, duplicated, non-canonical, and overflowing values before any
// runtime spawn, upgrade, or command allocation.
func directChatScopeFromRequest(r *http.Request) (directChatScope, error) {
	query := r.URL.Query()
	installationValues, installationOK := query["installation_id"]
	epochValues, epochOK := query["authority_epoch"]
	if !installationOK || len(installationValues) != 1 ||
		!canonicalid.IsUUIDv7(installationValues[0]) ||
		!epochOK || len(epochValues) != 1 || !isCanonicalPositiveInt64(epochValues[0]) {
		return directChatScope{}, errDirectChatInvalidScope
	}
	epoch, err := strconv.ParseInt(epochValues[0], 10, 64)
	if err != nil {
		return directChatScope{}, errDirectChatInvalidScope
	}
	return directChatScope{
		InstallationID: installationValues[0],
		AuthorityEpoch: epoch,
	}, nil
}

func writeDirectChatInvalidScope(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write([]byte("invalid_scope"))
}

func isCanonicalPositiveInt64(value string) bool {
	if value == "" || value[0] == '0' {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	_, err := strconv.ParseInt(value, 10, 64)
	return err == nil
}
