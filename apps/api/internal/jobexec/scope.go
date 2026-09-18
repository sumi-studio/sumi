package jobexec

import (
	"context"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
)

// FileScopeEnsurer verifies the persona's canonical files scope through
// filesvc — the storage authority — before a job launch. It uses the
// allowlisted mkdir operation on the scope root: the call creates the scope
// when missing and is a no-op when present, so it doubles as the verified
// creation path for personas that have never had a file operation. The
// provisioner then independently re-verifies mount, volume, scope and
// binding before it binds anything.
type FileScopeEnsurer struct {
	Client *fileaccess.Client
}

func (e *FileScopeEnsurer) EnsureScope(ctx context.Context, personaID string) error {
	scope, err := fileaccess.ScopeForPersona(personaID)
	if err != nil {
		return err
	}
	// A fixed op key makes every ensure a replay of the first committed
	// creation — retries after a lost response cannot double-create, and a
	// divergent replay would surface as idempotency_conflict rather than a
	// silent second mutation.
	_, _, err = e.Client.MkdirKeyed(ctx, scope, "", "jobexec-scope-ensure")
	return err
}
