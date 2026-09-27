package termexec

import (
	"context"

	"github.com/sumi-studio/sumi/apps/api/internal/fileaccess"
)

// FileScopeEnsurer verifies the persona's canonical files scope
// through filesvc — the storage authority — before a session launch.
// Same contract as the job driver's ensurer with its own dedup key:
// the allowlisted mkdir on the scope root creates the scope when
// missing and replays cleanly when present, and the provisioner
// independently re-verifies mount, volume, scope and binding before
// it binds anything.
type FileScopeEnsurer struct {
	Client *fileaccess.Client
}

func (e *FileScopeEnsurer) EnsureScope(ctx context.Context, personaID string) error {
	scope, err := fileaccess.ScopeForPersona(personaID)
	if err != nil {
		return err
	}
	_, _, err = e.Client.MkdirKeyed(ctx, scope, "", "termexec-scope-ensure")
	return err
}
