// Package localterminal runs the shared terminal in the Local install's own
// workspace. It is a local user process, not an isolation boundary or sandbox.
package localterminal

import "github.com/sumi-studio/sumi/apps/api/internal/termexec"

type Config struct {
	PersonaID     string
	WorkspaceRoot string
	JournalRoot   string
	// OutputLimit bounds retained backend scrollback. The Core independently
	// persists its shared scrollback; no user workspace files are pruned here.
	OutputLimit int
}

type ProcessBackend interface {
	termexec.ProcessAPI
	// Close is host shutdown, not attachment or driver shutdown. It closes the
	// PTYs and stops shell leaders we still own. It cannot prove that arbitrary
	// unsandboxed descendants have stopped.
	Close() error
}
