// Package localterminal runs the shared terminal in the Local install's own
// workspace. It is a local user process, not an isolation boundary or sandbox.
package localterminal

import (
	"errors"

	"github.com/sumi-studio/sumi/apps/api/internal/termexec"
)

// ErrJournal marks a terminal journal that could not be read or trusted at
// startup — a malformed, unreadable or foreign record, or a failed rewrite
// of a retained marker. Every record's bytes are preserved exactly; the
// failure is contained to the terminal capability, so the service may start
// the rest of the install while the journal awaits repair and a restart.
// Configuration and ownership problems (bad roots, a live owner.lock) are
// NOT journal errors — they fail the whole wiring as before.
var ErrJournal = errors.New("local terminal journal cannot be trusted")

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
