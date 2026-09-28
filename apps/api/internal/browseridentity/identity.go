// Package browseridentity carries an authenticated browser session across API services.
package browseridentity

import "context"

type Identity struct {
	HumanID   string
	SessionID string
	// Authorize revalidates the initiating session while executing a side effect.
	Authorize func(context.Context, func(context.Context) error) error
}
