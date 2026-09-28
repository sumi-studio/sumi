package modelconnections

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// A lock wait must never spend the time needed to receive and save a
// consuming issuer response. Each phase gets its own bounded, detached
// context. All contended rows are locked before contacting the issuer.
// The issuer's HTTP client additionally bounds each request at 20 seconds.
const (
	chatGPTLockTimeout    = 5 * time.Second
	chatGPTRefreshTimeout = 25 * time.Second
	chatGPTPollTimeout    = 45 * time.Second // poll + one-time code exchange
	chatGPTSaveTimeout    = 5 * time.Second
	chatGPTCancelWait     = 50 * time.Second // wait out an already-consuming poll
)

func chatGPTPhase(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func rollbackChatGPT(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), chatGPTSaveTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}
