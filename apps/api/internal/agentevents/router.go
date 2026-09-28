package agentevents

import (
	"fmt"
	"net/http"

	"github.com/sumi-studio/sumi/apps/api/internal/directchat"
)

// NewProductionMux assembles authenticated browser command, history and replay routes.
// The application installs CoreDirectChat before accepting requests.
func NewProductionMux(
	store *CommandStore,
	runtime *BrowserJournal,
	sv UserSessionAuthorizer,
	browserOrigins []string,
	authorizer DirectChatAuthorizer,
	lifecycleFence *directchat.LifecycleFence,
) (*http.ServeMux, *BrowserServer, error) {
	mux := http.NewServeMux()

	if store == nil {
		return nil, nil, fmt.Errorf("user command ingress: %w", errCommandAppenderRequired)
	}
	if runtime == nil {
		return nil, nil, fmt.Errorf("browser command admission: durable runtime gateway is required")
	}
	ingress, err := NewUserCommandIngress(runtime, sv)
	if err != nil {
		return nil, nil, fmt.Errorf("user command ingress: %w", err)
	}
	ingress.AllowedOrigins = browserOrigins
	ingress.Authorizer = authorizer
	ingress.LifecycleFence = lifecycleFence
	mux.Handle("POST /direct-chat/commands", ingress)

	browser := NewBrowserServer(sv, runtime, runtime)
	browser.commandIngress = ingress
	browser.AllowedOrigins = browserOrigins
	browser.Authorizer = authorizer
	browser.LifecycleFence = lifecycleFence
	mux.Handle("GET /direct-chat/ws", browser)
	mux.HandleFunc("GET /direct-chat/history", browser.ServeHistory)

	return mux, browser, nil
}
