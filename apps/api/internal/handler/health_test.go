package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReady(t *testing.T) {
	t.Run("ready without optional dependencies", func(t *testing.T) {
		response := httptest.NewRecorder()
		Ready()(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
		if response.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", response.Code)
		}
	})

	t.Run("unavailable dependency", func(t *testing.T) {
		response := httptest.NewRecorder()
		Ready(func(context.Context) error { return errors.New("database unavailable") })(
			response,
			httptest.NewRequest(http.MethodGet, "/ready", nil),
		)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", response.Code)
		}
	})
}
