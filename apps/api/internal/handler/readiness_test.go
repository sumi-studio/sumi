package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthIsExplicitLivenessNotReadiness(t *testing.T) {
	recorder := httptest.NewRecorder()
	Health(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("health response=%d headers=%v", recorder.Code, recorder.Header())
	}
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "alive" {
		t.Fatalf("health claimed a dependency state: %v", body)
	}
}

func TestReadinessFailsClosedOnDependencyFailure(t *testing.T) {
	readiness := Readiness{Checks: []ReadinessCheck{
		{Name: "database", Check: func(context.Context) error { return nil }},
		{Name: "migration_manifest", Check: func(context.Context) error { return errors.New("changed") }},
	}}
	recorder := httptest.NewRecorder()
	readiness.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("readiness response=%d headers=%v", recorder.Code, recorder.Header())
	}
	var body readinessResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "unavailable" || body.Checks["database"] != "ok" || body.Checks["migration_manifest"] != "failed" {
		t.Fatalf("unexpected readiness body: %+v", body)
	}
}

func TestReadinessHonorsSharedTimeout(t *testing.T) {
	readiness := Readiness{
		Timeout: 10 * time.Millisecond,
		Checks: []ReadinessCheck{{Name: "blocked", Check: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}}},
	}
	recorder := httptest.NewRecorder()
	readiness.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("timed-out readiness returned %d", recorder.Code)
	}
}
