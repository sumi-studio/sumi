package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"time"
)

// ReadinessCheck names one dependency whose current availability is required
// before a deploy may route browser traffic to this API process.
type ReadinessCheck struct {
	Name  string
	Check func(context.Context) error
}

// Readiness is deliberately separate from /health. A live process may be
// unavailable because its database, exact migration manifest, durable roots,
// or configured provisioner cannot currently be used.
type Readiness struct {
	Checks  []ReadinessCheck
	Timeout time.Duration
}

type readinessResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks"`
}

func (readiness Readiness) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	timeout := readiness.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	ctx, cancel := context.WithTimeout(request.Context(), timeout)
	defer cancel()

	checks := append([]ReadinessCheck(nil), readiness.Checks...)
	sort.Slice(checks, func(left, right int) bool { return checks[left].Name < checks[right].Name })
	result := readinessResponse{Status: "ready", Checks: make(map[string]string, len(checks))}
	for _, check := range checks {
		if check.Name == "" || check.Check == nil {
			result.Status = "unavailable"
			result.Checks["configuration"] = "failed"
			continue
		}
		if err := check.Check(ctx); err != nil {
			result.Status = "unavailable"
			result.Checks[check.Name] = "failed"
		} else {
			result.Checks[check.Name] = "ok"
		}
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result.Status = "unavailable"
			break
		}
	}

	response.Header().Set("Cache-Control", "no-store")
	response.Header().Set("Content-Type", "application/json")
	status := http.StatusOK
	if result.Status != "ready" {
		status = http.StatusServiceUnavailable
	}
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(result)
}
