package handler

import (
	"encoding/json"
	"net/http"
)

// Health implements GET /v1/health from contracts/openapi.yaml.
func Health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
