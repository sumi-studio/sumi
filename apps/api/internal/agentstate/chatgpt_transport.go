package agentstate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sumi-studio/sumi/apps/api/internal/modelconnections"
)

const chatGPTResponsesURL = modelconnections.ChatGPTBaseURL + "/responses"
const chatGPTRejectedHeader = "X-Sumi-ChatGPT-Rejected-Token"
const chatGPTErrorHeader = "X-Sumi-Model-Error"
const chatGPTStreamLimit = 32 << 20
const chatGPTTransportTimeout = 120 * time.Second

var chatGPTLiteModel = regexp.MustCompile(`^(gpt-6(\.\d+)?-|gpt-5\.6-|gpt-daybreak-|codex-auto-review$)`)
var chatGPTDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)
var chatGPTDiagnostic = regexp.MustCompile(`^[a-z]{1,20}(_[a-z]{1,20}){0,4}$`)

// A single HTTP transport operation, not an inference loop. Core owns the
// request, SSE interpretation, continuation and tools. Credentials and the
// upstream URL/headers are owned by this API. No redirect or network retry.
type chatGPTTransportRequest struct {
	ConnectionID string          `json:"connection_id"`
	Version      string          `json:"connection_version"`
	Rejected     string          `json:"rejected_token_sha256,omitempty"`
	Request      json.RawMessage `json:"request"`
}

func chatGPTTransportError(w http.ResponseWriter, status int, code string) {
	w.Header().Set(chatGPTErrorHeader, code)
	writeJSON(w, status, map[string]string{"error": code})
}

// Check the current authority and selection both before resolving a grant
// and after a potentially slow refresh. Reconnection/model/selection changes
// during that wait cannot dispatch a request built under an older binding.
// Admission is a snapshot: a later setting change affects the next request,
// not a stream already dispatched under this authority.
func (s *Server) chatGPTAdmission(ctx context.Context, persona string, req chatGPTTransportRequest, body map[string]json.RawMessage) (string, error) {
	p, err := s.store.persona(ctx, persona)
	if err != nil {
		return "", err
	}
	if p.Authority != "active" || p.HumanID == nil || s.conns == nil {
		return "", errChatGPTBindingChanged
	}
	if len(p.ModelIntent) > 0 {
		var intent struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(p.ModelIntent, &intent) != nil || intent.Kind != "api" {
			return "", errChatGPTBindingChanged
		}
	}
	sel, exists, err := s.conns.Selected(ctx, *p.HumanID)
	if err != nil {
		return "", err
	}
	if !exists || sel.Kind != "api" || sel.ConnectionID != req.ConnectionID {
		return "", errChatGPTBindingChanged
	}
	meta, err := s.conns.Describe(ctx, *p.HumanID, sel.ConnectionID)
	if err != nil {
		return "", err
	}
	var model string
	var reasoning struct {
		Effort string `json:"effort"`
	}
	if json.Unmarshal(body["model"], &model) != nil {
		return "", errChatGPTBindingChanged
	}
	if raw, ok := body["reasoning"]; ok && json.Unmarshal(raw, &reasoning) != nil {
		return "", errChatGPTBindingChanged
	}
	if meta.Connection.Preset != modelconnections.ChatGPTPreset || meta.Connection.BaseURL != modelconnections.ChatGPTBaseURL || meta.Version != req.Version || meta.Connection.Model != model || meta.Connection.ReasoningEffort != reasoning.Effort {
		return "", errChatGPTBindingChanged
	}
	return *p.HumanID, nil
}

var errChatGPTBindingChanged = errors.New("model binding changed")

func (s *Server) chatGPTResponses(w http.ResponseWriter, r *http.Request) {
	persona, ok := s.scope(w, r) // Core capabilities only; browser cookies do not authenticate.
	if !ok {
		return
	}
	// Bound input separately from the streamed output. Never echo malformed
	// request content (which includes private context) in an error message.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(15 * time.Second))
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBody))
	_ = rc.SetReadDeadline(time.Time{})
	var req chatGPTTransportRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err != nil || dec.Decode(&req) != nil || dec.Decode(new(any)) != io.EOF || req.ConnectionID == "" || req.Version == "" || (req.Rejected != "" && !chatGPTDigest.MatchString(req.Rejected)) {
		chatGPTTransportError(w, 400, "invalid_request")
		return
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(req.Request, &body) != nil || body == nil || string(body["stream"]) != "true" || string(body["store"]) != "false" {
		chatGPTTransportError(w, 400, "invalid_request")
		return
	}
	// Stateless Responses only. A previous server response/conversation must
	// not bypass Core's account-bound, durable continuation protocol.
	for _, field := range []string{"previous_response_id", "conversation", "background"} {
		if _, exists := body[field]; exists {
			chatGPTTransportError(w, 400, "invalid_request")
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), chatGPTTransportTimeout)
	defer cancel()
	human, err := s.chatGPTAdmission(ctx, persona, req, body)
	if err != nil {
		chatGPTTransportError(w, 409, "binding_changed")
		return
	}
	access, err := s.conns.ResolveChatGPT(ctx, human, req.ConnectionID, req.Rejected)
	if err != nil {
		code, status := "credential_unavailable", 503
		switch {
		case errors.Is(err, modelconnections.ErrChatGPTDisabled):
			code, status = "model_connection_disabled", 409
		case errors.Is(err, modelconnections.ErrReconnectRequired):
			code, status = "model_reconnect_required", 409
		case errors.Is(err, modelconnections.ErrNotFound):
			code, status = "binding_changed", 409
		}
		chatGPTTransportError(w, status, code)
		return
	}
	currentHuman, err := s.chatGPTAdmission(ctx, persona, req, body)
	if err != nil || currentHuman != human || access.Version != req.Version {
		chatGPTTransportError(w, 409, "binding_changed")
		return
	}
	if ctx.Err() != nil {
		return
	} // Refresh may finish detached; do not start a cancelled model call.
	upstream, err := http.NewRequestWithContext(ctx, http.MethodPost, chatGPTResponsesURL, bytes.NewReader(req.Request))
	if err != nil {
		chatGPTTransportError(w, 400, "invalid_request")
		return
	}
	upstream.Header.Set("Authorization", "Bearer "+access.AccessToken)
	upstream.Header.Set("ChatGPT-Account-ID", access.AccountID)
	upstream.Header.Set("Content-Type", "application/json")
	upstream.Header.Set("Accept", "text/event-stream")
	upstream.Header.Set("User-Agent", "sumi-secretary/alpha")
	upstream.Header.Set("originator", "sumi")
	upstream.Header.Set("session_id", persona)
	var requestModel string
	_ = json.Unmarshal(body["model"], &requestModel)
	if chatGPTLiteModel.MatchString(requestModel) {
		upstream.Header.Set("x-openai-internal-codex-responses-lite", "true")
	}
	client := http.Client{Timeout: chatGPTTransportTimeout}
	if s.chatGPTHTTP != nil {
		client = *s.chatGPTHTTP
	} // in-package synthetic transport fixture only
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	res, err := client.Do(upstream)
	if err != nil {
		chatGPTTransportError(w, 502, "transport_ambiguous")
		return
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		if seconds, err := strconv.Atoi(res.Header.Get("Retry-After")); err == nil && seconds >= 0 && seconds <= 86400 {
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
		}
		if res.StatusCode == 401 {
			w.Header().Set(chatGPTRejectedHeader, modelconnections.TokenDigest(access.AccessToken))
		}
		// Error pages may contain private material. Keep only bounded error
		// identifiers and a numeric usage-reset time, never arbitrary text.
		b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		var payload struct {
			Error json.RawMessage `json:"error"`
		}
		var detail map[string]any
		_ = json.Unmarshal(b, &payload)
		if json.Unmarshal(payload.Error, &detail) != nil {
			var code string
			if json.Unmarshal(payload.Error, &code) == nil {
				detail = map[string]any{"code": code}
			}
		}
		safe := map[string]any{"message": "ChatGPT request rejected (HTTP " + strconv.Itoa(res.StatusCode) + ")"}
		seenCode := false
		for _, field := range []string{"code", "type"} {
			seenCode = seenCode || detail[field] != nil
			if v, ok := detail[field].(string); ok && chatGPTDiagnostic.MatchString(v) {
				safe[field] = v
			}
		}
		if seenCode && len(safe) == 1 {
			safe["code"] = "unrecognized"
		}
		if v, ok := detail["resets_at"].(float64); ok && v > 0 && v < 253402300800 {
			safe["resets_at"] = v
		}
		writeJSON(w, res.StatusCode, map[string]any{"error": safe})
		return
	}
	// The real Codex endpoint can omit Content-Type on a valid SSE response.
	// Core still validates the event stream and requires response.completed;
	// an absent hint must not discard an already accepted model response.
	if contentType := strings.ToLower(res.Header.Get("Content-Type")); contentType != "" && !strings.HasPrefix(contentType, "text/event-stream") {
		chatGPTTransportError(w, 502, "transport_ambiguous")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	_ = rc.SetWriteDeadline(time.Now().Add(15 * time.Second))
	w.WriteHeader(200)
	defer rc.SetWriteDeadline(time.Time{})
	if rc.Flush() != nil {
		return
	}
	buf := make([]byte, 32<<10)
	remaining := int64(chatGPTStreamLimit)
	for {
		n, readErr := res.Body.Read(buf)
		if int64(n) > remaining {
			panic(http.ErrAbortHandler)
		}
		remaining -= int64(n)
		if n > 0 {
			_ = rc.SetWriteDeadline(time.Now().Add(15 * time.Second))
			if _, err := w.Write(buf[:n]); err != nil {
				return
			}
			if rc.Flush() != nil {
				return
			}
		}
		if readErr == io.EOF {
			return
		}
		if readErr != nil {
			panic(http.ErrAbortHandler)
		} // never fabricate response.completed
	}
}
