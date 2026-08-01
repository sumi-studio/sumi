package todo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	maxRequestBodyBytes = 1024 * 1024
	todoCSRFHeaderName  = "X-Sumi-CSRF"
	todoCSRFHeaderValue = "1"
)

type Principal struct{ UserID string }

type PrincipalVerifier interface {
	VerifyRequest(ctx context.Context, request *http.Request) (Principal, error)
}

type Handler struct {
	service    *Service
	principals PrincipalVerifier
}

func NewHandler(service *Service, principals PrincipalVerifier) *Handler {
	return &Handler{service: service, principals: principals}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/todos", privateResponse(h.create))
	mux.HandleFunc("GET /v1/todos", privateResponse(h.list))
	mux.HandleFunc("GET /v1/todos/{id}", privateResponse(h.get))
	mux.HandleFunc("PATCH /v1/todos/{id}", privateResponse(h.update))
	mux.HandleFunc("DELETE /v1/todos/{id}", privateResponse(h.delete))
}

func privateResponse(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Add("Vary", "Cookie")
		next(w, r)
	}
}

func (h *Handler) principal(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	if h.principals == nil {
		writeAPIError(w, http.StatusUnauthorized, "unauthenticated", "authentication required", 0)
		return Principal{}, false
	}
	principal, err := h.principals.VerifyRequest(r.Context(), r)
	if err != nil || !IsUUID(principal.UserID) {
		writeAPIError(w, http.StatusUnauthorized, "unauthenticated", "authentication required", 0)
		return Principal{}, false
	}
	return principal, true
}

type createRequest struct {
	Title       string          `json:"title"`
	Description string          `json:"description"`
	Status      Status          `json:"status"`
	Priority    Priority        `json:"priority"`
	Due         json.RawMessage `json:"due"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.principal(w, r)
	if !ok {
		return
	}
	if !allowMutation(w, r, true) {
		return
	}
	var request createRequest
	if err := readJSON(r, &request, "due"); err != nil {
		writeAPIError(w, http.StatusBadRequest, "validation_failed", err.Error(), 0)
		return
	}
	due, _, err := decodeDue(request.Due)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "validation_failed", err.Error(), 0)
		return
	}
	item, err := h.service.Create(r.Context(), principal.UserID, CreateInput{
		Title: request.Title, Description: request.Description, Status: request.Status,
		Priority: request.Priority, Due: due,
	}, false)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	w.Header().Set("Location", "/v1/todos/"+item.ID)
	writeJSON(w, http.StatusCreated, item)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.principal(w, r)
	if !ok {
		return
	}
	filter := ListFilter{Sort: r.URL.Query().Get("sort"), Query: r.URL.Query().Get("q")}
	if raw := r.URL.Query().Get("status"); raw != "" {
		status := Status(raw)
		filter.Status = &status
	}
	if raw := r.URL.Query().Get("overdue"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "validation_failed", "overdue must be true or false", 0)
			return
		}
		filter.Overdue = value
	}
	var err error
	filter.Limit, err = parseIntQuery(r, "limit", 50)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "validation_failed", "limit must be an integer", 0)
		return
	}
	filter.Offset, err = parseIntQuery(r, "offset", 0)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "validation_failed", "offset must be an integer", 0)
		return
	}
	result, err := h.service.List(r.Context(), principal.UserID, filter)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.principal(w, r)
	if !ok {
		return
	}
	item, err := h.service.Get(r.Context(), principal.UserID, r.PathValue("id"))
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

type updateRequest struct {
	ExpectedVersion int             `json:"expected_version"`
	Title           *string         `json:"title"`
	Description     *string         `json:"description"`
	Status          *Status         `json:"status"`
	Priority        *Priority       `json:"priority"`
	Due             json.RawMessage `json:"due"`
}

func (h *Handler) update(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.principal(w, r)
	if !ok {
		return
	}
	if !allowMutation(w, r, true) {
		return
	}
	var request updateRequest
	if err := readJSON(r, &request, "due"); err != nil {
		writeAPIError(w, http.StatusBadRequest, "validation_failed", err.Error(), 0)
		return
	}
	due, dueSet, err := decodeDue(request.Due)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "validation_failed", err.Error(), 0)
		return
	}
	item, err := h.service.Update(r.Context(), principal.UserID, r.PathValue("id"), UpdateInput{
		ExpectedVersion: request.ExpectedVersion, Title: request.Title, Description: request.Description,
		Status: request.Status, Priority: request.Priority, DueSet: dueSet, Due: due,
	}, false)
	if err != nil {
		h.writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	principal, ok := h.principal(w, r)
	if !ok {
		return
	}
	if !allowMutation(w, r, false) {
		return
	}
	expectedVersion, err := parseIntQuery(r, "expected_version", 0)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "validation_failed", "expected_version must be an integer", 0)
		return
	}
	if err := h.service.Delete(r.Context(), principal.UserID, r.PathValue("id"), expectedVersion); err != nil {
		h.writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func allowMutation(w http.ResponseWriter, r *http.Request, requireJSON bool) bool {
	if r.Header.Get(todoCSRFHeaderName) != todoCSRFHeaderValue {
		writeAPIError(w, http.StatusForbidden, "csrf_failed", "Todo mutation requires X-Sumi-CSRF: 1", 0)
		return false
	}
	switch r.Header.Get("Sec-Fetch-Site") {
	case "", "none", "same-origin":
	default:
		writeAPIError(w, http.StatusForbidden, "csrf_failed", "cross-site Todo mutation rejected", 0)
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" {
		parsed, err := url.Parse(origin)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || !strings.EqualFold(parsed.Host, r.Host) {
			writeAPIError(w, http.StatusForbidden, "csrf_failed", "cross-origin Todo mutation rejected", 0)
			return false
		}
	}
	if requireJSON {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeAPIError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json", 0)
			return false
		}
	}
	return true
}

func (h *Handler) writeServiceError(w http.ResponseWriter, err error) {
	var validation *ValidationError
	var conflict *VersionConflictError
	switch {
	case errors.As(err, &validation):
		writeAPIError(w, http.StatusBadRequest, "validation_failed", validation.Message, 0)
	case errors.Is(err, ErrNotFound):
		writeAPIError(w, http.StatusNotFound, "todo_not_found", "todo not found", 0)
	case errors.As(err, &conflict):
		writeAPIError(w, http.StatusConflict, "version_conflict", "todo was updated by another request", conflict.CurrentVersion)
	default:
		log.Printf("todo request failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", 0)
	}
}

func readJSON(r *http.Request, target any, nullableFields ...string) error {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBodyBytes+1))
	if err != nil {
		return fmt.Errorf("read JSON body: %w", err)
	}
	if len(raw) > maxRequestBodyBytes {
		return fmt.Errorf("JSON body exceeds %d bytes", maxRequestBodyBytes)
	}
	if !utf8.Valid(raw) {
		return fmt.Errorf("JSON body must be valid UTF-8")
	}
	if err := rejectDuplicateJSONFields(raw); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request body must contain one JSON object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	allowedFields := exactJSONFieldNames(target)
	nullable := make(map[string]struct{}, len(nullableFields))
	for _, field := range nullableFields {
		nullable[field] = struct{}{}
	}
	for name, value := range fields {
		if _, ok := allowedFields[name]; !ok {
			return fmt.Errorf("unknown field %q", name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			if _, ok := nullable[name]; !ok {
				return fmt.Errorf("%s must not be null", name)
			}
		}
	}
	return nil
}

func exactJSONFieldNames(target any) map[string]struct{} {
	typeOfTarget := reflect.TypeOf(target)
	for typeOfTarget.Kind() == reflect.Pointer {
		typeOfTarget = typeOfTarget.Elem()
	}
	fields := make(map[string]struct{}, typeOfTarget.NumField())
	for i := 0; i < typeOfTarget.NumField(); i++ {
		field := typeOfTarget.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "" {
			name = field.Name
		}
		if name != "-" {
			fields[name] = struct{}{}
		}
	}
	return fields
}

func rejectDuplicateJSONFields(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := consumeJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key must be a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate field %q", key)
			}
			seen[key] = struct{}{}
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return fmt.Errorf("unexpected delimiter %q", delimiter)
	}
}

func decodeDue(raw json.RawMessage) (*DueInput, bool, error) {
	if raw == nil {
		return nil, false, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, true, nil
	}
	var due DueInput
	if err := rejectUnknownJSONFields(raw, &due); err != nil {
		return nil, true, &ValidationError{Message: "invalid due value"}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&due); err != nil {
		return nil, true, &ValidationError{Message: "invalid due value"}
	}
	return &due, true, nil
}

func rejectUnknownJSONFields(raw []byte, target any) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	allowedFields := exactJSONFieldNames(target)
	for name := range fields {
		if _, ok := allowedFields[name]; !ok {
			return fmt.Errorf("unknown field %q", name)
		}
	}
	return nil
}

func parseIntQuery(r *http.Request, name string, defaultValue int) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return defaultValue, nil
	}
	return strconv.Atoi(raw)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

func writeAPIError(w http.ResponseWriter, status int, code, message string, currentVersion int) {
	type errorBody struct {
		Code           string `json:"code"`
		Message        string `json:"message"`
		CurrentVersion int    `json:"current_version,omitempty"`
	}
	writeJSON(w, status, struct {
		Error errorBody `json:"error"`
	}{Error: errorBody{Code: code, Message: message, CurrentVersion: currentVersion}})
}
