package todo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	ownerA = "019c0000-0000-7000-8000-000000000001"
	ownerB = "019c0000-0000-7000-8000-000000000002"
)

type memoryRepository struct {
	items map[string]map[string]Todo
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{items: make(map[string]map[string]Todo)}
}

func (r *memoryRepository) Create(_ context.Context, owner string, input CreateRecord) (Todo, error) {
	if r.items[owner] == nil {
		r.items[owner] = make(map[string]Todo)
	}
	item := Todo{
		ID: input.ID, Title: input.Title, Description: input.Description, Status: input.Status,
		Priority: input.Priority, Due: input.Due, Version: 1, ViaAgent: input.ViaAgent,
		CreatedAt: input.Now, UpdatedAt: input.Now,
	}
	if input.Status == StatusDone {
		completed := input.Now
		item.CompletedAt = &completed
	}
	r.items[owner][item.ID] = item
	return item, nil
}

func (r *memoryRepository) List(_ context.Context, owner string, filter ListFilter) (ListResult, error) {
	items := make([]Todo, 0)
	for _, item := range r.items[owner] {
		if filter.Status != nil && item.Status != *filter.Status {
			continue
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].UpdatedAt.After(items[j].UpdatedAt) })
	total := len(items)
	if filter.Offset >= len(items) {
		items = []Todo{}
	} else {
		items = items[filter.Offset:]
	}
	if len(items) > filter.Limit {
		items = items[:filter.Limit]
	}
	return ListResult{Items: items, Total: total}, nil
}

func (r *memoryRepository) Get(_ context.Context, owner, id string) (Todo, error) {
	item, ok := r.items[owner][id]
	if !ok {
		return Todo{}, ErrNotFound
	}
	return item, nil
}

func (r *memoryRepository) Update(_ context.Context, owner, id string, input UpdateRecord) (Todo, error) {
	item, ok := r.items[owner][id]
	if !ok {
		return Todo{}, ErrNotFound
	}
	if item.Version != input.ExpectedVersion {
		return Todo{}, &VersionConflictError{CurrentVersion: item.Version}
	}
	if input.Title != nil {
		item.Title = *input.Title
	}
	if input.Description != nil {
		item.Description = *input.Description
	}
	if input.Status != nil {
		if item.Status != *input.Status && *input.Status == StatusDone {
			now := item.UpdatedAt.Add(time.Second)
			item.CompletedAt = &now
		}
		if item.Status != *input.Status && *input.Status == StatusOpen {
			item.CompletedAt = nil
		}
		item.Status = *input.Status
	}
	if input.Priority != nil {
		item.Priority = *input.Priority
	}
	if input.DueSet {
		item.Due = input.Due
	}
	item.Version++
	item.ViaAgent = input.ViaAgent
	item.UpdatedAt = item.UpdatedAt.Add(time.Second)
	r.items[owner][id] = item
	return item, nil
}

func (r *memoryRepository) Delete(_ context.Context, owner, id string, expectedVersion int) error {
	item, ok := r.items[owner][id]
	if !ok {
		return ErrNotFound
	}
	if item.Version != expectedVersion {
		return &VersionConflictError{CurrentVersion: item.Version}
	}
	delete(r.items[owner], id)
	return nil
}

type fixedPrincipal struct{ userID string }

func (p fixedPrincipal) VerifyRequest(context.Context, *http.Request) (Principal, error) {
	return Principal{UserID: p.userID}, nil
}

func newTestService(t *testing.T, repository Repository) *Service {
	t.Helper()
	service, err := NewService(repository, "Asia/Tokyo")
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC) }
	return service
}

func TestOtherOwnersTodoReturns404(t *testing.T) {
	repository := newMemoryRepository()
	service := newTestService(t, repository)
	item, err := service.Create(context.Background(), ownerA, CreateInput{Title: "private"}, false)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	NewHandler(service, fixedPrincipal{userID: ownerB}).Register(mux)
	request := httptest.NewRequest(http.MethodGet, "/v1/todos/"+item.ID, nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("got status %d, want 404", response.Code)
	}
	if bytes.Contains(response.Body.Bytes(), []byte("private")) {
		t.Fatal("cross-owner response leaked Todo content")
	}
}

func TestStaleExpectedVersionReturns409WithoutMutation(t *testing.T) {
	repository := newMemoryRepository()
	service := newTestService(t, repository)
	item, err := service.Create(context.Background(), ownerA, CreateInput{Title: "original"}, false)
	if err != nil {
		t.Fatal(err)
	}
	firstTitle := "first update"
	if _, err := service.Update(context.Background(), ownerA, item.ID, UpdateInput{
		ExpectedVersion: 1, Title: &firstTitle,
	}, false); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	NewHandler(service, fixedPrincipal{userID: ownerA}).Register(mux)
	body := []byte(`{"expected_version":1,"title":"stale update"}`)
	request := httptest.NewRequest(http.MethodPatch, "/v1/todos/"+item.ID, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(todoCSRFHeaderName, todoCSRFHeaderValue)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("got status %d, want 409: %s", response.Code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code           string `json:"code"`
			CurrentVersion int    `json:"current_version"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != "version_conflict" || envelope.Error.CurrentVersion != 2 {
		t.Fatalf("unexpected error: %+v", envelope.Error)
	}
	current, _ := service.Get(context.Background(), ownerA, item.ID)
	if current.Title != firstTitle || current.Version != 2 {
		t.Fatalf("stale update mutated Todo: %+v", current)
	}
}

func TestDateDeadlineIsNextMidnightInTimezone(t *testing.T) {
	tests := []struct {
		name     string
		timezone string
		want     time.Time
	}{
		{name: "Tokyo", timezone: "Asia/Tokyo", want: time.Date(2026, 8, 1, 15, 0, 0, 0, time.UTC)},
		{name: "UTC", timezone: "UTC", want: time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deadline, err := Deadline(&Due{Kind: DueKindDate, Date: "2026-08-01", Timezone: test.timezone})
			if err != nil {
				t.Fatal(err)
			}
			if !deadline.Equal(test.want) {
				t.Fatalf("got %s, want %s", deadline, test.want)
			}
		})
	}
}

func TestReplacingDatetimeDueWithDateClearsAt(t *testing.T) {
	repository := newMemoryRepository()
	service := newTestService(t, repository)
	item, err := service.Create(context.Background(), ownerA, CreateInput{
		Title: "due", Due: &DueInput{Kind: DueKindDatetime, At: "2026-08-01T15:00:00+09:00", Timezone: "Asia/Tokyo"},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if item.Due == nil || item.Due.At == nil {
		t.Fatal("expected datetime due")
	}
	replacement := &DueInput{Kind: DueKindDate, Date: "2026-08-02", Timezone: "Asia/Tokyo"}
	updated, err := service.Update(context.Background(), ownerA, item.ID, UpdateInput{
		ExpectedVersion: 1, DueSet: true, Due: replacement,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Due == nil || updated.Due.Kind != DueKindDate || updated.Due.At != nil {
		t.Fatalf("datetime storage survived whole-value replacement: %+v", updated.Due)
	}
}

func TestAgentToolsDoNotExposeDelete(t *testing.T) {
	definitions := AgentToolDefinitions()
	for _, definition := range definitions {
		if definition.Name == "delete_todo" {
			t.Fatal("delete_todo must not be exposed")
		}
	}
	method, exists := reflect.TypeOf((*AgentTools)(nil)).MethodByName("DeleteTodo")
	if exists {
		t.Fatalf("AgentTools unexpectedly exposes %s", method.Name)
	}
	if len(definitions) != 5 || definitions[4].Name != "propose_delete" {
		t.Fatalf("unexpected tool surface: %+v", definitions)
	}
	updatePatch := definitions[3].Parameters["properties"].(map[string]any)["patch"].(map[string]any)
	if updatePatch["minProperties"] != 1 {
		t.Fatalf("update_todo patch allows an empty mutation: %+v", updatePatch)
	}
}

func TestTitleQueryEscapesLikeWildcards(t *testing.T) {
	if got, want := escapeLike(`100%_done\later`), `100\%\_done\\later`; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestAgentVersionConflictIsNotRetried(t *testing.T) {
	repository := newMemoryRepository()
	service := newTestService(t, repository)
	tools := NewAgentTools(service, Principal{UserID: ownerA})
	item, err := tools.CreateTodo(context.Background(), CreateInput{Title: "agent"})
	if err != nil {
		t.Fatal(err)
	}
	title := "human"
	if _, err := service.Update(context.Background(), ownerA, item.ID, UpdateInput{ExpectedVersion: 1, Title: &title}, false); err != nil {
		t.Fatal(err)
	}
	agentTitle := "agent stale"
	_, err = tools.UpdateTodo(context.Background(), item.ID, UpdateInput{ExpectedVersion: 1, Title: &agentTitle})
	var conflict *VersionConflictError
	if !errors.As(err, &conflict) || conflict.CurrentVersion != 2 {
		t.Fatalf("expected current version 2 conflict, got %v", err)
	}
	current, _ := tools.GetTodo(context.Background(), item.ID)
	if current.Title != title || current.ViaAgent {
		t.Fatalf("agent conflict overwrote current Todo: %+v", current)
	}
}

func TestHumanHTTPDoesNotTrustAgentMarker(t *testing.T) {
	repository := newMemoryRepository()
	service := newTestService(t, repository)
	mux := http.NewServeMux()
	NewHandler(service, fixedPrincipal{userID: ownerA}).Register(mux)
	request := httptest.NewRequest(http.MethodPost, "/v1/todos", bytes.NewReader([]byte(`{"title":"agent HTTP"}`)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(todoCSRFHeaderName, todoCSRFHeaderValue)
	request.Header.Set("X-Sumi-Via-Agent", "true")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("got status %d: %s", response.Code, response.Body.String())
	}
	var item Todo
	if err := json.Unmarshal(response.Body.Bytes(), &item); err != nil {
		t.Fatal(err)
	}
	if item.ViaAgent {
		t.Fatal("untrusted HTTP header set via_agent")
	}
	if _, ok := repository.items[ownerA][item.ID]; !ok {
		t.Fatal("Todo was not scoped to the authenticated owner")
	}
	if _, ok := repository.items[ownerB][item.ID]; ok {
		t.Fatal("agent marker changed owner scope")
	}
}

func TestTodoMutationsRequireCSRFHeader(t *testing.T) {
	service := newTestService(t, newMemoryRepository())
	mux := http.NewServeMux()
	NewHandler(service, fixedPrincipal{userID: ownerA}).Register(mux)
	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "create", method: http.MethodPost, path: "/v1/todos", body: `{"title":"blocked"}`},
		{name: "update", method: http.MethodPatch, path: "/v1/todos/019c0000-0000-7000-8000-000000000010", body: `{"expected_version":1,"title":"blocked"}`},
		{name: "delete", method: http.MethodDelete, path: "/v1/todos/019c0000-0000-7000-8000-000000000010?expected_version=1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			if test.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("got status %d, want 403: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestTodoMutationRejectsCrossOrigin(t *testing.T) {
	service := newTestService(t, newMemoryRepository())
	mux := http.NewServeMux()
	NewHandler(service, fixedPrincipal{userID: ownerA}).Register(mux)
	request := httptest.NewRequest(http.MethodPost, "http://api.example.test/v1/todos", strings.NewReader(`{"title":"blocked"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(todoCSRFHeaderName, todoCSRFHeaderValue)
	request.Header.Set("Origin", "https://attacker.example")
	request.Header.Set("Sec-Fetch-Site", "cross-site")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("got status %d, want 403: %s", response.Code, response.Body.String())
	}
}

func TestTodoMutationRequiresJSONContentType(t *testing.T) {
	service := newTestService(t, newMemoryRepository())
	mux := http.NewServeMux()
	NewHandler(service, fixedPrincipal{userID: ownerA}).Register(mux)
	request := httptest.NewRequest(http.MethodPost, "/v1/todos", strings.NewReader(`{"title":"blocked"}`))
	request.Header.Set(todoCSRFHeaderName, todoCSRFHeaderValue)
	request.Header.Set("Content-Type", "text/plain")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("got status %d, want 415: %s", response.Code, response.Body.String())
	}
}

func TestTodoJSONContractRejectsInvalidBodies(t *testing.T) {
	service := newTestService(t, newMemoryRepository())
	mux := http.NewServeMux()
	NewHandler(service, fixedPrincipal{userID: ownerA}).Register(mux)
	tests := []struct {
		name string
		path string
		body []byte
	}{
		{name: "null description", path: "/v1/todos", body: []byte(`{"title":"x","description":null}`)},
		{name: "null status", path: "/v1/todos", body: []byte(`{"title":"x","status":null}`)},
		{name: "null priority", path: "/v1/todos", body: []byte(`{"title":"x","priority":null}`)},
		{name: "null patch field", path: "/v1/todos/019c0000-0000-7000-8000-000000000010", body: []byte(`{"expected_version":1,"title":null,"priority":"high"}`)},
		{name: "duplicate top-level field", path: "/v1/todos", body: []byte(`{"title":"first","title":"second"}`)},
		{name: "case-variant field", path: "/v1/todos", body: []byte(`{"title":"first","Title":"second"}`)},
		{name: "duplicate nested field", path: "/v1/todos", body: []byte(`{"title":"x","due":{"kind":"date","date":"2026-08-01","date":"2026-08-02"}}`)},
		{name: "case-variant nested field", path: "/v1/todos", body: []byte(`{"title":"x","due":{"Kind":"date","Date":"2026-08-01"}}`)},
		{name: "invalid UTF-8", path: "/v1/todos", body: []byte{'{', '"', 't', 'i', 't', 'l', 'e', '"', ':', '"', 0xff, '"', '}'}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			method := http.MethodPost
			if strings.Contains(test.path, "/019c") {
				method = http.MethodPatch
			}
			request := httptest.NewRequest(method, test.path, bytes.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set(todoCSRFHeaderName, todoCSRFHeaderValue)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("got status %d, want 400: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestTodoResponsesAreNotCacheable(t *testing.T) {
	service := newTestService(t, newMemoryRepository())
	mux := http.NewServeMux()
	NewHandler(service, fixedPrincipal{userID: ownerA}).Register(mux)
	request := httptest.NewRequest(http.MethodGet, "/v1/todos", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("got status %d: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Cache-Control"); got != "private, no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if got := response.Header().Get("Vary"); got != "Cookie" {
		t.Fatalf("Vary = %q", got)
	}
}
