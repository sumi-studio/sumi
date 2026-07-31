package todo

import "context"

type AgentToolDefinition struct {
	Name        string
	Description string
	Parameters  map[string]any
}

func AgentToolDefinitions() []AgentToolDefinition {
	return []AgentToolDefinition{
		{Name: "list_todos", Description: "List the user's Todos", Parameters: objectSchema(nil, map[string]any{
			"status":  map[string]any{"type": "string", "enum": []string{"open", "done"}},
			"overdue": map[string]any{"type": "boolean"},
			"q":       map[string]any{"type": "string"},
		})},
		{Name: "get_todo", Description: "Get one of the user's Todos", Parameters: objectSchema([]string{"id"}, map[string]any{
			"id": map[string]any{"type": "string", "format": "uuid"},
		})},
		{Name: "create_todo", Description: "Create one Todo for the user", Parameters: objectSchema([]string{"title"}, map[string]any{
			"title":       map[string]any{"type": "string", "minLength": 1, "maxLength": 200},
			"description": map[string]any{"type": "string"},
			"priority":    map[string]any{"type": "string", "enum": []string{"none", "low", "medium", "high"}},
			"due":         agentDueSchema(),
		})},
		{Name: "update_todo", Description: "Update one Todo when its expected version still matches", Parameters: objectSchema([]string{"id", "expected_version", "patch"}, map[string]any{
			"id":               map[string]any{"type": "string", "format": "uuid"},
			"expected_version": map[string]any{"type": "integer", "minimum": 1},
			"patch": objectSchema(nil, map[string]any{
				"title":       map[string]any{"type": "string", "minLength": 1, "maxLength": 200},
				"description": map[string]any{"type": "string"},
				"status":      map[string]any{"type": "string", "enum": []string{"open", "done"}},
				"priority":    map[string]any{"type": "string", "enum": []string{"none", "low", "medium", "high"}},
				"due":         map[string]any{"anyOf": []any{agentDueSchema(), map[string]any{"type": "null"}}},
			}),
		})},
		{Name: "propose_delete", Description: "Ask the user to confirm deletion in the UI without deleting anything", Parameters: objectSchema([]string{"id"}, map[string]any{
			"id": map[string]any{"type": "string", "format": "uuid"},
		})},
	}
}

func objectSchema(required []string, properties map[string]any) map[string]any {
	schema := map[string]any{"type": "object", "additionalProperties": false, "properties": properties}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func agentDueSchema() map[string]any {
	timezone := map[string]any{"type": "string", "description": "IANA timezone; omitted values use the user setting or Asia/Tokyo"}
	return map[string]any{"oneOf": []any{
		objectSchema([]string{"kind", "date"}, map[string]any{
			"kind": map[string]any{"type": "string", "const": "date"}, "date": map[string]any{"type": "string", "format": "date"}, "timezone": timezone,
		}),
		objectSchema([]string{"kind", "at"}, map[string]any{
			"kind": map[string]any{"type": "string", "const": "datetime"}, "at": map[string]any{"type": "string", "format": "date-time"}, "timezone": timezone,
		}),
	}}
}

// AgentTools is the application boundary for a tool call after a user session
// has been verified. It intentionally has no Delete operation.
type AgentTools struct {
	service     *Service
	ownerUserID string
}

func NewAgentTools(service *Service, principal Principal) *AgentTools {
	return &AgentTools{service: service, ownerUserID: principal.UserID}
}

func (t *AgentTools) ListTodos(ctx context.Context, filter ListFilter) (ListResult, error) {
	return t.service.List(ctx, t.ownerUserID, filter)
}

func (t *AgentTools) GetTodo(ctx context.Context, id string) (Todo, error) {
	return t.service.Get(ctx, t.ownerUserID, id)
}

func (t *AgentTools) CreateTodo(ctx context.Context, input CreateInput) (Todo, error) {
	return t.service.Create(ctx, t.ownerUserID, input, true)
}

func (t *AgentTools) UpdateTodo(ctx context.Context, id string, input UpdateInput) (Todo, error) {
	return t.service.Update(ctx, t.ownerUserID, id, input, true)
}

type DeleteProposal struct {
	Todo    Todo   `json:"todo"`
	Message string `json:"message"`
}

func (t *AgentTools) ProposeDelete(ctx context.Context, id string) (DeleteProposal, error) {
	item, err := t.service.Get(ctx, t.ownerUserID, id)
	if err != nil {
		return DeleteProposal{}, err
	}
	return DeleteProposal{Todo: item, Message: "このTodoを削除しますか？ UIの削除ボタンで確定してください。"}, nil
}
