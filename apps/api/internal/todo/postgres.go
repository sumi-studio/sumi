package todo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const todoColumns = `id, title, description, status, priority,
due_kind, due_on::text, due_at, due_timezone, version, via_agent,
completed_at, created_at, updated_at`

type PostgresRepository struct{ pool *pgxpool.Pool }

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{pool: pool}
}

func (r *PostgresRepository) Create(ctx context.Context, ownerUserID string, input CreateRecord) (Todo, error) {
	var dueKind, dueOn, dueTimezone any
	var dueAt any
	if input.Due != nil {
		dueKind = input.Due.Kind
		dueTimezone = input.Due.Timezone
		if input.Due.Kind == DueKindDate {
			dueOn = input.Due.Date
		} else {
			dueAt = input.Due.At
		}
	}
	completedAt := any(nil)
	if input.Status == StatusDone {
		completedAt = input.Now
	}
	row := r.pool.QueryRow(ctx, `
INSERT INTO todos (
  id, owner_user_id, title, description, status, priority,
  due_kind, due_on, due_at, due_timezone, via_agent, completed_at,
  created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)
RETURNING `+todoColumns,
		input.ID, ownerUserID, input.Title, input.Description, input.Status, input.Priority,
		dueKind, dueOn, dueAt, dueTimezone, input.ViaAgent, completedAt, input.Now)
	return scanTodo(row)
}

func (r *PostgresRepository) List(ctx context.Context, ownerUserID string, filter ListFilter) (ListResult, error) {
	where := []string{"owner_user_id = $1"}
	args := []any{ownerUserID}
	if filter.Status != nil {
		args = append(args, *filter.Status)
		where = append(where, fmt.Sprintf("status = $%d", len(args)))
	}
	if filter.Overdue {
		where = append(where, `status = 'open' AND (
  CASE due_kind
    WHEN 'datetime' THEN due_at
    WHEN 'date' THEN ((due_on + 1)::timestamp AT TIME ZONE due_timezone)
  END
) < now()`)
	}
	if filter.Query != "" {
		args = append(args, "%"+escapeLike(filter.Query)+"%")
		where = append(where, fmt.Sprintf(`title ILIKE $%d ESCAPE '\'`, len(args)))
	}

	whereSQL := strings.Join(where, " AND ")
	var total int
	if err := r.pool.QueryRow(ctx, "SELECT count(*) FROM todos WHERE "+whereSQL, args...).Scan(&total); err != nil {
		return ListResult{}, fmt.Errorf("count todos: %w", err)
	}

	orderSQL := "updated_at DESC, id DESC"
	if filter.Sort == "due" {
		orderSQL = `(
  CASE due_kind
    WHEN 'datetime' THEN due_at
    WHEN 'date' THEN ((due_on + 1)::timestamp AT TIME ZONE due_timezone)
  END
) ASC NULLS LAST, updated_at DESC, id DESC`
	}
	args = append(args, filter.Limit, filter.Offset)
	query := "SELECT " + todoColumns + " FROM todos WHERE " + whereSQL +
		" ORDER BY " + orderSQL + fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return ListResult{}, fmt.Errorf("list todos: %w", err)
	}
	defer rows.Close()
	items := make([]Todo, 0)
	for rows.Next() {
		item, err := scanTodo(rows)
		if err != nil {
			return ListResult{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return ListResult{}, fmt.Errorf("iterate todos: %w", err)
	}
	return ListResult{Items: items, Total: total}, nil
}

func (r *PostgresRepository) Get(ctx context.Context, ownerUserID, id string) (Todo, error) {
	row := r.pool.QueryRow(ctx, "SELECT "+todoColumns+" FROM todos WHERE id = $1 AND owner_user_id = $2", id, ownerUserID)
	item, err := scanTodo(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Todo{}, ErrNotFound
	}
	return item, err
}

func (r *PostgresRepository) Update(ctx context.Context, ownerUserID, id string, input UpdateRecord) (Todo, error) {
	sets := make([]string, 0, 10)
	args := make([]any, 0, 14)
	add := func(column string, value any) {
		args = append(args, value)
		sets = append(sets, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	if input.Title != nil {
		add("title", *input.Title)
	}
	if input.Description != nil {
		add("description", *input.Description)
	}
	if input.Status != nil {
		args = append(args, *input.Status)
		position := len(args)
		sets = append(sets,
			fmt.Sprintf("completed_at = CASE WHEN $%d = 'done' AND status <> 'done' THEN now() WHEN $%d = 'open' AND status <> 'open' THEN NULL ELSE completed_at END", position, position),
			fmt.Sprintf("status = $%d", position),
		)
	}
	if input.Priority != nil {
		add("priority", *input.Priority)
	}
	if input.DueSet {
		var kind, on, at, timezone any
		if input.Due != nil {
			kind = input.Due.Kind
			timezone = input.Due.Timezone
			if input.Due.Kind == DueKindDate {
				on = input.Due.Date
			} else {
				at = input.Due.At
			}
		}
		add("due_kind", kind)
		add("due_on", on)
		add("due_at", at)
		add("due_timezone", timezone)
	}
	add("via_agent", input.ViaAgent)
	sets = append(sets, "version = version + 1", "updated_at = now()")
	args = append(args, id, ownerUserID, input.ExpectedVersion)
	row := r.pool.QueryRow(ctx,
		"UPDATE todos SET "+strings.Join(sets, ", ")+fmt.Sprintf(
			" WHERE id = $%d AND owner_user_id = $%d AND version = $%d RETURNING ",
			len(args)-2, len(args)-1, len(args))+todoColumns,
		args...,
	)
	item, err := scanTodo(row)
	if err == nil {
		return item, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Todo{}, err
	}
	return Todo{}, r.classifyMiss(ctx, ownerUserID, id)
}

func (r *PostgresRepository) Delete(ctx context.Context, ownerUserID, id string, expectedVersion int) error {
	command, err := r.pool.Exec(ctx,
		"DELETE FROM todos WHERE id = $1 AND owner_user_id = $2 AND version = $3",
		id, ownerUserID, expectedVersion,
	)
	if err != nil {
		return fmt.Errorf("delete todo: %w", err)
	}
	if command.RowsAffected() == 1 {
		return nil
	}
	return r.classifyMiss(ctx, ownerUserID, id)
}

func (r *PostgresRepository) classifyMiss(ctx context.Context, ownerUserID, id string) error {
	var currentVersion int
	err := r.pool.QueryRow(ctx,
		"SELECT version FROM todos WHERE id = $1 AND owner_user_id = $2",
		id, ownerUserID,
	).Scan(&currentVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("classify optimistic lock miss: %w", err)
	}
	return &VersionConflictError{CurrentVersion: currentVersion}
}

type todoScanner interface{ Scan(dest ...any) error }

func scanTodo(row todoScanner) (Todo, error) {
	var item Todo
	var status, priority string
	var dueKind, dueOn, dueTimezone *string
	var dueAt *time.Time
	err := row.Scan(
		&item.ID, &item.Title, &item.Description, &status, &priority,
		&dueKind, &dueOn, &dueAt, &dueTimezone, &item.Version, &item.ViaAgent,
		&item.CompletedAt, &item.CreatedAt, &item.UpdatedAt,
	)
	if err != nil {
		return Todo{}, err
	}
	item.Status = Status(status)
	item.Priority = Priority(priority)
	if dueKind != nil {
		item.Due = &Due{Kind: DueKind(*dueKind), At: dueAt}
		if dueOn != nil {
			item.Due.Date = *dueOn
		}
		if dueTimezone != nil {
			item.Due.Timezone = *dueTimezone
		}
	}
	return item, nil
}

func escapeLike(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `%`, `\%`)
	return strings.ReplaceAll(value, `_`, `\_`)
}
