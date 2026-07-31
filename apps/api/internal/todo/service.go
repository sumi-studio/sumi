package todo

import (
	"context"
	"fmt"
	"time"
)

type CreateRecord struct {
	ID          string
	Title       string
	Description string
	Status      Status
	Priority    Priority
	Due         *Due
	ViaAgent    bool
	Now         time.Time
}

type UpdateRecord struct {
	ExpectedVersion int
	Title           *string
	Description     *string
	Status          *Status
	Priority        *Priority
	DueSet          bool
	Due             *Due
	ViaAgent        bool
}

type Repository interface {
	Create(ctx context.Context, ownerUserID string, input CreateRecord) (Todo, error)
	List(ctx context.Context, ownerUserID string, filter ListFilter) (ListResult, error)
	Get(ctx context.Context, ownerUserID, id string) (Todo, error)
	Update(ctx context.Context, ownerUserID, id string, input UpdateRecord) (Todo, error)
	Delete(ctx context.Context, ownerUserID, id string, expectedVersion int) error
}

type Service struct {
	repository      Repository
	defaultTimezone string
	now             func() time.Time
}

func NewService(repository Repository, defaultTimezone string) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("todo repository is required")
	}
	if defaultTimezone == "" {
		defaultTimezone = "Asia/Tokyo"
	}
	if _, err := normalizeDue(&DueInput{Kind: DueKindDate, Date: "2000-01-01", Timezone: defaultTimezone}, defaultTimezone); err != nil {
		return nil, fmt.Errorf("default timezone: %w", err)
	}
	return &Service{repository: repository, defaultTimezone: defaultTimezone, now: time.Now}, nil
}

func (s *Service) Create(ctx context.Context, ownerUserID string, input CreateInput, viaAgent bool) (Todo, error) {
	if !IsUUID(ownerUserID) {
		return Todo{}, &ValidationError{Message: "authenticated user_id must be a UUID"}
	}
	if err := validateTitle(input.Title); err != nil {
		return Todo{}, err
	}
	if err := validateDescription(input.Description); err != nil {
		return Todo{}, err
	}
	if input.Status == "" {
		input.Status = StatusOpen
	}
	if err := validateStatus(input.Status); err != nil {
		return Todo{}, err
	}
	if input.Priority == "" {
		input.Priority = PriorityNone
	}
	if err := validatePriority(input.Priority); err != nil {
		return Todo{}, err
	}
	due, err := normalizeDue(input.Due, s.defaultTimezone)
	if err != nil {
		return Todo{}, err
	}
	now := s.now().UTC()
	id, err := newUUIDv7(now)
	if err != nil {
		return Todo{}, err
	}
	return s.repository.Create(ctx, ownerUserID, CreateRecord{
		ID: id, Title: input.Title, Description: input.Description, Status: input.Status,
		Priority: input.Priority, Due: due, ViaAgent: viaAgent, Now: now,
	})
}

func (s *Service) List(ctx context.Context, ownerUserID string, filter ListFilter) (ListResult, error) {
	if !IsUUID(ownerUserID) {
		return ListResult{}, &ValidationError{Message: "authenticated user_id must be a UUID"}
	}
	if filter.Status != nil {
		if err := validateStatus(*filter.Status); err != nil {
			return ListResult{}, err
		}
	}
	if filter.Sort == "" {
		filter.Sort = "updated_at"
	}
	if filter.Sort != "updated_at" && filter.Sort != "due" {
		return ListResult{}, &ValidationError{Message: "sort must be updated_at or due"}
	}
	if filter.Limit == 0 {
		filter.Limit = 50
	}
	if filter.Limit < 1 || filter.Limit > 100 || filter.Offset < 0 {
		return ListResult{}, &ValidationError{Message: "limit or offset is out of range"}
	}
	return s.repository.List(ctx, ownerUserID, filter)
}

func (s *Service) Get(ctx context.Context, ownerUserID, id string) (Todo, error) {
	if !IsUUID(ownerUserID) || !IsUUID(id) {
		return Todo{}, ErrNotFound
	}
	return s.repository.Get(ctx, ownerUserID, id)
}

func (s *Service) Update(ctx context.Context, ownerUserID, id string, input UpdateInput, viaAgent bool) (Todo, error) {
	if !IsUUID(ownerUserID) || !IsUUID(id) {
		return Todo{}, ErrNotFound
	}
	if input.ExpectedVersion < 1 {
		return Todo{}, &ValidationError{Message: "expected_version must be at least 1"}
	}
	if !input.HasChanges() {
		return Todo{}, &ValidationError{Message: "at least one Todo field must be changed"}
	}
	if input.Title != nil {
		if err := validateTitle(*input.Title); err != nil {
			return Todo{}, err
		}
	}
	if input.Description != nil {
		if err := validateDescription(*input.Description); err != nil {
			return Todo{}, err
		}
	}
	if input.Status != nil {
		if err := validateStatus(*input.Status); err != nil {
			return Todo{}, err
		}
	}
	if input.Priority != nil {
		if err := validatePriority(*input.Priority); err != nil {
			return Todo{}, err
		}
	}
	var due *Due
	var err error
	if input.DueSet {
		due, err = normalizeDue(input.Due, s.defaultTimezone)
		if err != nil {
			return Todo{}, err
		}
	}
	return s.repository.Update(ctx, ownerUserID, id, UpdateRecord{
		ExpectedVersion: input.ExpectedVersion, Title: input.Title, Description: input.Description,
		Status: input.Status, Priority: input.Priority, DueSet: input.DueSet, Due: due, ViaAgent: viaAgent,
	})
}

func (s *Service) Delete(ctx context.Context, ownerUserID, id string, expectedVersion int) error {
	if !IsUUID(ownerUserID) || !IsUUID(id) {
		return ErrNotFound
	}
	if expectedVersion < 1 {
		return &ValidationError{Message: "expected_version must be at least 1"}
	}
	return s.repository.Delete(ctx, ownerUserID, id, expectedVersion)
}
