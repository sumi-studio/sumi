# Entry points for common commands. See README.md for details.

.PHONY: setup dev dev-check dev-workspaces build lint test test-todo-db format api-dev db-up db-down migrate

setup: ## Install JS dependencies
	pnpm install

dev: ## Start the supported authenticated local Sumi stack
	pnpm dev

dev-check: ## Validate real-stack credentials and identity configuration
	pnpm dev:check

dev-workspaces: ## Run raw workspace dev tasks without stack orchestration
	pnpm dev:workspaces

build: ## Build all apps and packages
	pnpm build

lint: ## Lint all workspaces (Biome + go vet)
	pnpm lint

test: ## Run all tests
	pnpm test

test-todo-db: ## Run Todo migrations and repository tests against local Postgres
	cd apps/api && SUMI_TEST_DB_URL='postgres://sumi:sumi-dev@127.0.0.1:5432/sumi?sslmode=disable' go test ./internal/todo -run TestPostgresTodoContract -count=1

format: ## Format the whole repo
	pnpm format

api-dev: ## Run only the Go API (requires its full environment contract)
	cd apps/api && go run ./cmd/server

db-up: ## Start the control-plane Postgres via compose
	docker compose -f deploy/local/compose.yaml up -d postgres

db-down: ## Stop the control-plane Postgres
	docker compose -f deploy/local/compose.yaml down

migrate: ## Apply control-plane schema migrations (requires SUMI_DB_URL)
	cd apps/api && go run ./cmd/migrate
