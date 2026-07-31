# Entry points for common commands. See README.md for details.

.PHONY: setup dev build lint test format api-dev compose-env compose-up compose-down compose-session compose-todo-smoke

setup: ## Install JS dependencies
	pnpm install

dev: ## Run all dev servers (web / agent / api via turbo)
	pnpm dev

build: ## Build all apps and packages
	pnpm build

lint: ## Lint all workspaces (Biome + go vet)
	pnpm lint

test: ## Run all tests
	pnpm test

format: ## Format the whole repo
	pnpm format

api-dev: ## Run the Go API server directly
	cd apps/api && go run ./cmd/server

compose-env: ## Generate an ignored local .env with random Compose secrets
	./scripts/dev/create-compose-env.sh

compose-up: ## Build and start PostgreSQL, migrations, API, and Web
	docker compose up --detach --build

compose-down: ## Stop the Compose stack without deleting persisted data
	docker compose down

compose-session: ## Print a one-day local sumi_session cookie
	./scripts/dev/create-sumi-session.sh

compose-todo-smoke: ## Exercise Todo CRUD and optimistic locking through the Web proxy
	./scripts/dev/todo-api-curl-example.sh
