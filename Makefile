# Entry points for common commands. See README.md for details.

.PHONY: setup dev build lint test format api-dev

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
