.PHONY: help up down logs clean test seed restart

help: ## Show this help message
	@echo "Silk ERP - Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

up: ## Start all services with Docker Compose
	docker-compose up -d

down: ## Stop all services
	docker-compose down

logs: ## Show logs from all services
	docker-compose logs -f

clean: ## Stop services and remove volumes
	docker-compose down -v
	rm -rf migrations/*.sql

test: ## Run test script
	@bash test.sh

seed: ## Seed database with initial data
	@echo "Seeding database..."
	@docker-compose exec -T postgres psql -U postgres -d silk_erp -c "INSERT INTO roles (role_id, name, permitted_operations, description) VALUES ('ROLE-SYSTEM-ADMIN', 'System/Admin Superuser', '[\"*\"]', 'System administration') ON CONFLICT (role_id) DO NOTHING;"
	@python3 seed_db.py

restart: ## Restart all services
	docker-compose restart

shell-auth: ## Open shell in auth service container
	docker-compose exec auth-service bash

shell-postgres: ## Open PostgreSQL shell
	docker-compose exec postgres psql -U postgres -d silk_erp

migrations: ## Run all migrations
	@echo "Running migrations..."
	@for f in migrations/*.sql; do \
		echo "Running $$f..."; \
		docker-compose exec -T postgres psql -U postgres -d silk_erp -f /docker-entrypoint-initdb.d/$$(basename $$f); \
	done
