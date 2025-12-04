.PHONY: help up down restart logs logs-airflow logs-postgres status clean clean-all test validate init stop ps shell-airflow shell-postgres dbt-test dbt-run dbt-docs dbt-clean build

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

##@ General

help: ## Display this help message
	@echo "$(BLUE)Ebury Data Pipeline - Makefile Commands$(NC)"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2 } /^##@/ { printf "\n$(YELLOW)%s$(NC)\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Docker Services

up: ## Start all services (Airflow + PostgreSQL)
	@echo "$(BLUE)Starting all services...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)Services started successfully!$(NC)"
	@echo "$(YELLOW)Airflow UI: http://localhost:8081$(NC)"
	@echo "$(YELLOW)Default credentials: airflow/airflow$(NC)"

down: ## Stop all services
	@echo "$(BLUE)Stopping all services...$(NC)"
	docker-compose down
	@echo "$(GREEN)Services stopped successfully!$(NC)"

stop: down ## Alias for down

restart: ## Restart all services
	@echo "$(BLUE)Restarting all services...$(NC)"
	docker-compose restart
	@echo "$(GREEN)Services restarted successfully!$(NC)"

build: ## Build or rebuild services
	@echo "$(BLUE)Building services...$(NC)"
	docker-compose build
	@echo "$(GREEN)Build completed!$(NC)"

ps: ## Show status of all containers
	@echo "$(BLUE)Container status:$(NC)"
	@docker-compose ps

status: ps ## Alias for ps

##@ Logs & Monitoring

logs: ## Show logs from all services
	docker-compose logs -f

logs-airflow: ## Show Airflow logs
	docker-compose logs -f airflow-webserver airflow-scheduler

logs-postgres: ## Show PostgreSQL logs
	docker-compose logs -f postgres

logs-webserver: ## Show Airflow webserver logs
	docker-compose logs -f airflow-webserver

logs-scheduler: ## Show Airflow scheduler logs
	docker-compose logs -f airflow-scheduler

##@ Shell Access

shell-airflow: ## Open shell in Airflow container
	@echo "$(BLUE)Opening shell in Airflow container...$(NC)"
	docker-compose exec airflow-webserver bash

shell-postgres: ## Open PostgreSQL shell
	@echo "$(BLUE)Opening PostgreSQL shell...$(NC)"
	docker-compose exec postgres psql -U airflow -d ebury


##@ DBT Operations
dbt-deps: ## Install DBT package dependencies
	@echo "$(BLUE)Installing DBT package dependencies...$(NC)"
	docker-compose exec airflow-webserver bash -c "cd /opt/dbt && dbt deps --profiles-dir ."
	@echo "$(GREEN)DBT dependencies installed!$(NC)"

dbt-test: ## Run DBT tests
	@echo "$(BLUE)Running DBT tests...$(NC)"
	docker-compose exec airflow-webserver bash -c "cd /opt/dbt && dbt test --profiles-dir ."

dbt-run: ## Run DBT models
	@echo "$(BLUE)Running DBT models...$(NC)"
	docker-compose exec airflow-webserver bash -c "cd /opt/dbt && dbt run --profiles-dir ."

dbt-compile: ## Compile DBT models
	@echo "$(BLUE)Compiling DBT models...$(NC)"
	docker-compose exec airflow-webserver bash -c "cd /opt/dbt && dbt compile --profiles-dir ."

dbt-docs-generate: ## Generate DBT documentation
	@echo "$(BLUE)Generating DBT documentation...$(NC)"
	docker-compose exec airflow-webserver bash -c "cd /opt/dbt && dbt docs generate --profiles-dir ."

dbt-debug: ## Debug DBT connection
	@echo "$(BLUE)Debugging DBT connection...$(NC)"
	docker-compose exec airflow-webserver bash -c "cd /opt/dbt && dbt debug --profiles-dir ."

dbt-clean: ## Clean DBT artifacts
	@echo "$(BLUE)Cleaning DBT artifacts...$(NC)"
	docker-compose exec airflow-webserver bash -c "cd /opt/dbt && dbt clean"


