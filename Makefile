.PHONY: container-up infra-up sync-env yearly-ingestion backfill destroy help

# Builds the initial GCP resources
infra-up:
	@echo "Deploying foundational infrastructure..."
	terraform -chdir=infra init
	terraform -chdir=infra apply -auto-approve

# Injects Terraform outputs into your local environment
sync-env:
	@echo "Extracting Terraform outputs to .env..."
	@for var in GCP_PROJECT_ID LAKEHOUSE_BUCKET; do \
		value=$$(terraform -chdir=infra output -raw ENV_$${var}); \
		if grep -q "^$${var}=" .env; then \
			sed -i '' "s|^$${var}=.*|$${var}=$${value}|" .env; \
		else \
			echo "$${var}=$${value}" >> .env; \
		fi; \
	done
	@echo "Environment synced successfully."

# Build Docker setup
container-up:
	@echo "Starting up Docker setup"
	docker compose up -d

# Prefect Flow: Yearly Ingestion (processes one full calendar year)
# Usage: make yearly-ingestion YEAR=2026
yearly-ingestion:
	@echo "Running yearly ingestion flow for year $${YEAR:-2024}..."
	docker compose exec -T flow-runner python -c "from flows.chicago_pipeline import yearly_flow; yearly_flow($${YEAR:-2024})"

# Prefect Flow: Daily Ingestion (WAP - last 24 hours, scheduled via Prefect)
# For manual testing:
daily-ingestion:
	@echo "Running daily ingestion flow (WAP - last 24 hours)..."
	docker compose exec -T flow-runner python -c "from flows.chicago_pipeline import daily_flow; daily_flow()"

# Prefect Flow: Backfill (manual reprocessing of specific date ranges)
# Usage: make backfill START=2026-03-01 END=2026-04-01
backfill:
	@echo "Running backfill flow from $${START} to $${END}..."
	docker compose exec -T flow-runner python -c "from flows.chicago_pipeline import backfill_flow; backfill_flow('$${START}', '$${END}')"

# Rebuild flow-runner container (needed when dependencies or Dockerfile change)
rebuild:
	@echo "Rebuilding flow-runner container..."
	docker compose build flow-runner
	docker compose up -d flow-runner

# Register Prefect deployments
deploy:
	@echo "Registering Prefect deployments..."
	docker compose exec -T flow-runner prefect deploy \
		flows/chicago_pipeline.py:daily_flow \
		--name daily-311 \
		--pool chicago-311-pool \
		--cron "0 0 * * *"

# Destroy existing Terraform infrastructure
destroy:
	@echo "Destroying Terraform infrastructure..."
	terraform -chdir=infra destroy -auto-approve

# Show help
help:
	@echo "Available targets:"
	@echo "  make infra-up        - Deploy GCP infrastructure"
	@echo "  make sync-env        - Sync Terraform outputs to .env"
	@echo "  make container-up    - Start Docker containers"
	@echo "  make yearly-ingestion YEAR=2026  - Ingest a full year"
	@echo "  make daily-ingestion - Run daily WAP flow (last 24h)"
	@echo "  make backfill START=2026-03-01 END=2026-04-01 - Backfill a date range"
	@echo "  make rebuild         - Rebuild flow-runner container"
	@echo "  make deploy          - Register Prefect deployments"
	@echo "  make destroy         - Destroy GCP infrastructure"
