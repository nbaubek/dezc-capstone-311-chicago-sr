.PHONY: infra-up sync-env container-up wait-for-prefect setup-prefect yearly-ingestion backfill rebuild destroy lint test help

# ── Infrastructure ────────────────────────────────────────────────────────────

# Deploy GCP foundation (BigQuery dataset, GCS bucket, service accounts)
infra-up:
	@echo "Deploying GCP infrastructure..."
	terraform -chdir=infra init
	terraform -chdir=infra apply -auto-approve

# Inject Terraform outputs into .env (GCP_PROJECT_ID, LAKEHOUSE_BUCKET)
sync-env:
	@echo "Extracting Terraform outputs to .env..."
	@for var in GCP_PROJECT_ID LAKEHOUSE_BUCKET; do \
		value=$$(terraform -chdir=infra output -raw ENV_$${var}); \
		if grep -q "^$${var}=" .env; then \
			sed -i '' "s|^$${var}=.*|$${var}=$${value}|\" .env; \
		else \
			echo "$${var}=$${value}" >> .env; \
		fi; \
	done
	@echo "Environment synced."

# ── Containers & Prefect ──────────────────────────────────────────────────────

# Start Docker containers (Prefect server, worker, Postgres, Redis)
container-up:
	@echo "Starting Docker containers..."
	docker compose up -d

# Poll Prefect server until it is healthy (max 60s)
wait-for-prefect:
	@echo "Waiting for Prefect server to be ready..."
	@for i in $$(seq 1 60); do \
		if docker compose exec -T prefect-server curl -sf http://localhost:4200/api/health > /dev/null 2>&1; then \
			echo "Prefect server is ready."; \
			exit 0; \
		fi; \
		echo "  Attempt $$i/60 - still starting..."; \
		sleep 1; \
	done
	@echo "Prefect server did not become healthy in 60s. Check containers."

# Create Prefect work pool and register all three flow deployments.
# daily-311: cron-scheduled (midnight daily).
# yearly-311 + backfill-311: on-demand (manual trigger only).
# Prerequisites: container-up (Prefect server + worker must be running)
setup-prefect: wait-for-prefect
	@echo "Setting up Prefect work pool and deployments..."
	docker compose exec -T flow-runner prefect work-pool create chicago-311-pool --type process || true
	docker compose exec -T flow-runner prefect deploy \
		flows/chicago_pipeline.py:daily_flow \
		--name daily-311 \
		--pool chicago-311-pool \
		--cron "0 0 * * *"
	docker compose exec -T flow-runner prefect deploy \
		flows/chicago_pipeline.py:yearly_flow \
		--name yearly-311 \
		--pool chicago-311-pool
	docker compose exec -T flow-runner prefect deploy \
		flows/chicago_pipeline.py:backfill_flow \
		--name backfill-311 \
		--pool chicago-311-pool
	@echo "Prefect setup complete. 3 deployments registered (1 scheduled, 2 on-demand)."

# ── Ingestion Flows ──────────────────────────────────────────────────────────
# All ingestion targets require setup-prefect to have been run first.

# Full-year ingestion (one calendar year, 12 monthly chunks)
# Usage: make yearly-ingestion YEAR=2024
yearly-ingestion:
	@echo "Running yearly ingestion for $${YEAR:-2024}..."
	docker compose exec -T flow-runner python -c "from flows.chicago_pipeline import yearly_flow; yearly_flow($${YEAR:-2024})"

# Incremental daily ingestion via WAP pattern (last 24h of modified records)
daily-ingestion:
	@echo "Running daily WAP ingestion..."
	docker compose exec -T flow-runner python -c "from flows.chicago_pipeline import daily_flow; daily_flow()"

# Backfill a specific date range (useful for corrections or gap-filling)
# Usage: make backfill START=2026-03-01 END=2026-04-01
backfill:
	@echo "Running backfill from $${START} to $${END}..."
	docker compose exec -T flow-runner python -c "from flows.chicago_pipeline import backfill_flow; backfill_flow('$${START}', '$${END}')"

# ── Utilities ─────────────────────────────────────────────────────────────────

# Rebuild flow-runner container (run after dependency or Dockerfile changes)
rebuild:
	@echo "Rebuilding flow-runner container..."
	docker compose build flow-runner
	docker compose up -d flow-runner

# Tear down GCP infrastructure
destroy:
	@echo "Destroying GCP infrastructure..."
	terraform -chdir=infra destroy -auto-approve

# ── Code Quality ─────────────────────────────────────────────────────────────

lint:
	@echo "Running ruff linter..."
	uv run ruff check flows/

lint-fix:
	@echo "Running ruff auto-fix..."
	uv run ruff check flows/ --fix

typecheck:
	@echo "Running mypy type checker..."
	uv run mypy flows/ --no-error-summary

test:
	@echo "Running pytest..."
	uv run pytest flows/tests/ -v

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "Setup (run in order):"
	@echo "  make infra-up        - Deploy GCP infrastructure (Terraform)"
	@echo "  make sync-env        - Sync Terraform outputs to .env"
	@echo "  make container-up     - Start Docker containers"
	@echo "  make setup-prefect   - Create work pool + register deployments"
	@echo ""
	@echo "Ingestion (setup-prefect must run first):"
	@echo "  make yearly-ingestion YEAR=2024  - Ingest one full year"
	@echo "  make daily-ingestion             - Run daily WAP flow (last 24h)"
	@echo "  make backfill START=... END=...   - Backfill a date range"
	@echo ""
	@echo "Code quality:"
	@echo "  make lint        - Run ruff linter on Python code (flows/)"
	@echo "  make lint-fix    - Run ruff with --fix (auto-fix safe issues)"
	@echo "  make typecheck   - Run mypy type checker on Python code"
	@echo "  make test        - Run pytest on flows/tests/"
	@echo ""
	@echo "Utilities:"
	@echo "  make rebuild   - Rebuild flow-runner container"
	@echo "  make destroy   - Tear down GCP infrastructure"