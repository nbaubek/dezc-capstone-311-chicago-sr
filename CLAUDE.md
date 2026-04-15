
## Security Measures for Git Commits

**CRITICAL**: Before committing or pushing to any remote repository, verify that no credentials or sensitive data are included.

### Files/Directories That MUST Be Ignored

Ensure these are in `.gitignore`:

| Path | Reason |
|------|--------|
| `.env` | Contains GCP project ID, API tokens, database credentials |
| `.venv/`, `venv/` | Virtual environment with installed packages and potential cached credentials |
| `application_default_credentials.json` | GCP service account credentials |
| `*.key`, `*.json` (credential files) | Service account keys, API keys |
| `infra/.terraform/` | Terraform state and provider binaries (may contain sensitive info) |
| `infra/terraform.tfstate` | Terraform state file (contains resource IDs and secrets) |
| `infra/terraform.tfstate.backup` | Backup of Terraform state |
| `infra/.terraform.lock.hcl` | Contains provider versions (can be committed, but review first) |
| `transform/target/` | dbt compiled code and build artifacts |
| `transform/dbt_packages/` | Installed dbt packages (can be regenerated) |
| `transform/logs/` | dbt logs (may contain connection strings) |
| `transform/profiles.yml` | If it contains actual credentials (use template instead) |
| `.mcp.json` | MCP server configurations (may contain API keys) |
| `.claude/` | Claude Code workspace settings |
| `.vscode/` | IDE settings (local workspace config) |

### Terraform Security

1. **Never commit `terraform.tfstate` files** - they contain sensitive resource IDs and potentially secrets
2. **Use Terraform remote state** (GCS, Terraform Cloud, etc.) instead of local state
3. **Don't hardcode secrets in Terraform** - use environment variables or secret managers
4. **Use `tfsec` or `checkov`** to scan for security issues before committing
5. **Review `.terraform.lock.hcl`** before committing - it's usually safe, but verify no unexpected providers

### GCP Credentials

- **Never commit service account keys** (`.json` files)
- **Use Application Default Credentials** (`gcloud auth application-default login`) for development
- **For production**, use Workload Identity or Secret Manager
- **Rotate credentials immediately if accidentally committed**
- **Use `git-secrets` or similar tools** to scan for leaked credentials

### Environment Variables

- **Never commit `.env` files** - they contain all sensitive configuration
- **Always use `.env.example` as a template** with placeholder values
- **Document required environment variables** in README.md
- **Use `.env.local` for local overrides** (also gitignored)

### Pre-Commit Checklist

Before committing, run these checks:

```bash
# 1. Check git status for any untracked sensitive files
git status

# 2. Check .gitignore is working (should show no credential files)
git check-ignore -v .env .venv/ application_default_credentials.json infra/terraform.tfstate

# 3. Search for common credential patterns in staged files
git diff --cached | grep -i "password\|secret\|token\|key\|credential"

# 4. Use git-secrets if installed
git secrets --scan

# 5. Check Terraform state
git ls-files | grep -E "tfstate|\.terraform/"
```

### If You Accidentally Commit Credentials

1. **Immediately rotate the compromised credentials**
2. **Remove from git history** (not just `git rm`):
   ```bash
   # For single file
   git filter-branch --force --index-filter \
     "git rm --cached --ignore-unmatch PATH_TO_FILE" HEAD
   git push origin --force
   ```
3. **Force push to remote**
4. **Notify all team members to rotate their credentials**
5. **Consider repository access auditing**

### .gitignore Best Practices

Keep your `.gitignore` comprehensive and organized:

```gitignore
# Environment and credentials
.env
.env.local
.env.*.local
application_default_credentials.json
*.key
*.pem

# Virtual environments
.venv/
venv/
env/
ENV/

# Terraform
.terraform/
*.tfstate
*.tfstate.*
.terraform.lock.hcl

# dbt
transform/target/
transform/dbt_packages/
transform/logs/
transform/profiles.yml

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# MCP and AI tools
.mcp.json
.claude/
```

### Branching and Commit Practices

1. **Never commit directly to `main`** - always use feature branches
2. **Always ask for confirmation before committing and pushing** - show what will be committed and pushed
3. **Review the diff** before committing:

   ```bash
   git diff --staged
   ```
4. **Use commit message prefixes** for clarity:
   - `[infra]` - Infrastructure changes
   - `[feat]` - New features
   - `[fix]` - Bug fixes
   - `[docs]` - Documentation updates
   - `[chore]` - Maintenance tasks

### Recommended Pre-Commit Hook

Create `.git/hooks/pre-commit` (make executable):

```bash
#!/bin/bash
echo "🔍 Checking for sensitive data in staged files..."

# Check for common credential patterns
if git diff --cached --name-only | xargs grep -lE "password|secret|token|api_key|credential|private_key"; then
    echo "❌ ABORT: Found potential credentials in staged files!"
    echo "Please remove sensitive data before committing."
    exit 1
fi

# Check for terraform state
if git diff --cached --name-only | grep -E "\.tfstate$"; then
    echo "❌ ABORT: Terraform state file detected in staged files!"
    echo "Never commit .tfstate files. Use remote state instead."
    exit 1
fi

# Check for .env files
if git diff --cached --name-only | grep -E "\.env$"; then
    echo "❌ ABORT: .env file detected in staged files!"
    echo "Never commit .env files. Use .env.example as template."
    exit 1
fi

echo "✅ Pre-commit checks passed!"
```

---

## How to work with dbt and data models

A template for data engineering projects using **dbt**, structured for AI-assisted development with Claude Code. Conventions and guardrails are version-controlled so every team member (human or AI) follows the same rules.

### Example dbt Project structure

```
my_dbt_project/
├── dbt_project.yml          # Core project configuration
├── packages.yml             # External packages (dbt-utils, dbt-expectations)
├── selectors.yml            # (Optional) Node selection definitions
├── macros/                  # Custom SQL logic (e.g., generate_schema_name)
│   └── cents_to_dollars.sql
├── seeds/                   # Static CSVs (country codes, mapping tables)
│   └── country_codes.csv
├── analyses/                # SQL that doesn't become a model (ad-hoc)
├── snapshots/               # SCD Type 2 logic
├── tests/                   # Singular tests (generic tests go in YAMLs)
└── models/                  # THE CORE FOLDERS
    ├── staging/             # Layer 1: Clean, cast, and rename
    │   ├── _sources.yml     # Source definitions & Freshness
    │   ├── _stg_models.yml  # Schema tests & Documentation
    │   ├── stg_source_a__orders.sql
    │   └── stg_source_a__customers.sql
    ├── intermediate/        # Layer 2: Complex joins & business logic
    │   ├── _int_models.yml
    │   └── int_orders_joined_to_payments.sql
    └── marts/               # Layer 3: Kimball Stars (Fact/Dim)
        ├── _marts_models.yml
        ├── core/            # Common dimensions/facts
        │   ├── dim_date.sql
        │   ├── dim_customers.sql
        │   └── fct_orders.sql
        └── marketing/       # Department-specific marts
            └── mart_campaign_performance.sql
```



**Configure credentials**

Edit `.env` with your connection details. Set the schema to a personal dev schema (e.g., `dev_jdoe`) so your work is isolated.

**Configure profiles.yml**

Edit `dbt/profiles.yml` with your warehouse connection settings. The template uses environment variables so credentials stay in `.env` (which is gitignored).

**Source environment and verify**

```bash
source .venv/bin/activate
set -a && source .env && set +a   # loads .env vars into your shell
cd dbt && dbt debug                # should show "All checks passed!"
```

**Install dbt packages**
```bash
dbt deps
```

### Before Running Any dbt Command

Before executing any dbt command, you MUST verify that the environment is ready:
1. Check that `.venv/` exists. If not, create it: `python3 -m venv .venv`
2. Activate it: `source .venv/bin/activate`
3. Check that dbt is installed: `which dbt`. If not found, install it: `pip install dbt-snowflake` (or the appropriate adapter from `requirements.txt` if it exists)
4. Check that `.env` exists. If not, warn the user to create one from `.env.example`
5. Source the environment: `set -a && source .env && set +a`
6. Run commands from the `dbt/` directory


### Branching

Always ask for confirmation before committing or pushing anything. Never commit directly to main. Always create a separate branch:

```bash
git checkout main && git pull origin main
git checkout -b feature/<description>
```

### dbt Commands

These are basic commands, feel free to add additional arguments if applicable.
Run from the `dbt/` directory with the virtual environment activated and `.env` sourced:

```bash
dbt build --target dev -s <selection> # build + test
dbt test --select <selection>         # test only
dbt docs generate                     # generate documentation
dbt compile                           # generates executable SQL from Jinja/YAML models without touching the database, storing it in the /target folder for debugging
dbt run                               # Materialize models (create tables/views)
```