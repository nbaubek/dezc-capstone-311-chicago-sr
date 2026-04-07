

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

Never commit directly to main. Always branch:
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