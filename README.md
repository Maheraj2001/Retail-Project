# Retail-Project

This is a POC for a retail project — an end-to-end Data Lakehouse platform on Azure with Snowflake.

## Architecture

```
Sources → ADF → ADLS Gen2 (Bronze) → Databricks (Silver/Gold) → Snowflake → Power BI / API
```

See [docs/architecture.md](docs/architecture.md) for full details.

## Tech Stack

- **Orchestration**: Azure Data Factory (metadata-driven pipelines)
- **Storage**: Azure Data Lake Storage Gen2
- **Processing**: Databricks + PySpark (Medallion Architecture)
- **Warehouse**: Snowflake (star schema, RBAC)
- **API**: FastAPI (Python)
- **BI**: Power BI
- **IaC**: Terraform
- **CI/CD**: GitHub Actions

## Project Structure

```
├── infrastructure/terraform/   # IaC for Azure resources
├── pipelines/adf/              # ADF pipeline definitions
├── databricks/notebooks/       # PySpark notebooks (bronze/silver/gold)
├── snowflake/                  # DDL, views, stored procedures
├── api/                        # FastAPI REST service
├── powerbi/                    # Power BI report files
├── tests/                      # Unit & integration tests
├── docs/                       # Architecture & design docs
├── config/                     # Environment config templates
└── .github/workflows/          # CI/CD pipelines
```

## Getting Started

### Prerequisites

- Azure subscription
- Snowflake account
- Python 3.11+
- Terraform >= 1.5
- Databricks CLI

### Setup

1. Clone the repo
   ```bash
   git clone https://github.com/Maheraj2001/Retail-Project.git
   cd Retail-Project
   ```

2. Configure environment
   ```bash
   cp config/dev.env.example .env
   # Edit .env with your credentials
   ```

3. Deploy infrastructure
   ```bash
   cd infrastructure/terraform
   terraform init
   terraform plan
   terraform apply
   ```

4. Run Snowflake DDL
   ```bash
   # Execute in order:
   # snowflake/schemas/create_database.sql
   # snowflake/schemas/create_tables.sql
   # snowflake/views/analytics_views.sql
   # snowflake/procedures/refresh_kpi.sql
   ```

5. Run the API locally
   ```bash
   cd api
   pip install -r requirements.txt
   uvicorn app.main:app --reload
   ```

6. Run tests
   ```bash
   pytest tests/ -v
   ```

## Key Design Decisions

See [docs/design-decisions.md](docs/design-decisions.md) for detailed rationale on:
- Snowflake vs Synapse
- Delta Lake vs Parquet
- Metadata-driven ADF pipelines
- Star schema modeling
