# Architecture — Data Lakehouse POC

## High-Level Data Flow

```
Source Systems
    │
    ▼
Azure Data Factory (Orchestration)
    │
    ├── Copy Activity ──► ADLS Gen2 (Bronze / Raw)
    │
    ▼
Databricks (PySpark)
    ├── Bronze → Schema-on-read, audit columns, Delta Lake
    ├── Silver → Dedup, SCD Type 2, data quality checks
    └── Gold   → Star schema, pre-computed KPIs
    │
    ▼
Snowflake (Data Warehouse)
    ├── Gold tables, views, stored procedures
    └── Role-based access (DATA_READER, DATA_WRITER)
    │
    ├──► Power BI (Dashboards & Reports)
    └──► FastAPI (REST API for programmatic access)
```

## Technology Choices

| Layer          | Technology         | Why                                              |
|----------------|--------------------|--------------------------------------------------|
| Orchestration  | Azure Data Factory | Native Azure integration, metadata-driven design |
| Storage        | ADLS Gen2          | Hierarchical namespace, cost-effective at scale   |
| Processing     | Databricks/PySpark | Scalable compute, Delta Lake ACID support        |
| Warehouse      | Snowflake          | Separation of compute/storage, cross-cloud ready |
| BI             | Power BI           | Enterprise reporting, DAX, row-level security    |
| API            | FastAPI             | Async, auto-docs, Pydantic validation            |
| IaC            | Terraform          | Multi-provider, state management, modules        |
| CI/CD          | GitHub Actions     | Native git integration, free tier for POC        |

## Medallion Architecture

- **Bronze**: Raw data as-is from source. Schema evolution enabled. Audit columns added.
- **Silver**: Cleansed, deduplicated, conformed. SCD Type 2 for slowly changing dimensions.
- **Gold**: Business-ready star schema. Pre-computed aggregations for BI performance.

## Security

- Key Vault for all secrets (connection strings, tokens)
- Managed Identity for ADF ↔ Databricks ↔ ADLS
- Snowflake RBAC (DATA_READER / DATA_WRITER roles)
- Power BI row-level security
- No secrets in code — all via environment variables
