# Design Decisions

## 1. Why Snowflake over Azure Synapse?

| Criteria            | Snowflake                        | Azure Synapse                |
|---------------------|----------------------------------|------------------------------|
| Compute/Storage     | Fully separated                  | Partially coupled            |
| Cross-cloud         | Yes (AWS, Azure, GCP)           | Azure only                   |
| Concurrency         | Multi-cluster auto-scale        | Limited without scaling      |
| Maintenance         | Fully managed, zero-tuning      | Requires distribution keys   |
| Cost model          | Per-second billing               | Provisioned DWUs             |

**Decision**: Snowflake — better for POC portability and simpler management.

## 2. Why Delta Lake over Parquet?

- ACID transactions on data lake
- Time travel (query historical data)
- Schema evolution (handle source changes gracefully)
- MERGE support for upserts and SCD patterns
- OPTIMIZE + ZORDER for query performance

## 3. Why Metadata-Driven ADF Pipelines?

Instead of one pipeline per source, we use a configuration table:
- Single pipeline handles N sources
- Adding a new source = adding a row to config (no code change)
- Demonstrates senior-level pipeline design thinking

## 4. Why FastAPI over Flask/Django?

- Async support out of the box
- Auto-generated OpenAPI docs (Swagger UI)
- Pydantic for request/response validation
- Lightweight — no ORM overhead for read-only queries

## 5. Star Schema Design

- Fact table (`fact_sales`) stores measures at transaction grain
- Dimension tables (`dim_date`, `dim_customer`, `dim_product`) for slicing
- Pre-computed `kpi_daily_summary` reduces dashboard query time
- Clustered by `date_key` in Snowflake for range scan performance
