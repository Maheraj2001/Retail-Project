from fastapi import APIRouter, Depends, Query, Request
from typing import Optional

from app.models.schemas import (
    KPISummaryResponse,
    SalesResponse,
    PaginatedResponse,
)

router = APIRouter()


def get_data_client(request: Request):
    return request.app.state.sf_client


@router.get("/kpi/daily", response_model=list[KPISummaryResponse])
def get_daily_kpis(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    sf=Depends(get_data_client),
):
    """Retrieve daily KPI summary metrics."""
    query = "SELECT * FROM gold.kpi_daily_summary WHERE 1=1"
    params = {}

    if start_date:
        query += " AND date_key >= %(start_date)s"
        params["start_date"] = start_date
    if end_date:
        query += " AND date_key <= %(end_date)s"
        params["end_date"] = end_date

    query += " ORDER BY date_key DESC"
    return sf.fetch_all(query, params)


@router.get("/sales", response_model=PaginatedResponse)
def get_sales(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    customer_id: Optional[str] = Query(None),
    sf=Depends(get_data_client),
):
    """Retrieve paginated sales data."""
    offset = (page - 1) * page_size

    where_clause = ""
    params = {"limit": page_size, "offset": offset}

    if customer_id:
        where_clause = "WHERE customer_id = %(customer_id)s"
        params["customer_id"] = customer_id

    count_query = f"SELECT COUNT(*) as total FROM gold.fact_sales {where_clause}"
    total = sf.fetch_one(count_query, params)["total"]

    data_query = f"""
        SELECT * FROM gold.fact_sales
        {where_clause}
        ORDER BY date_key DESC
        LIMIT %(limit)s OFFSET %(offset)s
    """
    rows = sf.fetch_all(data_query, params)

    return {
        "data": rows,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": (total + page_size - 1) // page_size,
    }


@router.get("/sales/top-customers")
def get_top_customers(
    limit: int = Query(10, ge=1, le=100),
    sf=Depends(get_data_client),
):
    """Retrieve top customers by revenue."""
    query = """
        SELECT
            f.customer_id,
            c.customer_name,
            c.segment,
            ROUND(SUM(f.net_revenue), 2) AS total_revenue,
            COUNT(DISTINCT f.transaction_id) AS total_orders
        FROM gold.fact_sales f
        JOIN gold.dim_customer c ON f.customer_id = c.customer_id
        GROUP BY f.customer_id, c.customer_name, c.segment
        ORDER BY total_revenue DESC
        LIMIT %(limit)s
    """
    return sf.fetch_all(query, {"limit": limit})
