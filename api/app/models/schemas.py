from pydantic import BaseModel
from typing import Any, Optional
from datetime import date


class KPISummaryResponse(BaseModel):
    date_key: date
    total_transactions: int
    unique_customers: int
    total_revenue: float
    avg_order_value: float
    total_discounts: float


class SalesResponse(BaseModel):
    transaction_id: str
    date_key: date
    customer_id: str
    product_id: str
    quantity: int
    unit_price: float
    discount: Optional[float] = 0
    net_revenue: float
    gross_revenue: float


class PaginatedResponse(BaseModel):
    data: list[dict[str, Any]]
    page: int
    page_size: int
    total: int
    total_pages: int
