"""
Local CSV-based data client — drop-in replacement for SnowflakeClient.
Reads from data/raw/ CSVs so the API works without any cloud subscription.
"""

import csv
import os
from datetime import datetime
from typing import Optional


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "raw")


class LocalClient:
    """Mimics SnowflakeClient interface using local CSV files."""

    def __init__(self):
        self.orders = []
        self.customers = []
        self.products = []

    def connect(self):
        self.customers = self._load_csv("customers.csv")
        self.products = self._load_csv("products.csv")
        self.orders = self._load_csv("orders.csv")
        self._enrich_orders()
        print(f"LocalClient loaded: {len(self.orders)} orders, "
              f"{len(self.customers)} customers, {len(self.products)} products")

    def close(self):
        pass

    def _load_csv(self, filename: str) -> list[dict]:
        filepath = os.path.join(DATA_DIR, filename)
        with open(filepath, newline="") as f:
            return list(csv.DictReader(f))

    def _enrich_orders(self):
        """Cast numeric fields and build lookup maps."""
        customer_map = {c["customer_id"]: c for c in self.customers}
        product_map = {p["product_id"]: p for p in self.products}

        for o in self.orders:
            o["quantity"] = int(o["quantity"])
            o["unit_price"] = float(o["unit_price"])
            o["discount"] = float(o["discount"])
            o["gross_revenue"] = float(o["gross_revenue"])
            o["net_revenue"] = float(o["net_revenue"])
            o["profit"] = float(o["profit"])
            o["date_key"] = o["order_date"]

            # Attach customer/product info
            cust = customer_map.get(o["customer_id"], {})
            prod = product_map.get(o["product_id"], {})
            o["customer_name"] = cust.get("customer_name", "")
            o["segment"] = cust.get("segment", "")
            o["region"] = cust.get("region", "")
            o["category"] = prod.get("category", "")
            o["sub_category"] = prod.get("sub_category", "")
            o["product_name"] = prod.get("product_name", "")

        self._customer_map = customer_map
        self._product_map = product_map

    def fetch_all(self, query: str, params: Optional[dict] = None) -> list[dict]:
        """Route queries to the appropriate local handler."""
        params = params or {}
        q = query.lower()

        if "kpi_daily_summary" in q:
            return self._kpi_daily(params)
        elif "top" in q or ("customer" in q and "group by" in q):
            return self._top_customers(params)
        elif "count(*)" in q and "fact_sales" in q:
            return self._count_sales(params)
        elif "fact_sales" in q:
            return self._paginated_sales(params)
        return []

    def fetch_one(self, query: str, params: Optional[dict] = None) -> dict:
        params = params or {}
        if "count" in query.lower():
            return self._count_sales(params)
        results = self.fetch_all(query, params)
        return results[0] if results else {}

    # ---- Query handlers ----

    def _kpi_daily(self, params: dict) -> list[dict]:
        from collections import defaultdict

        daily = defaultdict(lambda: {"transactions": set(), "customers": set(),
                                      "revenue": 0.0, "discounts": 0.0})

        for o in self.orders:
            date = o["date_key"]
            if params.get("start_date") and date < params["start_date"]:
                continue
            if params.get("end_date") and date > params["end_date"]:
                continue

            d = daily[date]
            d["transactions"].add(o["order_id"])
            d["customers"].add(o["customer_id"])
            d["revenue"] += o["net_revenue"]
            d["discounts"] += o["gross_revenue"] - o["net_revenue"]

        results = []
        for date_key in sorted(daily.keys(), reverse=True):
            d = daily[date_key]
            tx_count = len(d["transactions"])
            results.append({
                "date_key": date_key,
                "total_transactions": tx_count,
                "unique_customers": len(d["customers"]),
                "total_revenue": round(d["revenue"], 2),
                "avg_order_value": round(d["revenue"] / tx_count, 2) if tx_count else 0,
                "total_discounts": round(d["discounts"], 2),
            })
        return results

    def _top_customers(self, params: dict) -> list[dict]:
        from collections import defaultdict

        agg = defaultdict(lambda: {"revenue": 0.0, "orders": set()})
        for o in self.orders:
            cid = o["customer_id"]
            agg[cid]["revenue"] += o["net_revenue"]
            agg[cid]["orders"].add(o["order_id"])

        limit = params.get("limit", 10)
        sorted_customers = sorted(agg.items(), key=lambda x: x[1]["revenue"], reverse=True)[:limit]

        results = []
        for cid, data in sorted_customers:
            cust = self._customer_map.get(cid, {})
            results.append({
                "customer_id": cid,
                "customer_name": cust.get("customer_name", ""),
                "segment": cust.get("segment", ""),
                "total_revenue": round(data["revenue"], 2),
                "total_orders": len(data["orders"]),
            })
        return results

    def _count_sales(self, params: dict) -> dict:
        if params.get("customer_id"):
            total = sum(1 for o in self.orders if o["customer_id"] == params["customer_id"])
        else:
            total = len(self.orders)
        return {"total": total}

    def _paginated_sales(self, params: dict) -> list[dict]:
        filtered = self.orders
        if params.get("customer_id"):
            filtered = [o for o in filtered if o["customer_id"] == params["customer_id"]]

        filtered = sorted(filtered, key=lambda x: x["date_key"], reverse=True)
        offset = params.get("offset", 0)
        limit = params.get("limit", 50)

        page = filtered[offset:offset + limit]
        return [{
            "transaction_id": o["order_id"],
            "date_key": o["date_key"],
            "customer_id": o["customer_id"],
            "product_id": o["product_id"],
            "quantity": o["quantity"],
            "unit_price": o["unit_price"],
            "discount": o["discount"],
            "net_revenue": o["net_revenue"],
            "gross_revenue": o["gross_revenue"],
        } for o in page]
