from datetime import date


def test_get_daily_kpis(test_client, mock_sf_client):
    mock_sf_client.fetch_all.return_value = [
        {
            "date_key": date(2024, 1, 15),
            "total_transactions": 120,
            "unique_customers": 80,
            "total_revenue": 15000.50,
            "avg_order_value": 125.00,
            "total_discounts": 450.25,
        }
    ]

    response = test_client.get("/api/v1/kpi/daily?start_date=2024-01-01&end_date=2024-01-31")
    assert response.status_code == 200

    data = response.json()
    assert len(data) == 1
    assert data[0]["total_transactions"] == 120


def test_get_sales_paginated(test_client, mock_sf_client):
    mock_sf_client.fetch_one.return_value = {"total": 100}
    mock_sf_client.fetch_all.return_value = [
        {"transaction_id": "T001", "net_revenue": 99.99}
    ]

    response = test_client.get("/api/v1/sales?page=1&page_size=10")
    assert response.status_code == 200

    data = response.json()
    assert data["total"] == 100
    assert data["page"] == 1
    assert len(data["data"]) == 1


def test_get_top_customers(test_client, mock_sf_client):
    mock_sf_client.fetch_all.return_value = [
        {
            "customer_id": "C001",
            "customer_name": "Acme Corp",
            "segment": "Enterprise",
            "total_revenue": 50000.00,
            "total_orders": 25,
        }
    ]

    response = test_client.get("/api/v1/sales/top-customers?limit=5")
    assert response.status_code == 200
    assert len(response.json()) == 1
