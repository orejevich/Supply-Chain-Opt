import pytest
import pandas as pd
from copy import deepcopy

import os, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.insert(0, ROOT)

@pytest.fixture
def tiny_inventory_records():
    # 3 products with varied demand/lead times
    return [
        {
            "product_id": 1, "sku": "SKU-A", "name": "Alpha",
            "current_stock": 80, "committed_stock": 10, "reorder_point": 30,
            "available_stock": 70,
            "average_lead_time_days": 14, "lead_time_std_dev": 3, "unit_cost": 5.0,
            "supplier_id": 100, "reliability_score": 0.95,
            "average_daily_demand": 5.0, "last_stockout_date": None,
            "shelf_life_days": 90, "financial_classification": "B", "operational_risk": "C",
            "forecasted_demand_30d": 150  # present for downstream nodes
        },
        {
            "product_id": 2, "sku": "SKU-B", "name": "Beta",
            "current_stock": 20, "committed_stock": 10, "reorder_point": 15,
            "available_stock": 10,
            "average_lead_time_days": 20, "lead_time_std_dev": 5, "unit_cost": 2.0,
            "supplier_id": 101, "reliability_score": 0.7,
            "average_daily_demand": 3.0, "last_stockout_date": None,
            "shelf_life_days": 25, "financial_classification": "C", "operational_risk": "A",
            "forecasted_demand_30d": 90
        },
        {
            "product_id": 3, "sku": "SKU-C", "name": "Gamma",
            "current_stock": 300, "committed_stock": 0, "reorder_point": 40,
            "available_stock": 300,
            "average_lead_time_days": 7, "lead_time_std_dev": 1, "unit_cost": 9.0,
            "supplier_id": 102, "reliability_score": 0.99,
            "average_daily_demand": 0.0, "last_stockout_date": None,
            "shelf_life_days": 365, "financial_classification": "A", "operational_risk": "C",
            "forecasted_demand_30d": 0
        },
    ]

@pytest.fixture
def tiny_state(tiny_inventory_records):
    return {"inventory_data": tiny_inventory_records}