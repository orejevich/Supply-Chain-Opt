import pandas as pd
import os
from sqlalchemy import create_engine, text

def get_sqlalchemy_engine():
    user = os.getenv('USER')
    return create_engine(f'postgresql://{user}@localhost:5432/supply_chain_optimizer')

def fetch_inventory_node(state: dict) -> dict:
    engine = get_sqlalchemy_engine()

    query = text("""
        SELECT
            p.product_id,
            p.sku,
            p.name,
            i.current_stock,
            i.committed_stock,
            i.reorder_point,
            (i.current_stock - i.committed_stock) AS available_stock,
            ps.average_lead_time_days,
            ps.lead_time_std_dev,
            ps.unit_cost,
            s.supplier_id,
            s.reliability_score,
            (
                SELECT AVG(actual_demand)
                FROM demand_history dh
                WHERE dh.product_id = p.product_id
                  AND dh.date >= CURRENT_DATE - INTERVAL '30 days'
            ) AS average_daily_demand,
            i.last_stockout_date,
            p.shelf_life_days,
            p.financial_classification,
            p.operational_risk
        FROM
            products p
        JOIN inventory i ON p.product_id = i.product_id
        JOIN product_suppliers ps ON p.product_id = ps.product_id
        JOIN suppliers s ON ps.supplier_id = s.supplier_id
        ORDER BY p.product_id;
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    state['inventory_data'] = df.to_dict(orient='records')
    return state

