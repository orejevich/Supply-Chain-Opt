import pandas as pd
import os
from sqlalchemy import create_engine, text

def get_sqlalchemy_engine():
    user = os.getenv('USER')
    return create_engine(f'postgresql://{user}@localhost:5432/supply_chain_optimizer')

def forecast_demand_node(state: dict) -> dict:
    engine = get_sqlalchemy_engine()
    product_ids = [item['product_id'] for item in state['inventory_data']]

    query = text("""
        SELECT product_id, date, actual_demand
        FROM demand_history
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
          AND product_id = ANY(:product_ids)
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"product_ids": product_ids})

    # Calculate 30-day total demand per product
    forecast_df = df.groupby('product_id')['actual_demand'].sum().reset_index()
    forecast_df.rename(columns={'actual_demand': 'forecasted_demand_30d'}, inplace=True)

    # Merge back into inventory_data
    enriched = []
    for record in state['inventory_data']:
        match = forecast_df[forecast_df['product_id'] == record['product_id']]
        record['forecasted_demand_30d'] = int(match['forecasted_demand_30d'].iloc[0]) if not match.empty else 0
        enriched.append(record)

    state['inventory_data'] = enriched
    return state
