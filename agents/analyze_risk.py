import pandas as pd

def risk_analyzer_node(state: dict) -> dict:
    df = pd.DataFrame(state['inventory_data'])

    # Ensure demand values are filled
    df['average_daily_demand'] = df['average_daily_demand'].fillna(0)
    df['forecasted_demand_30d'] = df.get('forecasted_demand_30d', 0)

    def calculate_risk(row):
        daily_demand = row['average_daily_demand'] or (row['forecasted_demand_30d'] / 30)
        if daily_demand == 0:
            return pd.Series({
                'at_risk_of_stockout': False,
                'days_until_stockout': None,
                'expected_consumption_during_lead_time': 0
            })

        expected_consumption = daily_demand * row['average_lead_time_days']
        return pd.Series({
            'at_risk_of_stockout': row['available_stock'] < expected_consumption,
            'days_until_stockout': round(row['available_stock'] / daily_demand, 2),
            'expected_consumption_during_lead_time': round(expected_consumption, 2)
        })

    risk_metrics = df.apply(calculate_risk, axis=1)
    df = pd.concat([df, risk_metrics], axis=1)

    state['inventory_data'] = df.to_dict(orient='records')
    return state