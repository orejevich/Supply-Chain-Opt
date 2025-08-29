import pandas as pd

def recommend_reorder_node(state: dict) -> dict:
    df = pd.DataFrame(state["inventory_data"])

    # Default assumptions
    lead_time_buffer = 1.25   # buffer multiplier for lead time consumption
    reorder_days_coverage = 30  # how many days to cover in reorder quantity

    def calculate_reorder(row):
        daily_demand = row["average_daily_demand"] or (row.get("forecasted_demand_30d", 0) / 30)
        if daily_demand == 0:
            return pd.Series({
                "should_reorder": False,
                "recommended_reorder_qty": 0,
                "reorder_reason": "No demand"
            })

        # Use average lead time + 1 std deviation for buffer
        lead_time_days = row.get("average_lead_time_days", 0)
        lead_time_std_dev = row.get("lead_time_std_dev", 0)
        effective_lead_time = lead_time_days + lead_time_std_dev

        expected_consumption = daily_demand * effective_lead_time

        if row["available_stock"] >= expected_consumption:
            return pd.Series({
                "should_reorder": False,
                "recommended_reorder_qty": 0,
                "reorder_reason": "Sufficient stock through lead time"
            })

        target_stock = daily_demand * reorder_days_coverage
        reorder_qty = max(0, round(target_stock - row["available_stock"]))

        return pd.Series({
            "should_reorder": True,
            "recommended_reorder_qty": reorder_qty,
            "reorder_reason": "Stockout risk within lead time"
        })

    reorder_info = df.apply(calculate_reorder, axis=1)
    df = pd.concat([df, reorder_info], axis=1)
    state["inventory_data"] = df.to_dict(orient="records")
    return state
