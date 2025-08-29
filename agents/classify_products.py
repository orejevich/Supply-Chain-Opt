import pandas as pd

def classify_product_node(state: dict) -> dict:
    df = pd.DataFrame(state["inventory_data"])

    # 1. Financial classification based on 30-day revenue
    df["recent_revenue"] = df["average_daily_demand"].fillna(0) * df["unit_cost"] * 30
    df = df.sort_values("recent_revenue", ascending=False).reset_index(drop=True)
    total_revenue = df["recent_revenue"].sum()
    df["revenue_pct"] = df["recent_revenue"].cumsum() / total_revenue

    def assign_financial_class(pct):
        if pct <= 0.7:
            return "A"
        elif pct <= 0.9:
            return "B"
        else:
            return "C"

    df["computed_financial_class"] = df["revenue_pct"].apply(assign_financial_class)

    # 2. Operational risk classification based on lead time and shelf life
    def assign_operational_risk(row):
        long_lead = row["average_lead_time_days"] is not None and row["average_lead_time_days"] > 14
        short_shelf = row["shelf_life_days"] is not None and row["shelf_life_days"] < 30

        if long_lead and short_shelf:
            return "A"
        elif long_lead or short_shelf:
            return "B"
        else:
            return "C"

    df["computed_operational_risk"] = df.apply(assign_operational_risk, axis=1)

    # Replace inventory_data with enriched version
    state["inventory_data"] = df.drop(columns=["revenue_pct", "recent_revenue"]).to_dict(orient="records")
    return state
