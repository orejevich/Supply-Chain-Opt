from langgraph.graph import StateGraph
from typing import TypedDict

# Import all node functions
from agents.fetch_inventory import fetch_inventory_node
from agents.classify_products import classify_product_node
from agents.forcast_demand import forecast_demand_node
from agents.analyze_risk import risk_analyzer_node
from agents.recommend import recommend_reorder_node

class InventoryState(TypedDict):
    inventory_data: list
    forecasted_data: list
    risk_summary: dict

builder = StateGraph(InventoryState)

# Register nodes
builder.add_node("fetch_inventory", fetch_inventory_node)
builder.add_node("classify_product", classify_product_node)
builder.add_node("forecast_demand", forecast_demand_node)
builder.add_node("risk_analyzer", risk_analyzer_node)
builder.add_node("recommend_reorder", recommend_reorder_node)

# Wire them together
builder.set_entry_point("fetch_inventory")
builder.add_edge("fetch_inventory", "classify_product")
builder.add_edge("classify_product", "forecast_demand")
builder.add_edge("forecast_demand", "risk_analyzer")
builder.add_edge("risk_analyzer", "recommend_reorder")
builder.set_finish_point("recommend_reorder")

# Compile the graph
graph = builder.compile()

if __name__ == "__main__":
    final_state = graph.invoke({})
    
    print("=== Recommended Reorders ===")
    for product in final_state["inventory_data"]:
        if product.get("should_reorder"):
            print(f"{product['sku']}: reorder {product['recommended_reorder_qty']} units — {product['reorder_reason']}")
