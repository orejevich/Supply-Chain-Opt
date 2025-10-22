from agents.classify_products import classify_product_node

def test_classify_adds_fields(tiny_state):
    out = classify_product_node(tiny_state)
    row = out["inventory_data"][0]
    assert "computed_financial_class" in row
    assert "computed_operational_risk" in row

    assert row["computed_financial_class"] is not None
    assert row["computed_operational_risk"] is not None

    assert row["computed_financial_class"] in ["A", "B", "C"]
    assert row["computed_operational_risk"] in ["A", "B", "C"]