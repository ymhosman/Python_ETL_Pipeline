import pandas as pd
import numpy as np

from transform import transform, reject, aggregate  # change to your actual module name


def test_transform_basic_logic():
    orders_raw = pd.DataFrame({
        "order_price": [100, -10, 500],
        "customer_rating": [None, 4, 5],
        "restaurant_code": ["A", "B", "A"],
        "own_driver": ["Yes", "No", None],
        "delivery_fee": [10, None, 20],
        "discount_applied": [5, None, 10],
        "adv_delivery_time": [30, 30, None],
        "time_to_deliver": [25, 35, 50],
        "quantity": [1, 2, 0]
    })

    ref_raw = pd.DataFrame({
        "restaurant_code": ["A", "B"],
        "category": ["Fast Food", None]
    })

    db = transform(orders_raw, ref_raw)

    # null rating filled
    assert db["customer_rating"].isna().sum() == 0
    assert (db["customer_rating"] == 2.5).any()

    # own_driver encoded
    assert set(db["own_driver"].dropna().unique()).issubset({0, 1})

    # derived columns exist
    assert "total_price" in db.columns
    assert "delivered_on_time" in db.columns

    # reject_reason created
    assert "reject_reason" in db.columns


def test_reject_split():
    df = pd.DataFrame({
        "order_price": [100, -5],
        "quantity": [1, 0],
        "reject_reason": ["", "INVALID ORDER PRICE,INVALID ITEM QUANTITY"],
        "time_to_deliver": [10, 20],
        "discount_applied": [0, 0],
        "own_driver": [1, 0],
        "adv_delivery_time": [15, 15]
    })

    clean, rejected = reject(df)

    assert len(clean) == 1
    assert len(rejected) == 1

    assert "reject_reason" not in clean.columns
    assert "order_price" not in rejected.columns


def test_aggregate_calculation():
    df = pd.DataFrame({
        "restaurant_code": ["A", "A", "B"],
        "total_price": [100, 200, 300],
        "customer_rating": [4, 5, 3],
        "quantity": [1, 2, 3],
        "sale_id": [1, 1, 1],
        "delivered_on_time": ["Yes", "No", "Yes"]
    })

    result = aggregate(df)

    # shape check
    assert set(result["restaurant_code"]) == {"A", "B"}

    # revenue aggregation
    assert result.loc[result["restaurant_code"] == "A", "total_revenue"].iloc[0] == 300

    # percent on time computed correctly
    a_row = result[result["restaurant_code"] == "A"].iloc[0]
    assert a_row["percent_on_time"] == 50.0