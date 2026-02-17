from __future__ import annotations

# AWS Glue job for processing silver layer orders data

import json
from datetime import datetime


def validate_order(order: dict) -> bool:
    """Validate an order record has required fields."""
    return (
        order.get("order_id") is not None
        and order.get("customer_id") is not None
        and order.get("order_date") is not None
    )


def process_orders(orders: list[dict]) -> dict:
    """Process orders data and return metrics."""
    valid_orders = []
    invalid_orders = []
    
    for order in orders:
        if validate_order(order):
            valid_orders.append(order)
        else:
            invalid_orders.append(order)
    
    return {
        "total_orders": len(orders),
        "valid_orders": len(valid_orders),
        "invalid_orders": len(invalid_orders),
        "processed_at": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    # Sample orders data for testing
    orders = [
        {
            "order_id": 1,
            "customer_id": 100,
            "order_date": "2025-01-01",
            "amount": 99.99,
        },
        {
            "order_id": 2,
            "customer_id": None,
            "order_date": "2025-01-02",
            "amount": 49.99,
        },
        {
            "order_id": 3,
            "customer_id": 101,
            "order_date": "2025-01-03",
            "amount": 149.99,
        },
    ]
    
    result = process_orders(orders)
    print(json.dumps(result, indent=2))
