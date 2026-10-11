# Alert integration verification

Fixed the back-in-stock query to use stock_changed, matching
the ClickHouse product_changes view.

## Verified scenario

Three simulated events were sent through:
Replay producer -> Kafka -> Spark -> ClickHouse -> alert evaluation.

Price and availability:
- 100 EGP: in_stock
- 90 EGP: out_of_stock
- 80 EGP: in_stock

Rules were created before the observations.
The price-below threshold was 85 EGP.

Verified:
- All three exact event IDs reached ClickHouse.
- Back-in-stock: one alert for the final event.
- Price-drop: two alerts for the second and final events.
- Price-below: one alert for the final event.
- Re-evaluation created zero duplicate alerts.

This verifies the three rule types for this simulated scenario.
It does not establish email/SMS delivery or all edge cases.

## Recorded evidence

```json
{
  "success": true,
  "verified_at": "2026-10-11T09:17:03.208037+00:00",
  "data_origin": "simulated",
  "source": "demo_alert_test_58d67d5e28a4",
  "product": "TEST-58d67d5e28a4",
  "event_ids": [
    "58a07c24-7727-4a74-a795-890ee82c0343",
    "f1609b21-5eaa-4f9e-ada4-282c56155f75",
    "7599e54f-90e1-4da8-a029-825a047eddd6"
  ],
  "received_events": 3,
  "alerts_by_type": {
    "price_below": 1,
    "price_drop": 2,
    "back_in_stock": 1
  },
  "duplicate_alerts_created": 0
}
```
