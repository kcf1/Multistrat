# OMS orders table: DB columns and injection sources

Single reference for Postgres `orders` columns and where each value is set (Redis → sync, or direct from flow).

| Column | Source | Injected when |
|--------|--------|----------------|
| **internal_id** | OMS | Primary key; set at stage (order_id from risk_approved or generated UUID). |
| **broker** | risk_approved | `stage_order` from stream message. |
| **account_id** | risk_approved | `stage_order` from stream message. |
| **broker_order_id** | Broker place_order response | `update_status` after `adapter.place_order()` (extra_fields). |
| **symbol** | risk_approved | `stage_order` from stream message. |
| **side** | risk_approved | `stage_order` from stream message. |
| **order_type** | risk_approved | `stage_order` from stream message. |
| **quantity** | risk_approved | `stage_order` from stream message. |
| **price** | Broker / fills | Place_order unified response, then fill callback (executed/avg price from event). Binance adapter enriches fill events with payload (price, time_in_force, cumulativeQuoteQty) when event values are 0/null/empty before invoking the OMS callback. |
| **limit_price** | risk_approved + broker | `stage_order` (from stream `price`), then place_order unified response. |
| **time_in_force** | risk_approved / broker / fills | `stage_order`; can be updated from broker response or fill callback. Fill event may get it from WebSocket execution report (`f`) or Binance adapter enriches from order payload when empty. |
| **status** | OMS / broker / fills | pending → sent (place_order) → partially_filled/filled/rejected/cancelled/expired (fills). |
| **executed_qty** | Broker / fills | Place_order unified response, then fill callback (cumulative). |
| **book** | risk_approved | `stage_order` from stream message. |
| **comment** | risk_approved | `stage_order` from stream message. |
| **created_at** | OMS | Set at `stage_order`. |
| **updated_at** | OMS | Set at `stage_order` and every `update_status` / `update_fill_status`. |
| **binance_cumulative_quote_qty** | Binance place_order / fills | `update_status` after place_order; fill event may get it from WebSocket execution report (`Z`) or adapter enriches from payload when 0/null. |
| **binance_transact_time** | Binance place_order | `update_status` after place_order (extra_fields). |
| **payload** | Broker place_order (raw) | `update_status` after place_order; adapter returns `payload={"binance": raw_resp}`. Written to the store **immediately** after place_order returns (before status is set to `sent`) so fill-callback enrichment can use it even when the WebSocket fill arrives first. |

All columns above are written from Redis to Postgres by `sync_one_order` / `sync_terminal_orders` (`oms/sync.py`). Redis order hash is populated by `stage_order` (risk_approved), then `update_status` (place_order response including payload; payload is written as soon as place_order returns), then `update_fill_status` (fills).
