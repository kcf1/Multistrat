# Binance API: restricted rules and error codes

This doc summarizes Binance Spot API rules that affect integration and tests (testnet and mainnet). Sources: [Binance Spot API](https://developers.binance.com/docs/binance-spot-api-docs/rest-api), [Testnet FAQ](https://developers.binance.com/docs/binance-spot-api-docs/faqs/testnet), [Filters](https://developers.binance.com/docs/binance-spot-api-docs/filters), and [Binance Developer Community](https://dev.binance.vision/).

---

## 1. Testnet vs mainnet API keys (code -2014)

**Error:** `API-key format invalid` (HTTP 401, code **-2014**).

**Rule:** Testnet and mainnet use **different API keys**. Keys created on [binance.com](https://www.binance.com) (mainnet) do **not** work on [testnet.binance.vision](https://testnet.binance.vision). Keys created on testnet do not work on mainnet.

**What to do:**

- For **testnet**: log in at https://testnet.binance.vision (e.g. with GitHub), go to Profile → API Management, create an HMAC_SHA256 key. Use base URL `https://testnet.binance.vision` (no `/api` in the base; paths are e.g. `/api/v3/order`).
- For **mainnet**: create keys at binance.com and use `https://api.binance.com`.
- Ensure `BINANCE_API_KEY` and `BINANCE_API_SECRET` have no extra quotes, spaces, or newlines.

**Sources:**

- [How to Test on Binance Testnet | Binance Support](https://www.binance.com/en/support/faq/how-to-test-my-functions-on-binance-testnet-ab78f9a1b8824cf0a106b4229c76496d)
- [Testnet FAQ – Binance docs](https://developers.binance.com/docs/binance-spot-api-docs/faqs/testnet)
- [API-key format invalid – Binance Developer Community](https://dev.binance.vision/t/apierror-code-2014-api-key-format-invalid/2817)

---

## 2. Signature for signed endpoints (code -1022)

**Error:** `Signature for this request is not valid` (HTTP 400, code **-1022**).

**Rule:** The HMAC-SHA256 signature must be computed over the **exact** query string that the server receives. If the order of parameters or their encoding differs between the string you sign and the request you send, verification fails.

**What to do:**

- Build the query string from **all** query parameters (including `timestamp`; optionally `recvWindow`).
- Use a **deterministic order** when signing (e.g. alphabetical by key). Send the request with parameters in the **same** order (e.g. pass `sorted(params.items())` or the same ordered list to your HTTP client).
- Use the **same string encoding** for signing and for the request (e.g. same number formatting: send `quantity` and `price` as strings in the format you use in the signed string).
- Include the API key in the `X-MBX-APIKEY` header.

**Sources:**

- [FAQ: Signature for this request is not valid – Binance Developer Community](https://dev.binance.vision/t/faq-signature-for-this-request-is-not-valid/176)
- [Request Security – Binance Spot API](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/request-security)

---

## 3. Symbol filters: PERCENT_PRICE_BY_SIDE (code -1013)

**Error:** `Filter failure: PERCENT_PRICE_BY_SIDE` (HTTP 400, code **-1013**).

**Rule:** Limit order prices must stay within the symbol’s **PERCENT_PRICE_BY_SIDE** filter. The filter uses a weighted average price (or last price) and multipliers:

- **Buy:** `weightedAveragePrice × bidMultiplierDown` ≤ order price ≤ `weightedAveragePrice × bidMultiplierUp`
- **Sell:** `weightedAveragePrice × askMultiplierDown` ≤ order price ≤ `weightedAveragePrice × askMultiplierUp`

If the limit price is too far from the current market (e.g. a buy at 1000 when market is 100000), the order is rejected.

**What to do:**

- For limit orders, use a price within the allowed range (e.g. a percentage below last price for buys, above for sells).
- Optionally read filters from **GET /api/v3/exchangeInfo** (symbol → `filters` → `PERCENT_PRICE_BY_SIDE`) and/or use **GET /api/v3/ticker/price** to get last price and compute a valid price.

**Sources:**

- [Filters – Binance Spot API](https://developers.binance.com/docs/binance-spot-api-docs/filters)
- [Price Filter and Percent Price – Binance Academy](https://academy.binance.com/en/articles/binance-api-responses-price-filter-and-percent-price)

---

## 4. Other symbol filters

Symbols also have filters such as **LOT_SIZE** (min/max quantity, step), **MIN_NOTIONAL** (min order value), **PRICE_FILTER** (tick size, min/max price). Violations return **-1013** with the filter name. Check **GET /api/v3/exchangeInfo** for each symbol’s `filters`.

**Source:** [Filters – Binance Spot API](https://developers.binance.com/docs/binance-spot-api-docs/filters)

---

## 5. User data stream (no signature)

Endpoints **POST/PUT/DELETE /api/v3/userDataStream** (create/keepalive/close listen key) require only the **X-MBX-APIKEY** header; they do **not** require a signature. Same API key as for signed endpoints; must be a **testnet** key when using testnet.

**WebSocket URL:** For **testnet**, the stream host is **stream.testnet.binance.vision** (not testnet.binance.vision). Full URL: `wss://stream.testnet.binance.vision/ws/<listenKey>`. Mainnet: `wss://stream.binance.com:9443/ws/<listenKey>`.

**Source:** [User Data Stream – Binance Spot API](https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream), [Testnet WebSocket Streams](https://developers.binance.com/docs/binance-spot-api-docs/testnet/web-socket-streams)
