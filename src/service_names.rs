//! Iceoryx2 service-name constants.
//!
//! Centralized so every process on the bus agrees on names.
//! Format: "Domain/Type" (e.g. "MarketData/Trades").

// Market data (pub/sub)
pub const MARKET_DATA_TRADES: &str = "MarketData/Trades";
pub const MARKET_DATA_ORDERBOOK: &str = "MarketData/OrderBook";
pub const MARKET_DATA_TICKER: &str = "MarketData/Ticker";
pub const MARKET_DATA_OHLCV: &str = "MarketData/OHLCV";

// Order management (request/response)
pub const OMS_CREATE_ORDER: &str = "OMS/CreateOrder";
pub const OMS_CANCEL_ORDER: &str = "OMS/CancelOrder";
pub const OMS_MODIFY_ORDER: &str = "OMS/ModifyOrder";
pub const OMS_GET_ORDER: &str = "OMS/GetOrder";

// Order status updates (pub/sub)
pub const OMS_ORDER_UPDATES: &str = "OMS/OrderUpdates";

// Hot-path order execution (request/response)
pub const HOT_PATH_ORDER_EXECUTION: &str = "HotPath/OrderExecution";
