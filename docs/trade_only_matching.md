# Trade-only matching (Rust)

Trade-only matching is opt-in, per asset. It is supported by the level-2
`NoPartialFillExchange`. The default remains `MatchingMode::BookAndTrade`.

## Enable it on an exchange

```rust
use hftbacktest::{
    backtest::{
        MatchingMode,
        assettype::LinearAsset,
        models::{CommonFees, ConstantLatency, RiskAdverseQueueModel, TradingValueFeeModel},
        order::order_bus,
        proc::NoPartialFillExchange,
        state::State,
    },
    depth::HashMapMarketDepth,
};

let (exchange_bus, local_bus) = order_bus(ConstantLatency::new(
    100_000_000, // order entry: 100 ms, in nanoseconds
    20_000_000,  // response: 20 ms, in nanoseconds
));
let exchange = NoPartialFillExchange::new(
    HashMapMarketDepth::new(0.01, 0.001),
    State::new(
        LinearAsset::new(1.0),
        TradingValueFeeModel::new(CommonFees::new(0.0, 0.000153)),
    ),
    RiskAdverseQueueModel::new(),
    exchange_bus,
)
.matching_mode(MatchingMode::TradeOnly);
// Use `exchange` and the matching `local_bus` to construct your Asset.
```

The enabling line is `.matching_mode(MatchingMode::TradeOnly)`. Configure it
before processing orders. Omit the line, or use `MatchingMode::BookAndTrade`,
for the original behavior.

## Enable it with the standard builder

Add the same setting to an existing `L2AssetBuilder`:

```rust,ignore
let asset = builder
    .exchange(ExchangeKind::NoPartialFillExchange)
    .matching_mode(MatchingMode::TradeOnly)
    .build()?;
```

Selecting `PartialFillExchange` together with `TradeOnly` returns a build error.
This option is not exposed by the level-3 or Python builders.

## Execution rules

- Only **opposing-aggressor exchange trade events** trigger fills. A sell trade
  can fill a buy order at or above its price; a buy trade can fill a sell order
  at or below its price. Same-price trades must also exhaust the queue ahead.
- New and repriced orders cannot use trades with timestamps at or before their
  exchange acceptance time. Quantity reductions preserve queue priority and
  eligibility; increases and repricing reinitialize them.
- All fills execute at the **order limit price**, including orders that crossed
  the recorded book at acceptance. The cached best bid/ask never supplies an
  execution price in this mode.
- Book updates initialize the book, but never trigger fills or advance an
  existing order's queue. Initial queue depth still comes from the book.
- The no-partial-fill assumption is retained: an eligible fill executes the
  whole remaining order, without sharing a finite trade-volume budget across
  simulated orders. This mode is not an exact market-impact simulation.
- Order entry, cancel, modify and response delays still use the latency model.
  An order can fill while its cancellation is in flight.

Only **GTC and GTX limit orders** are supported. Market, IOC and FOK requests
return `BacktestError::InvalidOrderRequest`. GTX orders crossing the recorded
book are still expired at acceptance.

## Fees and remaining book dependence

Fee classification is estimated at acceptance/requeue. A GTC order crossing
the then-recorded book is charged taker fees when it eventually fills; other
accepted orders are charged maker fees. This label does not trigger a fill or
grant book-price improvement. Reducing quantity preserves the label.

This is an approximation: delayed book timestamps can still affect initial
queue depth, GTX rejection and the maker/taker fee classification. The option
removes **book-triggered executions**; it does not reconstruct missing book
transaction timestamps or establish the true maker/taker identity.
