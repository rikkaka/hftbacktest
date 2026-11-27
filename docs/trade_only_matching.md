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

- The **first same-side aggressor trade** after acceptance estimates the opposite
  quote: a buy trade supplies an ask for our buy order; a sell trade supplies a
  bid for our sell order. If the limit crosses or equals that reference, GTC
  executes at the **reference trade price as taker**, and GTX expires. There is
  no need to wait for both sides. A noncrossing first reference completes this
  check; later same-side trades cannot turn the resting order into a taker.
- **Both GTC and GTX wait for the first same-side reference before any maker
  execution.** Opposing-aggressor trades received while waiting neither fill
  the order nor consume its queue ahead. After a noncrossing reference, only
  opposing trades with exchange timestamps **strictly later than the reference**
  can fill as maker at the **order limit price**. A sell trade can fill a buy order
  at or above its price; a buy trade can fill a sell order at or below its price.
  Same-price trades must also exhaust the queue ahead.
- New and repriced orders cannot use trades with timestamps at or before their
  exchange acceptance time. Quantity reductions preserve queue priority and
  eligibility and the reference-check state; increases and repricing reinitialize
  them. Trades at the acceptance timestamp do not consume the first reference.
  Trades at the reference timestamp cannot execute as maker or advance the queue.
  Reference confirmation preserves the original acceptance time used for priority.
- For example, a buy limit at 100 followed first by an aggressor buy at 99
  executes at 99 as taker (or expires for GTX). An aggressor sell at 98 arriving
  before that reference is ignored. If the first aggressor buy is instead 101,
  the order rests; a strictly later aggressor sell at 98 can fill it at 100 as maker.
- Fills and GTX expiry use the **triggering trade's exchange timestamp**, then
  the configured response latency. Nothing is backdated to order acceptance,
  and the strategy receives no future trade information. Within one trade event,
  maker candidates are processed before reference checks, each in price/time/id
  order. The cached best bid/ask never supplies an execution price.
- Book updates initialize the book, but never trigger fills or advance an
  existing order's queue. Initial queue depth still comes from the book.
- The no-partial-fill assumption is retained: an eligible fill executes the
  whole remaining order, without sharing a finite trade-volume budget across
  simulated orders. This mode is not an exact market-impact simulation.
- Order entry, cancel, modify and response delays still use the latency model.
  An order can fill while its cancellation is in flight. A cancellation that has
  already reached the exchange removes the order and its pending reference check.
  If no reference or eligible opposing trade arrives, the order remains unfilled;
  there is no fallback to a cached quote.

Only **GTC and GTX limit orders** are supported. Market, IOC and FOK requests
return `BacktestError::InvalidOrderRequest`. GTX is initially acknowledged and
can expire later on its first reference; the recorded book cannot reject it.

## Fees and remaining book dependence

Fees follow the simulated execution: first-reference fills pay taker fees;
opposing-trade fills pay maker fees. The recorded BBO does not classify fees.

This is still an approximation: the first same-side trade need not represent
the best quote at acceptance and may arrive later. It supplies a price, not
available depth. Initial queue size still comes from the recorded book, and
the no-partial-fill model does not constrain taker quantity to that trade's
quantity. This mode does not reconstruct the exact historical BBO.
