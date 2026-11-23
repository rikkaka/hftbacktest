use super::*;
use crate::{
    backtest::{
        Backtest, ExchangeKind, L2AssetBuilder,
        assettype::LinearAsset,
        data::{Data, DataSource},
        models::{CommonFees, ConstantLatency, RiskAdverseQueueModel, TradingValueFeeModel},
        order::order_bus,
    },
    depth::HashMapMarketDepth,
    prelude::{Bot, LOCAL_EVENT},
};

type Exchange = NoPartialFillExchange<
    LinearAsset,
    ConstantLatency,
    RiskAdverseQueueModel<HashMapMarketDepth>,
    HashMapMarketDepth,
    TradingValueFeeModel<CommonFees>,
>;

fn event(kind: u64, ts: i64, px: f64, qty: f64) -> Event {
    Event {
        ev: kind,
        exch_ts: ts,
        local_ts: ts,
        px,
        qty,
        order_id: 0,
        ival: 0,
        fval: 0.0,
    }
}

fn exchange(mode: MatchingMode) -> Exchange {
    let (bus, _) = order_bus(ConstantLatency::new(0, 0));
    let mut exchange = NoPartialFillExchange::new(
        HashMapMarketDepth::new(1.0, 1.0),
        State::new(
            LinearAsset::new(1.0),
            TradingValueFeeModel::new(CommonFees::new(0.0, 0.001)),
        ),
        RiskAdverseQueueModel::new(),
        bus,
    )
    .matching_mode(mode);
    exchange
        .process(&event(EXCH_BID_DEPTH_EVENT, 0, 99.0, 10.0))
        .unwrap();
    exchange
        .process(&event(EXCH_ASK_DEPTH_EVENT, 0, 101.0, 10.0))
        .unwrap();
    exchange
}

fn order(id: u64, side: Side, price: i64, qty: f64) -> Order {
    Order::new(id, price, 1.0, qty, side, OrdType::Limit, TimeInForce::GTC)
}

#[test]
fn default_keeps_immediate_and_book_update_execution() {
    let mut ex = exchange(MatchingMode::default());
    ex.ack_new(&mut order(1, Side::Buy, 102, 2.0), 1).unwrap();
    assert_eq!(ex.state.values().position, 2.0);
    assert_eq!(ex.state.values().balance, -202.0);
    assert_eq!(ex.state.values().fee, 0.202);
    ex.ack_new(&mut order(2, Side::Sell, 103, 1.0), 2).unwrap();
    ex.process(&event(EXCH_BID_DEPTH_EVENT, 3, 104.0, 10.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 1.0);
    assert_eq!(ex.state.values().balance, -99.0);
}

#[test]
fn crossed_orders_wait_for_later_opposing_trade_at_limit_and_keep_fee_class() {
    for (side, price, wrong_trade, right_trade, trade_price) in [
        (
            Side::Buy,
            102,
            EXCH_BUY_TRADE_EVENT,
            EXCH_SELL_TRADE_EVENT,
            100.0,
        ),
        (
            Side::Sell,
            98,
            EXCH_SELL_TRADE_EVENT,
            EXCH_BUY_TRADE_EVENT,
            100.0,
        ),
    ] {
        let mut ex = exchange(MatchingMode::TradeOnly);
        ex.ack_new(&mut order(1, side, price, 2.0), 10).unwrap();
        ex.process(&event(right_trade, 10, trade_price, 1.0))
            .unwrap();
        ex.process(&event(wrong_trade, 11, trade_price, 1.0))
            .unwrap();
        ex.process(&event(EXCH_BID_DEPTH_EVENT, 12, 103.0, 10.0))
            .unwrap();
        ex.process(&event(EXCH_ASK_DEPTH_EVENT, 13, 97.0, 10.0))
            .unwrap();
        assert_eq!(ex.state.values().num_trades, 0);
        ex.process(&event(right_trade, 14, trade_price, 1.0))
            .unwrap();
        assert_eq!(ex.state.values().trading_volume, 2.0);
        assert_eq!(ex.state.values().trading_value, price as f64 * 2.0);
        assert_eq!(ex.state.values().fee, price as f64 * 2.0 * 0.001);
        assert!(ex.trade_only_orders.is_empty());
    }
}

#[test]
fn same_price_queue_only_advances_on_trades() {
    for (side, price, depth, trade) in [
        (Side::Buy, 99, EXCH_BID_DEPTH_EVENT, EXCH_SELL_TRADE_EVENT),
        (Side::Sell, 101, EXCH_ASK_DEPTH_EVENT, EXCH_BUY_TRADE_EVENT),
    ] {
        let mut ex = exchange(MatchingMode::TradeOnly);
        ex.ack_new(&mut order(1, side, price, 3.0), 1).unwrap();
        ex.process(&event(depth, 2, price as f64, 0.0)).unwrap();
        ex.process(&event(trade, 3, price as f64, 10.0)).unwrap();
        assert_eq!(ex.state.values().num_trades, 0);
        ex.process(&event(trade, 4, price as f64, 1.0)).unwrap();
        assert_eq!(ex.state.values().trading_volume, 3.0);
        assert_eq!(ex.state.values().fee, 0.0);
    }
}

#[test]
fn stale_book_cannot_bound_the_trade_order_scan() {
    for (side, price, trade, px) in [
        (Side::Sell, 90, EXCH_BUY_TRADE_EVENT, 91.0),
        (Side::Buy, 110, EXCH_SELL_TRADE_EVENT, 109.0),
    ] {
        let mut ex = exchange(MatchingMode::TradeOnly);
        ex.ack_new(&mut order(1, side, price, 1.0), 1).unwrap();
        ex.process(&event(trade, 2, px, 1.0)).unwrap();
        assert_eq!(ex.state.values().num_trades, 1);
    }
}

#[test]
fn reduction_preserves_queue_and_activation_but_increase_or_reprice_requeues() {
    let mut ex = exchange(MatchingMode::TradeOnly);
    let mut buy = order(1, Side::Buy, 99, 5.0);
    ex.ack_new(&mut buy, 1).unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 2, 99.0, 6.0))
        .unwrap();
    buy.qty = 3.0;
    ex.ack_modify::<false>(&mut buy, 3).unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 3, 99.0, 5.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 3.0);

    let mut buy = order(2, Side::Buy, 99, 1.0);
    ex.ack_new(&mut buy, 4).unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 5, 99.0, 6.0))
        .unwrap();
    buy.qty = 4.0;
    ex.ack_modify::<false>(&mut buy, 6).unwrap();
    assert_eq!(buy.leaves_qty, 4.0);
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 6, 99.0, 11.0))
        .unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 7, 99.0, 5.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 3.0);
    buy.price_tick = 102;
    ex.ack_modify::<false>(&mut buy, 8).unwrap();
    assert_eq!(ex.state.values().position, 3.0);
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 8, 100.0, 10.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 3.0);
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 9, 100.0, 1.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 7.0);
    assert!((ex.state.values().fee - 0.408).abs() < 1e-12);

    let mut buy = order(3, Side::Buy, 99, 1.0);
    ex.ack_new(&mut buy, 10).unwrap();
    ex.ack_cancel(&mut buy, 11).unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 12, 98.0, 100.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 7.0);
    assert!(ex.trade_only_orders.is_empty());
}

#[test]
fn rejects_unsupported_orders_and_retains_post_only_rejection() {
    let mut ex = exchange(MatchingMode::TradeOnly);
    for (kind, tif) in [
        (OrdType::Market, TimeInForce::GTC),
        (OrdType::Limit, TimeInForce::IOC),
        (OrdType::Limit, TimeInForce::FOK),
    ] {
        let mut buy = order(1, Side::Buy, 102, 1.0);
        buy.order_type = kind;
        buy.time_in_force = tif;
        assert!(matches!(
            ex.ack_new(&mut buy, 1),
            Err(BacktestError::InvalidOrderRequest)
        ));
    }
    let mut buy = order(1, Side::Buy, 102, 1.0);
    buy.time_in_force = TimeInForce::GTX;
    ex.ack_new(&mut buy, 1).unwrap();
    assert_eq!(buy.status, Status::Expired);
    assert!(ex.orders.borrow().is_empty());
}

#[test]
fn zec_aug22_stale_bid_cannot_trigger_take_profit() {
    let mut ex = exchange(MatchingMode::TradeOnly);
    // Prices in cents: the recorded E-time bid was 741.25 while trades around the TP were lower.
    ex.process(&event(EXCH_BID_DEPTH_EVENT, 0, 74125.0, 10.0))
        .unwrap();
    ex.process(&event(EXCH_ASK_DEPTH_EVENT, 0, 74649.0, 10.0))
        .unwrap();
    ex.ack_new(&mut order(1, Side::Sell, 73600, 1.0), 1)
        .unwrap();
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 2, 73063.0, 1.0))
        .unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 2, 72848.0, 1.0))
        .unwrap();
    ex.process(&event(EXCH_BID_DEPTH_EVENT, 3, 74125.0, 20.0))
        .unwrap();
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 4, 72521.0, 1.0))
        .unwrap();
    assert_eq!(ex.state.values().num_trades, 0);
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 5, 73700.0, 1.0))
        .unwrap();
    assert_eq!(ex.state.values().balance, 73600.0);
}

#[test]
fn builder_and_order_bus_preserve_entry_response_and_cancel_latencies() {
    for response in [20, 50] {
        let data = Data::from_data(&[
            event(EXCH_BID_DEPTH_EVENT | LOCAL_EVENT, 0, 99.0, 10.0),
            event(EXCH_ASK_DEPTH_EVENT | LOCAL_EVENT, 0, 101.0, 10.0),
            event(EXCH_SELL_TRADE_EVENT | LOCAL_EVENT, 100, 100.0, 10.0),
            event(EXCH_SELL_TRADE_EVENT | LOCAL_EVENT, 101, 100.0, 10.0),
            event(EXCH_BUY_TRADE_EVENT | LOCAL_EVENT, 300, 103.0, 10.0),
            event(EXCH_BUY_TRADE_EVENT | LOCAL_EVENT, 500, 103.0, 10.0),
            event(EXCH_BUY_TRADE_EVENT | LOCAL_EVENT, 1000, 103.0, 10.0),
        ]);
        let asset = || {
            L2AssetBuilder::new()
                .data(vec![DataSource::Data(data.clone())])
                .latency_model(ConstantLatency::new(100, response))
                .asset_type(LinearAsset::new(1.0))
                .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.001)))
                .queue_model(RiskAdverseQueueModel::new())
                .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                .matching_mode(MatchingMode::TradeOnly)
        };
        assert!(
            asset()
                .exchange(ExchangeKind::PartialFillExchange)
                .build()
                .is_err()
        );
        let mut hbt = Backtest::builder()
            .add_asset(asset().build().unwrap())
            .build()
            .unwrap();
        hbt.elapse(0).unwrap();
        hbt.submit_buy_order(0, 1, 102.0, 1.0, TimeInForce::GTC, OrdType::Limit, false)
            .unwrap();
        hbt.elapse(100 + response).unwrap();
        assert_eq!(hbt.position(0), 0.0);
        hbt.elapse(1).unwrap();
        assert_eq!(hbt.position(0), 1.0);
        let buy = &hbt.orders(0)[&1];
        assert_eq!(buy.exch_timestamp, 101);
        assert_eq!(buy.exec_price(), 102.0);
        assert!(!buy.maker);
        hbt.submit_sell_order(0, 2, 102.0, 1.0, TimeInForce::GTC, OrdType::Limit, true)
            .unwrap();
        hbt.cancel(0, 2, false).unwrap();
        // The trade at 300 happens while cancellation is still in flight.
        hbt.elapse(300 + response - hbt.current_timestamp())
            .unwrap();
        assert_eq!(hbt.position(0), 0.0);
        hbt.elapse(200).unwrap();
        hbt.submit_sell_order(0, 3, 102.0, 1.0, TimeInForce::GTC, OrdType::Limit, true)
            .unwrap();
        hbt.cancel(0, 3, true).unwrap();
        hbt.elapse(1000 + response - hbt.current_timestamp())
            .unwrap();
        assert_eq!(hbt.orders(0)[&3].status, Status::Canceled);
        assert_eq!(hbt.position(0), 0.0);
    }
}
