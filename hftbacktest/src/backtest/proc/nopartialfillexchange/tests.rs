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
fn first_same_side_trade_takes_at_reference_price_and_time() {
    for (side, reference_trade, opposing_trade, reference_price) in [
        (Side::Buy, EXCH_BUY_TRADE_EVENT, EXCH_SELL_TRADE_EVENT, 99.0),
        (
            Side::Buy,
            EXCH_BUY_TRADE_EVENT,
            EXCH_SELL_TRADE_EVENT,
            100.0,
        ),
        (
            Side::Sell,
            EXCH_SELL_TRADE_EVENT,
            EXCH_BUY_TRADE_EVENT,
            101.0,
        ),
        (
            Side::Sell,
            EXCH_SELL_TRADE_EVENT,
            EXCH_BUY_TRADE_EVENT,
            100.0,
        ),
    ] {
        let mut ex = exchange(MatchingMode::TradeOnly);
        // The old BBO says this order is passive; the first reference says it takes.
        ex.ack_new(&mut order(1, side, 100, 2.0), 10).unwrap();
        ex.process(&event(reference_trade, 9, reference_price, 1.0))
            .unwrap();
        ex.process(&event(reference_trade, 10, reference_price, 1.0))
            .unwrap();
        ex.process(&event(opposing_trade, 10, 100.0, 1.0)).unwrap();
        ex.process(&event(EXCH_BID_DEPTH_EVENT, 12, 103.0, 10.0))
            .unwrap();
        ex.process(&event(EXCH_ASK_DEPTH_EVENT, 13, 97.0, 10.0))
            .unwrap();
        assert_eq!(ex.state.values().num_trades, 0);
        assert!(ex.trade_only_orders[&1].awaiting_reference);
        ex.process(&event(reference_trade, 14, reference_price, 1.0))
            .unwrap();
        assert_eq!(ex.state.values().trading_volume, 2.0);
        assert_eq!(ex.state.values().trading_value, reference_price * 2.0);
        assert_eq!(ex.state.values().fee, reference_price * 2.0 * 0.001);
        assert_eq!(ex.order_e2l.earliest_send_order_timestamp(), Some(14));
        assert!(ex.trade_only_orders.is_empty());
        assert!(ex.orders.borrow().is_empty());
        ex.process(&event(opposing_trade, 15, 100.0, 10.0)).unwrap();
        assert_eq!(ex.state.values().num_trades, 1);
    }
}

#[test]
fn noncrossing_first_reference_cannot_be_replaced_by_later_trades() {
    for (side, reference_trade, opposing_trade, first, later) in [
        (
            Side::Buy,
            EXCH_BUY_TRADE_EVENT,
            EXCH_SELL_TRADE_EVENT,
            103.0,
            101.0,
        ),
        (
            Side::Sell,
            EXCH_SELL_TRADE_EVENT,
            EXCH_BUY_TRADE_EVENT,
            97.0,
            99.0,
        ),
    ] {
        for tif in [TimeInForce::GTC, TimeInForce::GTX] {
            let mut ex = exchange(MatchingMode::TradeOnly);
            let limit = if side == Side::Buy { 102 } else { 98 };
            let mut resting = order(1, side, limit, 1.0);
            resting.time_in_force = tif;
            // Even GTX is not rejected using this crossed, stale BBO.
            ex.ack_new(&mut resting, 1).unwrap();
            assert_eq!(resting.status, Status::New);
            ex.process(&event(reference_trade, 2, first, 1.0)).unwrap();
            assert!(!ex.trade_only_orders[&1].awaiting_reference);
            ex.process(&event(reference_trade, 3, later, 1.0)).unwrap();
            assert_eq!(ex.state.values().num_trades, 0);
            ex.process(&event(opposing_trade, 4, later, 1.0)).unwrap();
            assert_eq!(ex.state.values().trading_value, limit as f64);
            assert_eq!(ex.state.values().fee, 0.0);
            assert!(ex.orders.borrow().is_empty());
        }
    }
}

#[test]
fn one_trade_handles_makers_and_first_references_in_price_time_order() {
    let mut ex = exchange(MatchingMode::TradeOnly);
    let (bus, mut local) = order_bus(ConstantLatency::new(0, 0));
    ex.order_e2l = bus;
    for (id, side, price) in [
        (2, Side::Buy, 100),
        (4, Side::Buy, 98),
        (3, Side::Sell, 98),
        (1, Side::Buy, 102),
    ] {
        ex.ack_new(&mut order(id, side, price, 1.0), 1).unwrap();
    }
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 2, 99.0, 1.0))
        .unwrap();
    for (id, maker, px) in [(3, true, 98.0), (1, false, 99.0), (2, false, 99.0)] {
        let filled = local.receive(2).unwrap();
        assert_eq!(filled.order_id, id);
        assert_eq!(filled.maker, maker);
        assert_eq!(filled.exec_price(), px);
        assert_eq!(filled.status, Status::Filled);
    }
    assert!(local.earliest_recv_order_timestamp().is_none());
    assert_eq!(ex.orders.borrow().len(), 1);
    assert!(!ex.trade_only_orders[&4].awaiting_reference);
    // A newly accepted order gets its own first reference; the resting order does not.
    ex.ack_new(&mut order(5, Side::Buy, 98, 1.0), 3).unwrap();
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 4, 97.0, 1.0))
        .unwrap();
    let filled = local.receive(4).unwrap();
    assert_eq!(filled.order_id, 5);
    assert!(!filled.maker);
    assert_eq!(filled.exec_price(), 97.0);
    assert!(ex.orders.borrow().contains_key(&4));
    assert_eq!(ex.state.values().num_trades, 4);
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
fn opposing_trade_can_fill_as_maker_before_reference_despite_crossed_book() {
    for (side, price, trade, px) in [
        (Side::Sell, 90, EXCH_BUY_TRADE_EVENT, 91.0),
        (Side::Buy, 110, EXCH_SELL_TRADE_EVENT, 109.0),
    ] {
        let mut ex = exchange(MatchingMode::TradeOnly);
        ex.ack_new(&mut order(1, side, price, 1.0), 1).unwrap();
        ex.process(&event(trade, 2, px, 1.0)).unwrap();
        assert_eq!(ex.state.values().num_trades, 1);
        assert_eq!(ex.state.values().trading_value, price as f64);
        assert_eq!(ex.state.values().fee, 0.0);
        let reference = if side == Side::Buy {
            EXCH_BUY_TRADE_EVENT
        } else {
            EXCH_SELL_TRADE_EVENT
        };
        ex.process(&event(reference, 3, px, 1.0)).unwrap();
        assert_eq!(ex.state.values().num_trades, 1);
    }
}

#[test]
fn reduction_preserves_queue_and_activation_but_increase_or_reprice_requeues() {
    let mut ex = exchange(MatchingMode::TradeOnly);
    let mut buy = order(1, Side::Buy, 99, 5.0);
    ex.ack_new(&mut buy, 1).unwrap();
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 2, 101.0, 1.0))
        .unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 2, 99.0, 6.0))
        .unwrap();
    buy.qty = 3.0;
    ex.ack_modify::<false>(&mut buy, 3).unwrap();
    assert_eq!(ex.trade_only_orders[&1].active_after, 1);
    assert!(!ex.trade_only_orders[&1].awaiting_reference);
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 3, 98.0, 1.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 0.0);
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 3, 99.0, 5.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 3.0);

    let mut buy = order(2, Side::Buy, 99, 1.0);
    ex.ack_new(&mut buy, 4).unwrap();
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 5, 101.0, 1.0))
        .unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 5, 99.0, 6.0))
        .unwrap();
    buy.qty = 4.0;
    ex.ack_modify::<false>(&mut buy, 6).unwrap();
    assert_eq!(buy.leaves_qty, 4.0);
    assert_eq!(ex.trade_only_orders[&2].active_after, 6);
    assert!(ex.trade_only_orders[&2].awaiting_reference);
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 6, 98.0, 1.0))
        .unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 6, 99.0, 11.0))
        .unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 7, 99.0, 5.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 3.0);
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 7, 101.0, 1.0))
        .unwrap();
    assert!(!ex.trade_only_orders[&2].awaiting_reference);
    buy.price_tick = 102;
    ex.ack_modify::<false>(&mut buy, 8).unwrap();
    assert!(ex.trade_only_orders[&2].awaiting_reference);
    assert_eq!(ex.state.values().position, 3.0);
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 8, 100.0, 10.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 3.0);
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 8, 100.0, 1.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 3.0);
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 9, 100.0, 1.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 7.0);
    assert!((ex.state.values().fee - 0.400).abs() < 1e-12);

    let mut buy = order(3, Side::Buy, 99, 1.0);
    ex.ack_new(&mut buy, 10).unwrap();
    ex.ack_cancel(&mut buy, 11).unwrap();
    ex.process(&event(EXCH_BUY_TRADE_EVENT, 12, 98.0, 100.0))
        .unwrap();
    ex.process(&event(EXCH_SELL_TRADE_EVENT, 12, 98.0, 100.0))
        .unwrap();
    assert_eq!(ex.state.values().position, 7.0);
    assert!(ex.trade_only_orders.is_empty());
}

#[test]
fn rejects_unsupported_orders_and_expires_post_only_on_first_reference() {
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
    for (side, trade, px) in [
        (Side::Buy, EXCH_BUY_TRADE_EVENT, 100.0),
        (Side::Sell, EXCH_SELL_TRADE_EVENT, 100.0),
    ] {
        let mut ex = exchange(MatchingMode::TradeOnly);
        let (bus, mut local) = order_bus(ConstantLatency::new(0, 20));
        ex.order_e2l = bus;
        let mut post_only = order(1, side, 100, 1.0);
        post_only.time_in_force = TimeInForce::GTX;
        ex.ack_new(&mut post_only, 1).unwrap();
        assert_eq!(post_only.status, Status::New);
        ex.process(&event(trade, 1, px, 1.0)).unwrap();
        assert!(ex.trade_only_orders[&1].awaiting_reference);
        ex.process(&event(trade, 2, px, 1.0)).unwrap();
        assert!(ex.orders.borrow().is_empty());
        assert!(ex.trade_only_orders.is_empty());
        assert_eq!(ex.state.values().num_trades, 0);
        assert_eq!(ex.order_e2l.earliest_send_order_timestamp(), Some(22));
        let expired = local.receive(22).unwrap();
        assert_eq!(expired.status, Status::Expired);
        assert_eq!(expired.exch_timestamp, 2);
        assert_eq!(expired.exec_qty, 0.0);
    }
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
    assert_eq!(ex.state.values().fee, 0.0);
}

#[test]
fn builder_and_order_bus_preserve_entry_response_and_cancel_latencies() {
    for response in [20, 50] {
        for first_trade in [EXCH_BUY_TRADE_EVENT, EXCH_SELL_TRADE_EVENT] {
            let data = Data::from_data(&[
                event(EXCH_BID_DEPTH_EVENT | LOCAL_EVENT, 0, 99.0, 10.0),
                event(EXCH_ASK_DEPTH_EVENT | LOCAL_EVENT, 0, 101.0, 10.0),
                event(first_trade | LOCAL_EVENT, 100, 100.0, 10.0),
                event(first_trade | LOCAL_EVENT, 101, 100.0, 10.0),
                event(first_trade | LOCAL_EVENT, 300, 103.0, 10.0),
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
            let maker = first_trade == EXCH_SELL_TRADE_EVENT;
            assert_eq!(buy.exec_price(), if maker { 102.0 } else { 100.0 });
            assert_eq!(buy.maker, maker);
            hbt.submit_sell_order(0, 2, 102.0, 1.0, TimeInForce::GTC, OrdType::Limit, true)
                .unwrap();
            hbt.cancel(0, 2, false).unwrap();
            // The trade at 300 happens while cancellation is still in flight.
            hbt.elapse(300 + response - hbt.current_timestamp())
                .unwrap();
            assert_eq!(hbt.position(0), 0.0);
            let sell = &hbt.orders(0)[&2];
            assert_eq!(sell.maker, !maker);
            assert_eq!(sell.exec_price(), if maker { 103.0 } else { 102.0 });
            assert_eq!(sell.exch_timestamp, 300);
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
}
