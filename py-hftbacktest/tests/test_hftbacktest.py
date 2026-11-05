import unittest
import numpy as np

from numba import njit

from hftbacktest import (
    BacktestAsset,
    HashMapMarketDepthBacktest,
    ALL_ASSETS, ROIVectorMarketDepthBacktest
)
from hftbacktest.data.validation import correct_event_order, validate_event_order
from hftbacktest.types import EXCH_EVENT, LOCAL_EVENT, event_dtype


@njit
def test_run(hbt):
    order_id = 0
    while hbt.elapse(10_000_000_000) == 0:
        current_timestamp = hbt.current_timestamp
        depth = hbt.depth(0)
        best_bid = depth.best_bid
        best_ask = depth.best_ask

        # trades = hbt.last_trades(0)
        #
        # i = 0
        # for trade in trades:
        #     print(trade.local_ts, trade.px, trade.qty)
        #     i += 1
        #     if i > 5:
        #         break

        hbt.clear_last_trades(ALL_ASSETS)

        cnt = 0
        orders = hbt.orders(0)
        values = orders.values()
        while True:
            order = values.next()
            if order is None:
                break
            cnt += 1
            print(order.order_id, order.side, order.price_tick, order.qty)

        hbt.clear_inactive_orders(ALL_ASSETS)

        if cnt <= 2:
            hbt.submit_buy_order(0, order_id, best_bid, 1, 1, 0, False)
            order_id += 1
            hbt.submit_sell_order(0, order_id, best_ask, 1, 1, 0, False)
            order_id += 1

        print(current_timestamp, best_bid, best_ask)


class TestPyHftBacktest(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def test_correct_event_order_boundaries(self):
        checked = njit(boundscheck=True)(correct_event_order.py_func)
        both = EXCH_EVENT | LOCAL_EVENT
        cases = (
            ('empty', [], [], []),
            ('single', [(1, 10)], [0], [both]),
            ('aligned', [(2, 20), (1, 10)], [1, 0], [both, both]),
            ('duplicate', [(1, 10), (1, 10)], [0, 1], [both, both]),
            ('exchange_exhausted', [(1, 30), (2, 20), (3, 10)],
             [0, 1, 2, 1, 0], [EXCH_EVENT, EXCH_EVENT, both, LOCAL_EVENT, LOCAL_EVENT]),
            ('local_exhausted', [(1, 30), (1, 20), (1, 10)],
             [2, 1, 0, 1, 2], [LOCAL_EVENT, LOCAL_EVENT, both, EXCH_EVENT, EXCH_EVENT]),
        )
        for name, timestamps, rows, flags in cases:
            data = np.zeros(len(timestamps), event_dtype)
            data['ev'] = 1
            data['px'] = np.nan
            data['qty'] = 1
            data['order_id'] = np.arange(len(data))
            for row, (exch_ts, local_ts) in enumerate(timestamps):
                data[row]['exch_ts'] = exch_ts
                data[row]['local_ts'] = local_ts
            original = data.copy()
            expected = data[np.array(rows, dtype=np.int64)].copy()
            expected['ev'] |= np.array(flags, dtype=np.uint64)
            for mode, correct in (('default', correct_event_order), ('boundscheck', checked)):
                with self.subTest(case=name, mode=mode):
                    result = correct(
                        data,
                        np.argsort(data['exch_ts'], kind='mergesort'),
                        np.argsort(data['local_ts'], kind='mergesort'),
                    )
                    self.assertEqual(result.dtype, event_dtype)
                    for field in event_dtype.names:
                        np.testing.assert_array_equal(result[field], expected[field])
                        np.testing.assert_array_equal(data[field], original[field])
                    validate_event_order(result)

    def test_run_backtest(self):
        arr = np.load('tmp_20240501.npz')['data']

        asset = (
            BacktestAsset()
                .linear_asset(1.0)
                .data(['tmp_20240501.npz'])
                .no_partial_fill_exchange()
                .constant_latency(100, 100)
                .power_prob_queue_model3(3.0)
                .tick_size(0.000001)
                .lot_size(1.0)
                .trade_len(1000)
                .roi_lb(0.0)
                .roi_ub(1.0)
        )

        # hbt = HashMapMarketDepthMultiAssetMultiExchangeBacktest([asset])
        hbt = ROIVectorMarketDepthBacktest([asset])
        test_run(hbt)
