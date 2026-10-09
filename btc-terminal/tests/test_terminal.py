import asyncio
from datetime import datetime, timezone
from decimal import Decimal as D
import time
import unittest
from unittest.mock import patch

from textual.widgets import DataTable

from btc_terminal.app import BTCTerminal
from btc_terminal.feed import GeminiFeed
from btc_terminal.market import BookGap, Contract, MarketState, OrderBook, select_contract


def frame(first, last, bids=(), asks=()):
    return {"e": "depthUpdate", "s": "gemi-btc05m-test-up", "U": first, "u": last,
            "b": list(bids), "a": list(asks)}


def state_fixture():
    state = MarketState()
    now = time.time()
    state.switch(Contract("BTC05M-TEST", "GEMI-BTC05M-TEST-UP", now - 100, now + 200,
                          D("100000"), "Example", "TEST-BTC"))
    state.connected = state.acknowledged = True
    state.book.apply(frame(10, 10,
        [(str(D("0.49") - D(i) / 100), str(10 + i)) for i in range(40)],
        [(str(D("0.51") + D(i) / 100), str(15 + i)) for i in range(40)]))
    state.ingest({"e": "thirdPartyPrice", "I": "TEST-BTC", "p": "100025", "E": 1, "u": 1})
    return state


class BookTests(unittest.TestCase):
    def test_snapshot_then_absolute_quantities_and_zero_removal(self):
        book = OrderBook()
        book.apply(frame(100, 100, [("0.48", "300.25"), ("0.47", "20")], [("0.52", "15")]))
        book.apply(frame(100, 108, [("0.48", "1.25"), ("0.47", "0")], [("0.53", "8")]))
        self.assertEqual(book.bids, {D("0.48"): D("1.25")})
        self.assertEqual(book.levels()[1], [(D("0.52"), D(15)), (D("0.53"), D(8))])

    def test_duplicate_does_not_roll_book_back(self):
        book = OrderBook()
        book.apply(frame(100, 105, [("0.48", "10")]))
        self.assertFalse(book.apply(frame(100, 104, [("0.48", "999")])))
        self.assertEqual(book.bids[D("0.48")], D(10))

    def test_gap_clears_book_before_next_snapshot(self):
        book = OrderBook()
        book.apply(frame(100, 105, [("0.48", "10")]))
        with self.assertRaises(BookGap):
            book.apply(frame(108, 110, [("0.49", "30")]))
        self.assertEqual(book.bids, {})
        self.assertIsNone(book.sequence)
        book.apply(frame(200, 200, [("0.50", "4")]))
        self.assertEqual(book.bids, {D("0.50"): D(4)})

    def test_down_complement_reverses_sides_and_preserves_size(self):
        book = OrderBook()
        book.apply(frame(1, 1, [("0.30", "7.25"), ("0.20", "9")], [("0.40", "12")]))
        self.assertEqual(book.levels(True), ([(D("0.60"), D(12))],
                                          [(D("0.70"), D("7.25")), (D("0.80"), D(9))]))

    def test_current_round_preferred_and_rollover_at_exact_expiry(self):
        iso = lambda t: datetime.fromtimestamp(t, timezone.utc).isoformat()
        def event(start, end):
            return {"series": "BTC05M", "ticker": str(start), "sourceDetails": {"agency": "A", "index": "I"},
                    "contracts": [{"ticker": "UP", "instrumentSymbol": str(start), "effectiveDate": iso(start),
                                   "expiryDate": iso(end), "strike": {"availableAt": iso(start)}}]}
        events = [event(600, 900), event(0, 300), event(300, 600)]
        self.assertEqual(select_contract(events, 299.99).symbol, "0")
        self.assertEqual(select_contract(events, 300).symbol, "300")
        self.assertIsNone(select_contract(events, 901))

    def test_round_change_clears_previous_round_data(self):
        state = state_fixture()
        contract = Contract("NEXT", "NEXT-UP", time.time(), time.time() + 300, None, "A", "I")
        state.switch(contract)
        self.assertIsNone(state.reference)
        self.assertIsNone(state.book.sequence)
        self.assertEqual(len(state.history), 0)

    def test_trade_deduplication_and_official_strike_update(self):
        state = state_fixture()
        trade = {"s": state.contract.symbol.lower(), "t": 123, "E": 1789972506407058382,
                 "p": "0.48", "q": "3.25", "m": True}
        state.ingest(trade)
        state.ingest(trade)
        self.assertEqual(len(state.trades), 1)
        state.ingest({"e": "contractStatus", "s": state.contract.symbol, "p": "99999.5", "n": "Active"})
        self.assertEqual(state.contract.strike, D("99999.5"))


class UITests(unittest.IsolatedAsyncioTestCase):
    async def test_all_levels_scroll_pause_resume_and_outcome(self):
        state = state_fixture()
        app = BTCTerminal(state, live=False)
        async with app.run_test(size=(160, 44)) as pilot:
            await pilot.pause()
            table = app.query_one("#book", DataTable)
            self.assertEqual(table.row_count, 40)
            await pilot.press("pagedown")
            await pilot.pause()
            position = table.scroll_y
            self.assertGreater(position, 0)
            state.book.apply(frame(10, 11, [("0.49", "123")]))
            state.revision += 1
            app.paint_market()
            await pilot.pause()
            self.assertEqual(table.scroll_y, position)
            await pilot.press("space")
            frozen_bid = app.frozen.book.bids[D("0.49")]
            state.book.apply(frame(11, 12, [("0.49", "456")]))
            self.assertEqual(app.frozen.book.bids[D("0.49")], frozen_bid)
            await pilot.press("d")
            self.assertTrue(app.down)
            await pilot.press("space", "b")
            self.assertIsNone(app.frozen)
            self.assertEqual(table.scroll_y, 0)
            await pilot.resize_terminal(80, 24)
            await pilot.pause()
            self.assertTrue(app.compact)
            self.assertTrue(app.screen.has_class("narrow"))
            self.assertEqual(len(table.columns), 6)

    async def test_small_terminal_tape_toggle_and_expired_quotes_hidden(self):
        state = state_fixture()
        app = BTCTerminal(state, live=False)
        async with app.run_test(size=(80, 24)) as pilot:
            await pilot.pause()
            self.assertTrue(app.compact)
            await pilot.press("t")
            await pilot.pause()
            self.assertFalse(app.query_one("#book-pane").display)
            await pilot.press("t")
            self.assertTrue(app.query_one("#book-pane").display)
            state.contract.expiry = time.time() - 1
            app.paint_market()
            self.assertEqual(app.query_one("#book", DataTable).row_count, 0)

    async def test_discovery_worker_switches_without_reusing_old_book(self):
        state = MarketState()
        feed = GeminiFeed(state)
        contracts = [Contract("A", "A-UP", time.time() - 1, time.time() + 300, None, "A", "I"),
                     Contract("B", "B-UP", time.time() - 1, time.time() + 300, None, "A", "I")]
        calls = []
        async def discover(client):
            return []
        async def stream():
            calls.append(state.contract.symbol)
            state.book.apply(frame(1, 1, [("0.5", "20")]))
            await asyncio.Event().wait()
        feed.discover, feed.stream = discover, stream
        with patch("btc_terminal.feed.select_contract", side_effect=contracts):
            worker = asyncio.create_task(feed.run())
            try:
                for _ in range(100):
                    if calls:
                        break
                    await asyncio.sleep(.01)
                feed.wakeup.set()
                for _ in range(100):
                    if len(calls) == 2:
                        break
                    await asyncio.sleep(.01)
                self.assertEqual(calls, ["A-UP", "B-UP"])
                self.assertEqual(state.contract.symbol, "B-UP")
            finally:
                worker.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await worker


if __name__ == "__main__":
    unittest.main()
