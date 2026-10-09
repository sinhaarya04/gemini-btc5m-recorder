from __future__ import annotations

import argparse
import asyncio
import contextlib
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import json
import math
import sys
import time

from rich.table import Table
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.events import Resize
from textual.widgets import DataTable, Footer, Sparkline, Static

from .feed import GeminiFeed
from .market import MarketState

GREEN = "#4cdda3"
RED = "#ff7189"
CYAN = "#66d4ed"
GRAY = "#8293aa"
WHITE = "#e6edf7"
ORANGE = "#ffb454"


def quantity(value: Decimal) -> str:
    return f"{value:,.2f}".rstrip("0").rstrip(".")


def cents(value: Decimal | None) -> str:
    return f"{value * 100:.2f}" if value is not None else "—"


def money(value: Decimal | None) -> str:
    return f"${value:,.2f}" if value is not None else "Unavailable"


def card(label: str, value: str, note: str, color: str = WHITE) -> Text:
    result = Text(label + "\n", style=GRAY, no_wrap=True, overflow="ellipsis")
    result.append(value + "\n", style=f"bold {color}")
    result.append(note, style=GRAY)
    return result


class BTCTerminal(App):
    TITLE = "BTC · Gemini market terminal"
    ENABLE_COMMAND_PALETTE = False
    CSS = """
    Screen { background: #080d14; color: #e6edf7; }
    #masthead { height: 3; padding: 1 2 0 2; background: #101923; }
    #metrics { height: 5; margin: 1 1 0 1; }
    .metric { width: 1fr; height: 5; padding: 0 1; margin: 0 1 0 0;
              border-left: tall #29374a; background: #101923;
              text-wrap: nowrap; text-overflow: ellipsis; }
    #body { height: 1fr; margin: 0 1; }
    #book-pane { width: 1fr; border: round #29374a; padding: 0 1; }
    #book-summary { height: 2; text-wrap: nowrap; text-overflow: ellipsis; }
    #book { height: 1fr; }
    #book-note { height: 1; color: #8293aa; text-wrap: nowrap; text-overflow: ellipsis; }
    #sidebar { width: 39; margin-left: 1; }
    #chart-pane { height: 10; border: round #29374a; padding: 0 1; }
    #chart-label { height: 2; }
    #reference-chart { height: 3; color: #66d4ed; background: #080d14; }
    #chart-range { height: 2; color: #8293aa; }
    #tape-pane { height: 1fr; border: round #29374a; padding: 0 1; }
    #tape-note { height: 2; color: #8293aa; }
    #tape { height: 1fr; }
    DataTable { background: #080d14; scrollbar-color: #344a64;
                scrollbar-background: #101923; }
    DataTable > .datatable--header { background: #172232; color: #8293aa; text-style: bold; }
    DataTable > .datatable--cursor { background: #203247; color: #e6edf7; }
    DataTable > .datatable--even-row { background: #0d1520; }
    DataTable > .datatable--odd-row { background: #080d14; }
    DataTable:focus { border: none; }
    #connection { height: 2; padding: 0 2; color: #8293aa;
                  text-wrap: nowrap; text-overflow: ellipsis; }
    Footer { background: #172232; color: #8293aa; }
    Footer > .footer-key--key { background: #29374a; color: #e6edf7; }
    Footer > .footer-key--description { color: #c0ccdb; }
    .narrow #sidebar { display: none; }
    .short #metrics { height: 4; margin-top: 0; }
    .short .metric { height: 4; }
    .short #masthead { height: 2; padding-top: 0; }
    """
    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("space", "freeze", "Freeze", key_display="Space"),
        Binding("d", "outcome", "UP / DOWN"),
        Binding("b", "top", "Best prices"),
        Binding("r", "reconnect", "Reconnect"),
        Binding("t", "tape", "Trade tape"),
    ]

    def __init__(self, state: MarketState | None = None, live: bool = True):
        super().__init__()
        self.market = state or MarketState()
        self.feed = GeminiFeed(self.market)
        self.live = live
        self.down = False
        self.frozen: MarketState | None = None
        self.frozen_at = 0.0
        self.display_revision = None
        self.book_rows: list[tuple] = []
        self.tape_rows: list[tuple] = []
        self.compact = False
        self.tape_visible = False

    def compose(self) -> ComposeResult:
        yield Static(id="masthead")
        with Horizontal(id="metrics"):
            for name in ("reference", "strike", "distance", "clock", "spread"):
                yield Static(id=name, classes="metric")
        with Horizontal(id="body"):
            with Vertical(id="book-pane"):
                yield Static(id="book-summary")
                yield DataTable(id="book", zebra_stripes=True, cursor_type="row", cell_padding=1)
                yield Static(id="book-note")
            with Vertical(id="sidebar"):
                with Vertical(id="chart-pane"):
                    yield Static(id="chart-label")
                    yield Sparkline([], id="reference-chart")
                    yield Static(id="chart-range")
                with Vertical(id="tape-pane"):
                    yield Static("Live executions · newest first\nSize is number of contracts", id="tape-note")
                    yield DataTable(id="tape", zebra_stripes=True, cursor_type="row", cell_padding=1)
        yield Static(id="connection")
        yield Footer()

    def on_mount(self) -> None:
        self.theme = "textual-dark"
        self.query_one("#chart-pane").border_title = "BTC REFERENCE · RECENT UPDATES"
        self.query_one("#tape-pane").border_title = "TIME & SALES"
        tape = self.query_one("#tape", DataTable)
        for index, (label, width) in enumerate((("TIME", 8), ("SIDE", 4), ("¢", 6), ("SIZE", 8))):
            tape.add_column(label, key=str(index), width=width)
        self.resize_layout()
        self.query_one("#book", DataTable).focus()
        self.set_interval(0.25, self.paint_market)
        if self.live:
            self.run_worker(self.feed.run(), name="gemini-public-feed")
        self.paint_market()

    def on_resize(self, event: Resize) -> None:
        if self.is_mounted:
            self.resize_layout(event.size)

    def resize_layout(self, size=None) -> None:
        size = size or self.size
        self.screen.set_class(size.width < 145, "narrow")
        self.screen.set_class(size.height < 30, "short")
        compact = size.width < 100
        table = self.query_one("#book", DataTable)
        if compact != self.compact or not table.columns:
            self.compact = compact
            table.clear(columns=True)
            self.book_rows = []
            columns = [("BID DEPTH", 10), ("TOTAL", 10), ("SIZE", 10), ("BID ¢", 7),
                       ("ASK ¢", 7), ("SIZE", 10), ("TOTAL", 10), ("ASK DEPTH", 10)]
            if compact:
                columns = columns[1:-1]
            for index, (label, width) in enumerate(columns):
                table.add_column(label, key=str(index), width=width)
            self.display_revision = None
        self.paint_market()

    def action_freeze(self) -> None:
        if self.frozen is None:
            self.frozen = deepcopy(self.market)
            self.frozen_at = time.time()
        else:
            self.frozen = None
        self.display_revision = None
        self.paint_market()

    def action_outcome(self) -> None:
        self.down = not self.down
        self.display_revision = None
        self.paint_market()

    def action_top(self) -> None:
        self.query_one("#book", DataTable).move_cursor(row=0, column=0)
        self.query_one("#book", DataTable).scroll_home(animate=False)
        self.query_one("#book", DataTable).focus()

    def action_reconnect(self) -> None:
        self.frozen = None
        self.feed.reconnect()
        self.display_revision = None

    def action_tape(self) -> None:
        if self.size.width >= 145:
            self.query_one("#tape", DataTable).focus()
        else:
            self.tape_visible = not self.tape_visible
            self.query_one("#book-pane").display = not self.tape_visible
            self.query_one("#sidebar").styles.display = "block" if self.tape_visible else None
            self.query_one("#sidebar").styles.width = "1fr" if self.tape_visible else 39
            self.query_one("#tape" if self.tape_visible else "#book", DataTable).focus()

    def update_table(self, selector: str, rows: list[tuple], previous: list[tuple]) -> None:
        table = self.query_one(selector, DataTable)
        for index in range(len(previous) - 1, len(rows) - 1, -1):
            table.remove_row(str(index))
        for index, row in enumerate(rows):
            if index >= len(previous):
                table.add_row(*row, key=str(index))
            elif row != previous[index]:
                for column, value in enumerate(row):
                    if value != previous[index][column]:
                        table.update_cell(str(index), str(column), value)

    def paint_market(self) -> None:
        if not self.is_mounted:
            return
        state = self.frozen or self.market
        now = self.frozen_at if self.frozen else time.time()
        contract = state.contract
        expired = contract is not None and now >= contract.expiry
        ready = state.connected and state.book.sequence is not None and not expired
        age = time.monotonic() - state.reference_at if state.reference_at else None
        if self.frozen:
            mode, color = "▮▮ FROZEN", ORANGE
        elif expired:
            mode, color = "↻ NEXT ROUND", ORANGE
        elif ready and state.acknowledged and age is not None and age < 10:
            mode, color = "● LIVE", GREEN
        else:
            mode, color = "○ SYNCING", ORANGE
        header = Table.grid(expand=True)
        header.add_column(ratio=1)
        header.add_column(justify="right")
        brand = Text("₿ BTC  ", style=f"bold {ORANGE}")
        brand.append("/  GEMINI PREDICTIONS  /  5 MIN", style=WHITE)
        header.add_row(brand, Text(f"{mode}   {datetime.now().astimezone():%H:%M:%S %Z}", style=color))
        self.query_one("#masthead", Static).update(header)
        self.query_one("#reference", Static).update(card("BTC REFERENCE", money(state.reference),
            (contract.agency + " index") if contract else "Waiting for public feed", CYAN))
        strike = contract.strike if contract else None
        self.query_one("#strike", Static).update(card("ROUND START PRICE", money(strike), "Official contract strike"))
        delta = state.reference - strike if state.reference is not None and strike is not None else None
        self.query_one("#distance", Static).update(card("BTC VS START", f"{delta:+,.2f} USD" if delta is not None else "—",
            "Above / below the strike", GREEN if delta is not None and delta >= 0 else RED))
        seconds = max(0, math.ceil(contract.expiry - now)) if contract else 0
        label, note = "TIME REMAINING", "Auto-switches at expiry"
        if contract and contract.start > now:
            label, seconds, note = "ROUND STARTS IN", math.ceil(contract.start - now), "Upcoming five-minute round"
        self.query_one("#clock", Static).update(card(label, f"{seconds // 60:02}:{seconds % 60:02}" if contract else "—",
            note, ORANGE if seconds < 30 else WHITE))
        bids, asks = state.book.levels(self.down) if ready else ([], [])
        spread = asks[0][0] - bids[0][0] if bids and asks else None
        self.query_one("#spread", Static).update(card("BID / ASK SPREAD", cents(spread) + "¢" if spread is not None else "—",
            "Contract prices in cents", WHITE))
        outcome = "DOWN / NO" if self.down else "UP / YES"
        pane = self.query_one("#book-pane")
        pane.border_title = f"{outcome}  ·  FULL ORDER BOOK  ·  {len(bids)} BIDS / {len(asks)} ASKS"
        pane.border_subtitle = "↑ ↓ / PgUp PgDn / mouse wheel to scroll · B to return to best prices"
        summary = Text(no_wrap=True, overflow="ellipsis")
        summary.append(f"BEST BID  {cents(bids[0][0] if bids else None)}¢", style=f"bold {GREEN}")
        summary.append("    /    ", style=GRAY)
        summary.append(f"BEST ASK  {cents(asks[0][0] if asks else None)}¢", style=f"bold {RED}")
        summary.append(f"\n{contract.symbol if contract else 'Discovering current contract…'}", style=GRAY)
        self.query_one("#book-summary", Static).update(summary)
        total_bid, total_ask = sum(q for _, q in bids), sum(q for _, q in asks)
        book_note = f"Total contracts: bid {quantity(Decimal(total_bid))} / ask {quantity(Decimal(total_ask))}"
        if self.down:
            book_note += " · DOWN = 100¢ − UP, sides reversed"
        self.query_one("#book-note", Static).update(Text(book_note, style=GRAY, no_wrap=True, overflow="ellipsis"))
        revision = (state.revision, self.down, self.frozen is not None, ready, self.compact)
        if revision != self.display_revision:
            rows, bid_sum, ask_sum = [], Decimal(0), Decimal(0)
            maximum = max((q for _, q in bids + asks), default=Decimal(1))
            for index in range(max(len(bids), len(asks))):
                if index < len(bids):
                    price, size = bids[index]
                    bid_sum += size
                    left = (Text(("█" * max(1, round(size / maximum * 10))).rjust(10), style=GREEN),
                            Text(quantity(bid_sum).rjust(10), style=GRAY),
                            Text(quantity(size).rjust(10), style=GREEN),
                            Text(cents(price).rjust(7), style=f"bold {GREEN}"))
                else:
                    left = ("", "", "", "")
                if index < len(asks):
                    price, size = asks[index]
                    ask_sum += size
                    right = (Text(cents(price).rjust(7), style=f"bold {RED}"),
                             Text(quantity(size).rjust(10), style=RED),
                             Text(quantity(ask_sum).rjust(10), style=GRAY),
                             Text("█" * max(1, round(size / maximum * 10)), style=RED))
                else:
                    right = ("", "", "", "")
                row = left + right
                rows.append(row[1:-1] if self.compact else row)
            self.update_table("#book", rows, self.book_rows)
            self.book_rows = rows
            tape_rows = []
            for when, price, size, maker_buyer in state.trades:
                buy = not maker_buyer
                if self.down:
                    price, buy = Decimal(1) - price, not buy
                tint = GREEN if buy else RED
                tape_rows.append((datetime.fromtimestamp(when).astimezone().strftime("%H:%M:%S"),
                                  Text("BUY" if buy else "SELL", style=tint),
                                  Text(cents(price), style=tint), quantity(size)))
            self.update_table("#tape", tape_rows, self.tape_rows)
            self.tape_rows = tape_rows
            self.query_one("#reference-chart", Sparkline).data = list(state.history)
            self.display_revision = revision
        self.query_one("#chart-label", Static).update(card("Official BTC index", money(state.reference), "", CYAN))
        history = state.history
        self.query_one("#chart-range", Static).update(Text(
            f"Low ${min(history):,.2f}   High ${max(history):,.2f}\nLast {len(history)} received price updates" if history else "Waiting for BTC reference prices…",
            style=GRAY))
        age_text = f"{age:.1f}s" if age is not None else "—"
        rtt = f"{state.latency_ms:.0f}ms" if state.latency_ms else "measuring"
        health = f"VIEW ONLY  ·  WebSocket RTT {rtt}  ·  Reference age {age_text}  ·  {state.message_count:,} messages  ·  {state.reconnects} reconnects"
        detail = state.discovery_error or state.status
        if self.frozen:
            detail = f"DISPLAY FROZEN at {datetime.fromtimestamp(self.frozen_at):%H:%M:%S} · feed continues · Space resumes live"
        elif expired:
            detail = "Round expired · clearing the old book and loading the next contract"
        elif strike is None and contract and now >= contract.start:
            detail += " · Official start price not supplied yet"
        self.query_one("#connection", Static).update(Text(health + "\n" + detail, style=GRAY, no_wrap=True, overflow="ellipsis"))


async def check_feed(seconds: float) -> int:
    state = MarketState()
    feed = GeminiFeed(state)
    task = asyncio.create_task(feed.run())
    try:
        await asyncio.sleep(seconds)
        good = state.connected and state.book.sequence is not None and state.reference_at > 0
        print(json.dumps({"connected": state.connected, "contract": state.contract.symbol if state.contract else None,
                          "bid_levels": len(state.book.bids), "ask_levels": len(state.book.asks),
                          "reference_price": str(state.reference), "messages": state.message_count,
                          "book_updates": state.book.updates, "reconnects": state.reconnects,
                          "status": state.status, "discovery_error": state.discovery_error}, indent=2))
        return 0 if good else 1
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def main() -> None:
    parser = argparse.ArgumentParser(description="Live Gemini BTC five-minute full order book. Public data, view only.")
    parser.add_argument("--check", type=float, metavar="SECONDS", help="Check the live feed, print a summary and exit")
    args = parser.parse_args()
    if args.check is not None:
        if not 1 <= args.check <= 300:
            parser.error("--check must be between 1 and 300 seconds")
        raise SystemExit(asyncio.run(check_feed(args.check)))
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        parser.exit(2, "Run btc in an interactive terminal, or use btc --check 10 to test the feed.\n")
    BTCTerminal().run()
