from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
import time

D = Decimal
ONE = D(1)


def timestamp(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


@dataclass
class Contract:
    ticker: str
    symbol: str
    start: float
    expiry: float
    strike: Decimal | None
    agency: str
    index: str
    state: str = "open"

    @property
    def reference_stream(self) -> str:
        return f"{self.agency}:{self.index}@indexPrice"

    @classmethod
    def from_event(cls, event: dict, contract: dict) -> Contract:
        strike = contract.get("strike", {})
        source = event.get("sourceDetails", {})
        return cls(
            ticker=event["ticker"], symbol=contract["instrumentSymbol"],
            start=timestamp(strike.get("availableAt") or contract["effectiveDate"]),
            expiry=timestamp(contract["expiryDate"]),
            strike=D(str(strike["value"])) if strike.get("value") is not None else None,
            agency=source["agency"], index=source["index"],
            state=contract.get("marketState", "open"),
        )


def select_contract(events: list[dict], now: float) -> Contract | None:
    contracts = []
    for event in events:
        if event.get("series", "").upper() != "BTC05M":
            continue
        for raw in event.get("contracts", []):
            if raw.get("ticker", "").upper() != "UP":
                continue
            try:
                contract = Contract.from_event(event, raw)
            except (KeyError, TypeError, ValueError):
                continue
            if contract.expiry > now:
                contracts.append(contract)
    return min(contracts, key=lambda c: (c.start > now, c.expiry), default=None)


class BookGap(ValueError):
    pass


@dataclass
class OrderBook:
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    sequence: int | None = None
    changed_at: float = 0
    updates: int = 0

    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.sequence = None
        self.changed_at = 0

    def apply(self, message: dict) -> bool:
        first, last = int(message["U"]), int(message["u"])
        if first > last or (self.sequence is not None and first > self.sequence + 1):
            self.reset()
            raise BookGap("Sequence gap; requesting a new full snapshot")
        if self.sequence is not None and last <= self.sequence:
            return False
        # First frame is the full snapshot requested with snapshot=-1.
        if self.sequence is None:
            self.bids.clear()
            self.asks.clear()
        for name, levels in (("b", self.bids), ("a", self.asks)):
            for price, quantity in message.get(name, []):
                p, q = D(price), D(quantity)
                if not p.is_finite() or not q.is_finite() or not 0 <= p <= 1 or q < 0:
                    self.reset()
                    raise BookGap("Invalid price level; requesting a new full snapshot")
                if q == 0:
                    levels.pop(p, None)
                else:
                    levels[p] = q
        self.sequence = last
        self.changed_at = time.monotonic()
        self.updates += 1
        return True

    def levels(self, down: bool = False) -> tuple[list, list]:
        if down:
            bids = [(ONE - p, q) for p, q in self.asks.items()]
            asks = [(ONE - p, q) for p, q in self.bids.items()]
        else:
            bids, asks = list(self.bids.items()), list(self.asks.items())
        return sorted(bids, reverse=True), sorted(asks)


@dataclass
class MarketState:
    contract: Contract | None = None
    book: OrderBook = field(default_factory=OrderBook)
    reference: Decimal | None = None
    reference_at: float = 0
    reference_key: tuple | None = None
    history: deque = field(default_factory=lambda: deque(maxlen=300))
    trades: deque = field(default_factory=lambda: deque(maxlen=100))
    trade_ids: deque = field(default_factory=lambda: deque(maxlen=500))
    connected: bool = False
    acknowledged: bool = False
    latency_ms: float = 0
    message_count: int = 0
    reconnects: int = 0
    status: str = "Finding the current five-minute round…"
    discovery_error: str = ""
    revision: int = 0

    def switch(self, contract: Contract) -> None:
        self.contract = contract
        self.book.reset()
        self.trades.clear()
        self.trade_ids.clear()
        self.reference = None
        self.reference_at = 0
        self.reference_key = None
        self.history.clear()
        self.connected = self.acknowledged = False
        self.status = "Connecting · requesting every price level"
        self.revision += 1

    def ingest(self, message: dict) -> None:
        self.message_count += 1
        if "id" in message:
            if message.get("status") != 200:
                raise RuntimeError(f"Subscription rejected ({message.get('status')})")
            self.acknowledged = True
            return
        kind = message.get("e")
        if kind == "thirdPartyPrice":
            if self.contract and message.get("I", "").lower() != self.contract.index.lower():
                return
            self.reference_at = time.monotonic()
            key = (message.get("E"), message.get("u"))
            if key != self.reference_key:
                price = D(message["p"])
                if not price.is_finite() or price <= 0:
                    raise ValueError("Invalid reference price")
                self.reference = price
                self.history.append(float(price))
                self.reference_key = key
                self.revision += 1
            return
        if not self.contract or message.get("s", "").lower() != self.contract.symbol.lower():
            return
        if kind == "depthUpdate":
            if self.book.apply(message):
                self.revision += 1
        elif kind == "contractStatus":
            if message.get("p") is not None:
                self.contract.strike = D(message["p"])
            self.contract.state = message.get("n", self.contract.state)
            self.revision += 1
        elif "t" in message and "p" in message and "q" in message:
            if message["t"] in self.trade_ids:
                return
            self.trade_ids.append(message["t"])
            # Gemini documents trade event timestamps as Unix nanoseconds.
            self.trades.appendleft((int(message["E"]) / 1e9, D(message["p"]),
                                    D(message["q"]), bool(message["m"])))
            self.revision += 1
