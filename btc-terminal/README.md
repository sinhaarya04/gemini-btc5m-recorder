# BTC terminal

Run this from your terminal:

```sh
btc
```

The command is installed on this Mac at `~/.local/bin/btc`. It opens the live Gemini BTC five-minute **prediction contract** order book. Maximize the terminal window for the full layout: at least 145 columns shows the book, reference-price chart and trade tape together. Smaller windows keep the book visible; press **T** to see the trade tape.

## Controls

| Key | Action |
|---|---|
| ↑ / ↓, Page Up / Page Down, mouse wheel | Scroll through all available price levels |
| Space | Freeze the displayed snapshot; press again to return to live data |
| D | Toggle UP / YES and DOWN / NO |
| B | Jump to the best prices and focus the book |
| T | Focus the trade tape; on smaller screens, toggle book / tape |
| Tab | Move keyboard focus between tables |
| R | Reconnect and request a new full book snapshot |
| Q or Ctrl-C | Quit |

Freeze only affects the display. The public feed continues in the background. The program automatically follows the next five-minute round while live.

## Reading the screen

- Green **bids** are standing buy prices. Red **asks** are standing sell prices for the selected outcome. Best prices appear first.
- **SIZE** is the number of contracts at that price, including fractional quantities. **TOTAL** is the cumulative number of contracts from the best price through that row.
- Prices in the book and trade tape are **cents per prediction contract**, not the dollar price of Bitcoin. Depth bars compare quantities across both sides. Every available price level is retained; scroll to inspect levels below the viewport. Only the bars are omitted in compact mode.
- **BTC REFERENCE** is the contract's designated BTC index. **ROUND START PRICE** is the official strike, when provided by Gemini; it is never guessed from the first price received after launch.
- **BTC VS START** is the current reference minus the strike in dollars. It is not a model prediction.
- **TIME & SALES** contains trades received since connecting to this round, newest first. BUY / SELL describes the aggressor in the selected outcome. An empty tape simply means no trades have arrived in this session.
- DOWN is the equivalent view of Gemini's YES-normalized book: DOWN bid = 100¢ − UP ask, DOWN ask = 100¢ − UP bid. Quantities remain unchanged. It is not a second independent book.
- **WebSocket RTT** is the measured ping round trip to the feed. **Reference age** measures time since receiving a reference message locally. These are not order-execution latency measurements.

This is the full **public price-level book (L2)**. Gemini aggregates orders at the same price; individual trader identities, hidden orders and queue positions are not exposed. A market may have only a few occupied levels. No extra depth is invented to fill the screen.

## Data and recovery

The terminal connects directly to Gemini's public API. It requires no Gemini key and contains no order-entry functions. It does not read Supabase credentials or change the existing Fly recorder. The terminal itself keeps its view in memory; the separately deployed recorder continues archiving data.

It requests `wss://ws.gemini.com?snapshot=-1` and applies 100 ms differential depth updates. It validates update sequences, replaces absolute quantities, removes zero-quantity levels and clears the book before resynchronizing after a detected gap. The display repaints up to four times a second while ingesting every received update. Expired quotes are hidden before switching rounds. Disconnections are shown explicitly and retried automatically.

Feed and contract details:

- [Gemini public streams and full snapshot protocol](https://developer.gemini.com/trading/websocket/streams)
- [Event and contract discovery](https://developer.gemini.com/rest-api/prediction-markets/events/list-events)

## Diagnostic command

```sh
btc --check 10
```

This collects live data for ten seconds, prints connection status, the contract symbol, number of price levels and reference price, then exits. It returns a nonzero status if a usable feed was not received.

If an already-open shell cannot find the command, open a new terminal window or run `~/.local/bin/btc`. This Mac's existing `.zprofile` already adds `~/.local/bin` to PATH.

## Installation on another machine

With Python 3.12+ and `uv` installed, run this in the source directory:

```sh
uv tool install --editable .
uv tool update-shell
```

Then open a new terminal and run `btc`. Keep the source directory in place because this is an editable installation. To uninstall the command, run `uv tool uninstall btc-terminal`.

## Verification

Ten tests cover snapshot/delta reconstruction, duplicate updates, gap recovery, complementary DOWN prices, round selection, clearing old-round state, trade deduplication, official strike updates, full-book scrolling, freeze/resume, small-window controls and expiry handling. Live API and full-screen terminal startup were also checked. `verification.json` and `preview.svg` contain a captured live check, not a simulated order book.

Run tests with the installed tool environment's Python:

```sh
"$(uv tool dir)/btc-terminal/bin/python" -m unittest discover -s tests -v
```
