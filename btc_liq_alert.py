"""
BTC liquidation alert -> Telegram  (v2)

Sources (free, no API key):
  - Binance USD-M futures  (btcusdt, btcusdc forceOrder)
  - Binance COIN-M futures (btcusd_perp forceOrder)
  - Bybit  (allLiquidation.BTCUSDT)         [ENABLE_BYBIT=1]
  - OKX    (liquidation-orders, BTC swaps)  [ENABLE_OKX=1]
  - Coinbase spot price (backup price-move alert, no liquidation data needed)

Run:  pip install websockets
      export TG_TOKEN="bot token"  TG_CHAT_ID="chat id"
      python btc_liq_alert.py

Env (all optional):
  THRESH_60=500000      alert if >= $ same-side liquidations within 60s
  THRESH_300=1500000    alert if >= $ same-side liquidations within 300s
  COOLDOWN=180          seconds between alerts of the same side
  MAX_FAILS=6           failed connects in a row before a Telegram warning
  RUN_SECONDS=21000     exit after N seconds (for GitHub Actions); 0 = forever
  PRICE_ALERT=1         backup price-move alert on/off
  PRICE_PCT_5M=1.5      alert if price moves >= this % in 5 min
  PRICE_PCT_15M=2.5     alert if price moves >= this % in 15 min
  START_MSG=1           send "started" message on boot
"""
import asyncio, json, os, time, collections
import urllib.request, urllib.parse
import websockets

TG_TOKEN = os.environ["TG_TOKEN"]
TG_CHAT = os.environ["TG_CHAT_ID"]

# (window seconds, USD threshold) - smaller window first
WINDOWS = [
    (60, float(os.getenv("THRESH_60", "500000"))),
    (300, float(os.getenv("THRESH_300", "1500000"))),
]
MAX_WINDOW = max(w for w, _ in WINDOWS)
COOLDOWN = int(os.getenv("COOLDOWN", "180"))
MAX_FAILS = int(os.getenv("MAX_FAILS", "6"))
RUN_SECONDS = float(os.getenv("RUN_SECONDS", "0")) or None
ENABLE_BYBIT = os.getenv("ENABLE_BYBIT", "1") == "1"
ENABLE_OKX = os.getenv("ENABLE_OKX", "1") == "1"
PRICE_ALERT = os.getenv("PRICE_ALERT", "1") == "1"
PRICE_PCT_5M = float(os.getenv("PRICE_PCT_5M", "1.5"))
PRICE_PCT_15M = float(os.getenv("PRICE_PCT_15M", "2.5"))
START_MSG = os.getenv("START_MSG", "1") == "1"

BINANCE_USDM = "wss://fstream.binance.com/stream?streams=btcusdt@forceOrder/btcusdc@forceOrder"
BINANCE_COINM = "wss://dstream.binance.com/ws/btcusd_perp@forceOrder"
BYBIT = "wss://stream.bybit.com/v5/public/linear"
OKX = "wss://ws.okx.com:8443/ws/v5/public"

events = collections.deque()  # (ts, side, usd, label)
last_alert = {"LONG": 0.0, "SHORT": 0.0}
stats = collections.defaultdict(lambda: {"n": 0, "usd": 0.0})  # per exchange, reset by heartbeat


def log(*a):
    print(*a, flush=True)


def send(text):
    data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": text}).encode()
    try:
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data, timeout=15
        )
    except Exception as e:
        log("telegram error:", e)


async def asend(text):
    # run blocking urllib in a thread so websockets keep flowing
    await asyncio.to_thread(send, text)


def fmt(x):
    return f"${x/1e6:.1f}M" if x >= 1e6 else f"${x/1e3:.1f}K"


def record(exchange, side, usd, label):
    events.append((time.time(), side, usd, label))
    stats[exchange]["n"] += 1
    stats[exchange]["usd"] += usd


# ---------------------------------------------------------------- handlers
def handle_binance(raw):
    msg = json.loads(raw)
    d = msg.get("data", msg)
    o = d.get("o")
    if not o:
        return
    sym = o["s"]
    # SELL order = a LONG got liquidated, BUY order = a SHORT got liquidated
    side = "LONG" if o["S"] == "SELL" else "SHORT"
    qty = float(o["z"])
    price = float(o["ap"])
    # COIN-M: qty is in contracts, 1 contract = $100
    usd = qty * 100 if sym.endswith("_PERP") else qty * price
    record("Binance", side, usd, f"{sym} (Binance)")


def handle_bybit(raw):
    msg = json.loads(raw)
    if not str(msg.get("topic", "")).startswith("allLiquidation"):
        return
    for x in msg.get("data", []):
        # Bybit: S="Buy" => a LONG position was liquidated
        side = "LONG" if x["S"] == "Buy" else "SHORT"
        usd = float(x["v"]) * float(x["p"])
        record("Bybit", side, usd, f"{x['s']} (Bybit)")


OKX_SWAPS = ("BTC-USDT-SWAP", "BTC-USD-SWAP")


def handle_okx(raw):
    if raw == "pong":
        return
    msg = json.loads(raw)
    if msg.get("arg", {}).get("channel") != "liquidation-orders":
        return
    for d in msg.get("data", []):
        inst = d.get("instId", "")
        if inst not in OKX_SWAPS:
            continue
        for x in d.get("details", []):
            # side=sell => long liquidated
            side = "LONG" if x["side"] == "sell" else "SHORT"
            sz = float(x["sz"])  # contracts
            px = float(x["bkPx"])
            # BTC-USD-SWAP: 1 contract = $100 ; BTC-USDT-SWAP: 1 contract = 0.01 BTC
            usd = sz * 100 if inst.endswith("-USD-SWAP") else sz * 0.01 * px
            record("OKX", side, usd, f"{inst} (OKX)")


# ---------------------------------------------------------------- websocket
async def keepalive(ws, msg):
    try:
        while True:
            await asyncio.sleep(15)
            await ws.send(msg)
    except Exception:
        return


async def listen(name, url, handler, subscribe=None, ping=None):
    fails = 0
    warned = False
    while True:
        pinger = None
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                if subscribe:
                    await ws.send(json.dumps(subscribe))
                if ping:
                    pinger = asyncio.create_task(keepalive(ws, ping))
                log(f"connected: {name}")
                if warned:
                    await asend(f"✅ {name} liquidation feed wapas connect ho gaya")
                fails = 0
                warned = False
                async for raw in ws:
                    try:
                        handler(raw)
                    except Exception as e:
                        log(f"{name} parse error: {e!r} raw={str(raw)[:200]}")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            fails += 1
            log(f"{name} ws error #{fails}, retrying: {e!r}")
            if fails >= MAX_FAILS and not warned:
                warned = True
                await asend(
                    f"❌ {name} se connect nahi ho paa raha ({fails} baar fail).\n"
                    f"Error: {e}\nRetry chalu hai, baaki feeds kaam kar rahe hain."
                )
        finally:
            if pinger:
                pinger.cancel()
        await asyncio.sleep(min(5 + fails * 5, 60))


# ---------------------------------------------------------------- checker
async def checker():
    while True:
        await asyncio.sleep(5)
        now = time.time()
        while events and now - events[0][0] > MAX_WINDOW:
            events.popleft()
        for side in ("LONG", "SHORT"):
            if now - last_alert[side] <= COOLDOWN:
                continue
            fired = None
            for secs, thr in WINDOWS:  # smallest window first
                rows = [e for e in events if e[1] == side and now - e[0] <= secs]
                total = sum(e[2] for e in rows)
                if total >= thr:
                    fired = (secs, rows, total)
                    break
            if not fired:
                continue
            secs, rows, total = fired
            per = collections.defaultdict(float)
            for e in rows:
                per[e[3]] += e[2]
            lines = "\n".join(
                f"{fmt(v)} {k}"
                for k, v in sorted(per.items(), key=lambda x: -x[1])[:8]
            )
            icon = "📉" if side == "LONG" else "📈"
            action = "Sell" if side == "LONG" else "Buy"  # forced sell / forced buy
            await asend(
                f"🚨 BTC {side.title()} Cascade ({action})\n\n"
                f"{icon} {len(rows)} {side} liquidations in {secs}s\n"
                f"Total: {fmt(total)}\n\n{lines}"
            )
            last_alert[side] = now
            keep = [e for e in events if e[1] != side]
            events.clear()
            events.extend(keep)


# ---------------------------------------------------------------- price backup
def fetch_price():
    with urllib.request.urlopen(
        "https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=10
    ) as r:
        return float(json.load(r)["data"]["amount"])


def pct_since(hist, now, secs):
    for ts, p in hist:  # ascending, first entry inside the window is the oldest
        if now - ts <= secs:
            return (hist[-1][1] / p - 1) * 100
    return 0.0


last_price = {"price": 0.0}


async def price_watch():
    hist = collections.deque()
    last = {"up": 0.0, "down": 0.0}
    while True:
        await asyncio.sleep(15)
        try:
            p = await asyncio.to_thread(fetch_price)
        except Exception as e:
            log("price error:", e)
            continue
        now = time.time()
        last_price["price"] = p
        hist.append((now, p))
        while hist and now - hist[0][0] > 900:
            hist.popleft()
        for secs, limit, label in ((300, PRICE_PCT_5M, "5 min"), (900, PRICE_PCT_15M, "15 min")):
            chg = pct_since(hist, now, secs)
            key = "down" if chg < 0 else "up"
            if abs(chg) >= limit and now - last[key] > 600:
                await asend(
                    f"{'📉' if chg < 0 else '📈'} BTC {chg:+.2f}% in {label}\n"
                    f"Price: ${p:,.0f}"
                )
                last[key] = now
                break


# ---------------------------------------------------------------- heartbeat
async def heartbeat():
    while True:
        await asyncio.sleep(300)
        parts = [f"{k}: {v['n']} liqs ${v['usd']:,.0f}" for k, v in stats.items()] or ["no liquidations received"]
        log(f"hb 5m | {' | '.join(parts)} | btc=${last_price['price']:,.0f}")
        stats.clear()


# ---------------------------------------------------------------- main
async def main():
    if START_MSG:
        w = ", ".join(f"{fmt(t)}/{s}s" for s, t in WINDOWS)
        await asend(f"✅ BTC liquidation alert started\nThresholds: {w}")
    tasks = [
        listen("Binance USDM", BINANCE_USDM, handle_binance),
        listen("Binance COIN-M", BINANCE_COINM, handle_binance),
        checker(),
        heartbeat(),
    ]
    if ENABLE_BYBIT:
        tasks.append(listen(
            "Bybit", BYBIT, handle_bybit,
            subscribe={"op": "subscribe", "args": ["allLiquidation.BTCUSDT"]},
            ping=json.dumps({"op": "ping"}),
        ))
    if ENABLE_OKX:
        tasks.append(listen(
            "OKX", OKX, handle_okx,
            subscribe={"op": "subscribe", "args": [{"channel": "liquidation-orders", "instType": "SWAP"}]},
            ping="ping",
        ))
    if PRICE_ALERT:
        tasks.append(price_watch())
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=RUN_SECONDS)
    except asyncio.TimeoutError:
        log("time limit reached, exiting so the next scheduled run takes over")


if __name__ == "__main__":
    asyncio.run(main())
