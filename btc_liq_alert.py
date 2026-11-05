"""
BTC liquidation alert -> Telegram
Source: Binance futures websocket (free, no API key)
Run:  pip install websockets
      export TG_TOKEN="bot token"  TG_CHAT_ID="chat id"
      python btc_liq_alert.py
"""
import asyncio, json, os, time, collections
import urllib.request, urllib.parse
import websockets

TG_TOKEN = os.environ["TG_TOKEN"]
TG_CHAT = os.environ["TG_CHAT_ID"]
THRESHOLD = float(os.getenv("THRESHOLD_USD", "2000000"))  # alert if >= this in WINDOW
WINDOW = 60      # seconds
COOLDOWN = 180   # seconds between alerts of same side
MAX_FAILS = int(os.getenv("MAX_FAILS", "0"))  # give up after N failed connects in a row (0 = retry forever)

USDM = "wss://fstream.binance.com/stream?streams=btcusdt@forceOrder/btcusdc@forceOrder"
COINM = "wss://dstream.binance.com/ws/btcusd_perp@forceOrder"

events = collections.deque()  # (ts, side, usd, symbol)
last_alert = {"LONG": 0.0, "SHORT": 0.0}


def send(text):
    data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": text}).encode()
    try:
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data, timeout=15
        )
    except Exception as e:
        print("telegram error:", e)


def fmt(x):
    return f"${x/1e6:.1f}M" if x >= 1e6 else f"${x/1e3:.1f}K"


def handle(raw):
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
    events.append((time.time(), side, usd, sym))


async def listen(url):
    fails = 0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                print("connected:", url)
                fails = 0
                async for raw in ws:
                    handle(raw)
        except Exception as e:
            fails += 1
            print("ws error, reconnecting in 5s:", e)
            if MAX_FAILS and fails >= MAX_FAILS:
                send(f"❌ Liquidation alert band: {url.split('/')[2]} se connect nahi ho paa raha "
                     f"(region block ho sakta hai). Error: {e}")
                os._exit(1)
            await asyncio.sleep(5)


async def checker():
    while True:
        await asyncio.sleep(5)
        now = time.time()
        while events and now - events[0][0] > WINDOW:
            events.popleft()
        for side in ("LONG", "SHORT"):
            rows = [e for e in events if e[1] == side]
            total = sum(e[2] for e in rows)
            if total >= THRESHOLD and now - last_alert[side] > COOLDOWN:
                per = collections.defaultdict(float)
                for e in rows:
                    per[e[3]] += e[2]
                lines = "\n".join(
                    f"{fmt(v)} {k} (Binance)"
                    for k, v in sorted(per.items(), key=lambda x: -x[1])
                )
                icon = "📉" if side == "LONG" else "📈"
                # long liquidated = forced sell, short liquidated = forced buy
                action = "Sell" if side == "LONG" else "Buy"
                send(
                    f"🚨 BTC {side.title()} Cascade ({action})\n\n"
                    f"{icon} {len(rows)} {side} liquidations in {WINDOW}s\n"
                    f"Total: {fmt(total)}\n\n{lines}"
                )
                last_alert[side] = now
                for e in rows:
                    events.remove(e)


async def main():
    send("✅ BTC liquidation alert started")
    run_for = float(os.getenv("RUN_SECONDS", "0")) or None  # None = run forever
    try:
        await asyncio.wait_for(
            asyncio.gather(listen(USDM), listen(COINM), checker()), timeout=run_for
        )
    except asyncio.TimeoutError:
        print("time limit reached, exiting so the next scheduled run takes over")


if __name__ == "__main__":
    asyncio.run(main())
