"""
fetch_real_data.py — Fetches 60 days of real 1m OHLC candles from Deriv API
and prints a sanity check table: ADR, ATR, spread, spread % of ATR

Run:  python fetch_real_data.py
"""
import asyncio
import json
import os
import pandas as pd
import websockets
import yaml
from datetime import datetime, timedelta, timezone

CONFIG_PATH = '/Users/chizanumidemili/Projects/bot/config.yaml'
OUTPUT_DIR  = '/Users/chizanumidemili/Projects/bot/data/historical'
os.makedirs(OUTPUT_DIR, exist_ok=True)

SYMBOLS = ['1HZ10V', '1HZ25V', '1HZ75V']

# pip_size from config (used for spread calc)
PIP_SIZE = {'1HZ10V': 0.01, '1HZ25V': 1.0, '1HZ75V': 0.01}
# Typical live spreads observed from WS feed (in price units)
LIVE_SPREAD = {'1HZ10V': 0.20, '1HZ25V': 60.0, '1HZ75V': 0.80}

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

APP_ID = config['deriv']['app_id']
TOKEN  = config['deriv']['api_token']
WS_URI = f"wss://ws.binaryws.com/websockets/v3?app_id={APP_ID}"

END_EPOCH   = int(datetime(2026, 3, 17, tzinfo=timezone.utc).timestamp())
START_EPOCH = int((datetime(2026, 3, 17, tzinfo=timezone.utc) - timedelta(days=60)).timestamp())
GRANULARITY = 60  # 1 minute


async def fetch_candles(ws, symbol: str, granularity: int, start: int, end: int) -> list:
    """Fetch candles in 1000-candle batches (Deriv limit)."""
    all_candles = []
    batch_start = start
    batch_size = 1000 * granularity  # seconds
    req_id = 1

    while batch_start < end:
        batch_end = min(batch_start + batch_size, end)
        req = {
            "ticks_history": symbol,
            "granularity": granularity,
            "start": batch_start,
            "end": batch_end,
            "style": "candles",
            "count": 1000,
            "req_id": req_id
        }
        await ws.send(json.dumps(req))
        resp = json.loads(await ws.recv())

        if 'error' in resp:
            print(f"  Error for {symbol}: {resp['error']['message']}")
            break

        candles = resp.get('candles', [])
        if not candles:
            break

        all_candles.extend(candles)
        batch_start = candles[-1]['epoch'] + granularity
        req_id += 1
        await asyncio.sleep(0.3)  # rate limit

    return all_candles


async def main():
    print(f"Connecting to {WS_URI}")
    async with websockets.connect(WS_URI, ping_interval=None) as ws:
        # Authorize
        await ws.send(json.dumps({"authorize": TOKEN, "req_id": 0}))
        auth = json.loads(await ws.recv())
        if 'error' in auth:
            print(f"Auth failed: {auth['error']['message']}")
            return
        print(f"Authorized as: {auth['authorize'].get('loginid', 'unknown')}\n")

        results = {}

        for symbol in SYMBOLS:
            print(f"Fetching {symbol} 1m candles ({START_EPOCH} → {END_EPOCH})...")
            candles = await fetch_candles(ws, symbol, GRANULARITY, START_EPOCH, END_EPOCH)

            if not candles:
                print(f"  No data received for {symbol}")
                continue

            df = pd.DataFrame(candles)
            df['timestamp'] = pd.to_datetime(df['epoch'], unit='s', utc=True)
            df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close'})
            df = df[['timestamp', 'epoch', 'open', 'high', 'low', 'close']].copy()
            df[['open','high','low','close']] = df[['open','high','low','close']].astype(float)

            # Save CSV
            out_path = os.path.join(OUTPUT_DIR, f"{symbol}_1m.csv")
            df.to_csv(out_path, index=False)
            print(f"  Saved {len(df)} candles → {out_path}")

            # Sample
            print(f"  Sample (first 3 rows):")
            for _, row in df.head(3).iterrows():
                print(f"    {row['timestamp'].strftime('%Y-%m-%d %H:%M')}  "
                      f"O={row['open']:.4f}  H={row['high']:.4f}  "
                      f"L={row['low']:.4f}  C={row['close']:.4f}")
            print()

            results[symbol] = df

    # --- Sanity Check Table ---
    print("=" * 72)
    print(f"{'Symbol':<10} {'Avg ADR':>10} {'ATR 1m':>10} {'Spread':>10} {'Spread%ATR':>12}")
    print("-" * 72)

    for symbol in SYMBOLS:
        if symbol not in results:
            print(f"{symbol:<10} {'N/A':>10}")
            continue
        df = results[symbol]

        # Daily range: group by date, compute high-low
        df['date'] = df['timestamp'].dt.date
        daily = df.groupby('date').agg(day_high=('high','max'), day_low=('low','min'))
        daily['range'] = daily['day_high'] - daily['day_low']
        avg_adr = daily['range'].mean()

        # ATR 14-period on 1m
        df2 = df.copy()
        df2['prev_close'] = df2['close'].shift(1)
        df2['tr'] = pd.concat([
            df2['high'] - df2['low'],
            (df2['high'] - df2['prev_close']).abs(),
            (df2['low']  - df2['prev_close']).abs()
        ], axis=1).max(axis=1)
        atr = df2['tr'].ewm(alpha=1/14, adjust=False).mean().iloc[-1]

        spread      = LIVE_SPREAD[symbol]
        spread_pct  = (spread / atr * 100) if atr > 0 else float('inf')
        flag        = " ← WIDE" if spread_pct > 20 else ""

        print(f"{symbol:<10} {avg_adr:>10.4f} {atr:>10.4f} {spread:>10.4f} {spread_pct:>11.1f}%{flag}")

    print("=" * 72)
    print("\nSpread % of ATR < 20% = viable  |  > 20% = spread dominates edge")


if __name__ == '__main__':
    asyncio.run(main())
