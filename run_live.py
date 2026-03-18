"""
run_live.py — OHLC Momentum Bot (Demo Mode)

Prerequisites:
  1. python test_connection.py   — confirm ticks received
  2. All five import checks pass (see README)

Usage:
  python run_live.py

Stop:
  Ctrl+C  → prints session summary then exits cleanly
"""
import asyncio
import os
import yaml
import signal as os_signal
import pandas as pd
import logging
from collections import deque
from data_feed import DataFeed
from signal_engine import SignalEngine
from risk_manager import RiskManager
from execution import TradeManager
from logger import Logger

logging.basicConfig(level=logging.WARNING)  # suppress noisy lib output

CONFIG_PATH = 'config.yaml'

async def main():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    symbols = config.get('trading', {}).get('symbols', ['1HZ10V', '1HZ75V'])
    initial_balance = 10000.0  # placeholder — replace with live balance fetch

    logger = Logger(config)
    logger.set_opening_balance(initial_balance)

    signal_engine = SignalEngine(config_path=CONFIG_PATH)
    risk_manager  = RiskManager(config_path=CONFIG_PATH)

    def on_trade_event(event):
        logger.log_trade_event(event)

    trade_manager = TradeManager(CONFIG_PATH, on_trade_event, paper_mode=True)

    feed = DataFeed(CONFIG_PATH)

    # Per-symbol rolling candle buffers
    buf_1m  = {sym: deque(maxlen=300) for sym in symbols}
    buf_5m  = {sym: deque(maxlen=100) for sym in symbols}
    raw_5m  = {sym: []               for sym in symbols}

    # ── Preload historical CSVs so signal engine has context from tick 1 ──
    DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(CONFIG_PATH)), 'data', 'historical')
    for sym in symbols:
        csv_path = os.path.join(DATA_DIR, f"{sym}_1m.csv")
        if not os.path.exists(csv_path):
            print(f"  [WARN] No historical CSV for {sym}, starting cold.")
            continue
        df_hist = pd.read_csv(csv_path, parse_dates=['timestamp'])
        df_hist[['open','high','low','close']] = df_hist[['open','high','low','close']].astype(float)

        # 1. Feed ALL candles into tracker to build ADR + daily H/L history
        for _, row in df_hist.iterrows():
            signal_engine.tracker.update(sym, row)

        # 2. Fill live 1m buffer with last 300 candles
        for _, row in df_hist.tail(300).iterrows():
            buf_1m[sym].append({
                'timestamp': pd.Timestamp(row['timestamp']),
                'open': row['open'], 'high': row['high'],
                'low':  row['low'],  'close': row['close'],
            })
        # 3. Build 5m buffer from those 300 candles
        rows = list(buf_1m[sym])
        for j in range(4, len(rows), 5):
            five = rows[j-4:j+1]
            buf_5m[sym].append({
                'timestamp': five[-1]['timestamp'],
                'open':  five[0]['open'],
                'high':  max(c['high'] for c in five),
                'low':   min(c['low']  for c in five),
                'close': five[-1]['close'],
            })

        ctx = signal_engine.tracker.get_daily_context(sym)
        adr = ctx.get('adr')
        pdh = ctx.get('prior_day_high')
        adr_str = f"{adr:.2f}" if adr else "n/a"
        pdh_str = f"{pdh:.2f}" if pdh else "n/a"
        print(f"  [PRELOAD] {sym}: tracker warmed ({len(df_hist)} candles) | "
              f"ADR={adr_str}  prior_day_high={pdh_str} | "
              f"buf={len(buf_1m[sym])}x1m / {len(buf_5m[sym])}x5m")
    print()

    def make_handler(symbol):
        def on_candle(candle):
            try:
                row = {
                    'timestamp': pd.Timestamp(candle.timestamp, unit='s', tz='UTC'),
                    'open': candle.open, 'high': candle.high,
                    'low': candle.low,   'close': candle.close,
                }
                buf_1m[symbol].append(row)
                raw_5m[symbol].append(row)

                # Build 5m candle every 5 × 1m candles
                if len(raw_5m[symbol]) >= 5:
                    five = raw_5m[symbol][-5:]
                    candle_5m = {
                        'timestamp': five[-1]['timestamp'],
                        'open':  five[0]['open'],
                        'high':  max(c['high'] for c in five),
                        'low':   min(c['low']  for c in five),
                        'close': five[-1]['close'],
                    }
                    buf_5m[symbol].append(candle_5m)

                if len(buf_1m[symbol]) < 100 or len(buf_5m[symbol]) < 20:
                    return  # not enough history yet

                df_1m = pd.DataFrame(list(buf_1m[symbol]))
                df_5m = pd.DataFrame(list(buf_5m[symbol]))

                signal = signal_engine.evaluate(symbol, df_1m, df_5m)
                logger.log_signal(signal)

                if signal.direction != 'FLAT':
                    open_trades = list(trade_manager.active_trades.values())
                    history     = []
                    order = risk_manager.evaluate(signal, df_1m, open_trades, history, initial_balance)
                    if not order.rejected:
                        asyncio.create_task(trade_manager.execute(order))
                    else:
                        print(f"  [SKIP] {symbol} – {order.rejection_reason}")

                # Manage open positions
                close_px = buf_1m[symbol][-1]['close']
                asyncio.create_task(
                    trade_manager.on_candle_close(symbol, close_px, df_1m, risk_manager)
                )

            except Exception as e:
                logger.log_error(f'on_candle({symbol})', str(e))

        return on_candle

    for sym in symbols:
        feed.subscribe(sym, make_handler(sym))

    # Graceful Ctrl+C shutdown
    loop = asyncio.get_event_loop()

    def shutdown(sig, frame):
        print("\nShutting down...")
        logger.print_session_summary(initial_balance)
        loop.stop()

    os_signal.signal(os_signal.SIGINT,  shutdown)
    os_signal.signal(os_signal.SIGTERM, shutdown)

    print("Connecting to Deriv demo account...")
    print("Press Ctrl+C to stop and print session summary.\n")
    await feed.start()


if __name__ == '__main__':
    asyncio.run(main())
