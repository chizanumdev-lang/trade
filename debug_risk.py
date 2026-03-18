import pandas as pd
import numpy as np
import yaml
import sys
import os

sys.path.append(os.getcwd())

from backtester import Backtester

bt = Backtester(config_path="config.yaml")
symbol = "1HZ10V"
start_date = "2026-01-01"
end_date = "2026-03-17"

df_1m = bt.loader.load_data(symbol, '1m', start_date=pd.to_datetime(start_date), end_date=pd.to_datetime(end_date))
df_5m = bt.loader.load_data(symbol, '5m', start_date=pd.to_datetime(start_date), end_date=pd.to_datetime(end_date))

engine = bt.vec_tester.engine
risk = bt.vec_tester.risk

print(f"Checking {len(df_1m)} candles for signals and risk evaluation...")

for i in range(100, 10000):
    current_time = df_1m['timestamp'].iloc[i]
    w1 = df_1m.iloc[i-100:i+1]
    w5 = df_5m[df_5m['timestamp'] <= current_time].iloc[-50:]
    
    if len(w5) < 20: continue
    
    sig = engine.evaluate(symbol, w1, w5)
    if sig.direction != 'FLAT':
        print(f"Signal detected at {current_time}: {sig.direction}")
        order = risk.evaluate(sig, w1, 0, [], 10000)
        if order.rejected:
            print(f"  REJECTED: {order.rejection_reason}")
        else:
            print(f"  APPROVED: Lot {order.lot_size}, SL {order.sl_price}, TP1 {order.tp1_price}")
            break # Found one!
