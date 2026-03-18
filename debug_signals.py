import pandas as pd
import numpy as np
import yaml
import logging
import sys
import os

sys.path.append(os.getcwd())

from backtester import Backtester
from signal_engine import SignalEngine
from datetime import datetime

# Set logging to DEBUG for signal engine
logging.basicConfig(level=logging.DEBUG)

bt = Backtester(config_path="config.yaml")
symbol = "1HZ10V"
start_date = "2026-01-01"
end_date = "2026-03-17"

df_1m = bt.loader.load_data(symbol, '1m', start_date=pd.to_datetime(start_date), end_date=pd.to_datetime(end_date))
df_5m = bt.loader.load_data(symbol, '5m', start_date=pd.to_datetime(start_date), end_date=pd.to_datetime(end_date))

print(f"Loaded {len(df_1m)} 1m candles.")
print(f"Loaded {len(df_5m)} 5m candles.")

# Check a few candles
engine = bt.vec_tester.engine

crosses = 0
for i in range(100, 1000): # Just check the first 1000
    current_time = df_1m['timestamp'].iloc[i]
    w1 = df_1m.iloc[i-100:i+1]
    w5 = df_5m[df_5m['timestamp'] <= current_time].iloc[-50:]
    
    if len(w5) < 20: continue
    
    sig = engine.evaluate(symbol, w1, w5)
    if 'EMA' in sig.triggered_by:
        print(f"EMA Cross detected at {current_time}! Triggered by: {sig.triggered_by}")
        crosses += 1

print(f"Total potential signals (with EMA) in first 1000: {crosses}")
