import pandas as pd
import yaml
import logging
import sys
import os

# Add current directory to path
sys.path.append(os.getcwd())

from backtester import Backtester
from datetime import datetime

logging.basicConfig(level=logging.INFO)

bt = Backtester(config_path="config.yaml")
symbol = "1HZ10V"
start_date = "2026-01-01"
end_date = "2026-03-17"

# Force clear cache if needed?
# loader = bt.loader
# cache_path = os.path.join(loader.cache_dir, f"{symbol}_1m_20260101_20260317.csv")
# if os.path.exists(cache_path): os.remove(cache_path)

metrics, trade_log = bt.run(symbol, start_date=start_date, end_date=end_date, mode='VECTORIZED', plot=False)

print(f"Total Trades: {len(trade_log)}")
if metrics:
    print(f"Profit Factor: {metrics.get('profit_factor')}")
    print(f"Win Rate: {metrics.get('win_rate')}")
    print(f"Exit Reasons: {metrics.get('exit_reasons')}")
else:
    print("No metrics returned.")
