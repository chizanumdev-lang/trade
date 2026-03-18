import pandas as pd
import yaml
import logging
import sys
import os

sys.path.append(os.getcwd())

from backtester import Backtester

bt = Backtester(config_path="config.yaml")
symbols = ["1HZ10V", "1HZ25V", "1HZ75V"]
start_date = "2026-01-01"
end_date = "2026-03-17"

all_results = {}
for symbol in symbols:
    metrics, _ = bt.run(symbol, start_date=start_date, end_date=end_date, mode='VECTORIZED', plot=False)
    if metrics:
        all_results[symbol] = metrics

for symbol, m in all_results.items():
    print(f"\n--- {symbol} RESULTS (75 Days) ---")
    print(f"Profit Factor: {m.get('profit_factor'):.2f}")
    print(f"Win Rate: {m.get('win_rate')*100:.1f}%")
    print(f"Total Trades: {m.get('total_trades')}")
    print(f"Exit Reasons: {m.get('exit_reasons')}")
