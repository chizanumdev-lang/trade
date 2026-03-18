import pandas as pd
import yaml
import logging
import sys
import os

sys.path.append(os.getcwd())

from backtester import Backtester

# Small account simulation parameters
INITIAL_BALANCE = 50.0
SYMBOLS = ["1HZ10V", "1HZ25V", "1HZ75V"]
START_DATE = "2026-01-01"
END_DATE = "2026-03-17"

print(f"====================================================")
print(f"   SMALL ACCOUNT SIMULATION ($ {INITIAL_BALANCE})")
print(f"====================================================")

bt = Backtester(config_path="config.yaml")

all_results = {}
for symbol in SYMBOLS:
    print(f"\nRunning backtest for {symbol}...")
    metrics, _ = bt.run(symbol, start_date=START_DATE, end_date=END_DATE, initial_balance=INITIAL_BALANCE, mode='VECTORIZED', plot=False)
    if metrics:
        all_results[symbol] = metrics

print("\n" + "="*80)
print(f"{'Symbol':<15} | {'Trades':<10} | {'Win Rate':<10} | {'Profit Factor':<15} | {'Final Equity':<15} | {'ROI %':<10}")
print("-" * 80)

for symbol, m in all_results.items():
    final_equity = m.get('final_equity', 0)
    roi = ((final_equity - INITIAL_BALANCE) / INITIAL_BALANCE) * 100
    print(f"{symbol:<15} | {m.get('total_trades'):<10} | {m.get('win_rate')*100:6.1f}%   | {m.get('profit_factor'):13.2f} | ${final_equity:13.2f} | {roi:8.1f}%")

print("="*80)
