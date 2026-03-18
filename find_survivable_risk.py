import pandas as pd
import yaml
import logging
import sys
import os
import copy

sys.path.append(os.getcwd())

from backtester import Backtester

# Parameters
INITIAL_BALANCE = 50.0
SYMBOLS = ["1HZ10V", "1HZ25V", "1HZ75V"]
START_DATE = "2026-01-01"
END_DATE = "2026-03-17"
RISK_LEVELS = [0.03, 0.02, 0.015, 0.01, 0.0075, 0.005, 0.0025] # 3% down to 0.25%

print(f"====================================================")
print(f"   RISK SURVIVABILITY SWEEP ($ {INITIAL_BALANCE})")
print(f"====================================================")

# Load base config
with open("config.yaml", 'r') as f:
    base_config = yaml.safe_load(f)

best_survivable_risk = 0
results_table = []

for risk in RISK_LEVELS:
    print(f"\nTesting Risk Level: {risk*100:.2f}% ...")
    
    # Create override config
    config_dict = copy.deepcopy(base_config)
    config_dict['risk_manager']['kelly']['min_risk_fraction'] = risk
    config_dict['risk_manager']['kelly']['max_risk_fraction'] = risk
    config_dict['risk_manager']['kelly']['kelly_multiplier'] = 1.0
    
    # Also update the 'risk' top-level section if it exists (it does)
    if 'risk' in config_dict:
        # These don't directly control risk fraction in evaluation yet 
        # but keep them consistent for any other logic
        pass

    bt = Backtester(config_dict=config_dict)
    
    survived_all = True
    combined_roi = 0
    symbol_stats = {}
    
    for symbol in SYMBOLS:
        metrics, _ = bt.run(symbol, start_date=START_DATE, end_date=END_DATE, initial_balance=INITIAL_BALANCE, mode='VECTORIZED', plot=False)
        if metrics:
            final_equity = metrics.get('final_equity', 0)
            if final_equity <= 0:
                survived_all = False
            roi = ((final_equity - INITIAL_BALANCE) / INITIAL_BALANCE) * 100
            symbol_stats[symbol] = {'roi': roi, 'equity': final_equity, 'trades': metrics.get('total_trades', 0)}
            combined_roi += roi
        else:
            survived_all = False
            symbol_stats[symbol] = {'roi': -100, 'equity': 0, 'trades': 0}

    avg_roi = combined_roi / len(SYMBOLS)
    results_table.append({
        'risk': risk,
        'survived': survived_all,
        'avg_roi': avg_roi,
        'stats': symbol_stats
    })
    
    if survived_all and best_survivable_risk == 0:
        best_survivable_risk = risk

print("\n" + "="*90)
print(f"{'Risk %':<10} | {'Survived?':<10} | {'Avg ROI %':<12} | {'1HZ10V (TR)':<15} | {'1HZ25V (TR)':<15} | {'1HZ75V (TR)':<15}")
print("-" * 110)

for r in results_table:
    status = "YES ✅" if r['survived'] else "NO ❌"
    s = r['stats']
    def fmt(sym):
        stats = s.get(sym, {})
        return f"{stats.get('roi', -100):>6.1f}% ({stats.get('trades', 0)})"

    print(f"{r['risk']*100:>6.2f}%    | {status:<10} | {r['avg_roi']:>10.1f}% | {fmt('1HZ10V'):<15} | {fmt('1HZ25V'):<15} | {fmt('1HZ75V'):<15}")

if best_survivable_risk:
    print(f"\nMax safe risk level found: {best_survivable_risk*100:.2f}%")
else:
    print("\nNo survivable risk level found in tested range. Consider even lower risk or different params.")
print("="*90)
