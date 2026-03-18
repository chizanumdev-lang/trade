import pandas as pd
import numpy as np
import yaml
from datetime import datetime, timedelta

def generate_perfect_signal_data():
    # Load config to see EMA periods
    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)['signal_engine']
    
    # 5m data setup
    periods_5m = 50
    dates_5m = [datetime(2024, 1, 1) + timedelta(minutes=5*i) for i in range(periods_5m)]
    ts_5m = [d.timestamp() for d in dates_5m]
    
    # Prices: Base -> Impulse -> Pullback
    # Base (0-20): 1.1000 range
    # Impulse (20-30): Move to 1.1500 (Aggressive move)
    # Pullback (30-50): Move back to 1.1190 (Demand zone and Fib level)
    
    prices_5m = []
    # Base
    for i in range(20): prices_5m.append(1.1000 + np.random.normal(0, 0.00001))
    
    # Force a base candle (small body) before impulse
    prices_5m[19] = 1.1001
    
    # Impulse
    for i in range(20, 30): prices_5m.append(1.1001 + (i-19) * 0.0050) # 500 pip move
    
    high_point = prices_5m[-1]
    low_point = prices_5m[19]
    diff = high_point - low_point
    
    # Pullback into 61.8% region
    pullback_target = high_point - diff * 0.618
    for i in range(30, 50): prices_5m.append(high_point - (i-29) * (high_point - pullback_target) / 20)
    
    df_5m = pd.DataFrame({
        'timestamp': ts_5m,
        'open': [p - 0.00005 for p in prices_5m],
        'high': [p + 0.0001 for p in prices_5m],
        'low': [p - 0.0001 for p in prices_5m],
        'close': prices_5m,
        'volume': 1000
    })
    
    # Force the base candles to be really small for S&D
    for i in range(16, 20):
        df_5m.loc[i, 'open'] = 1.1000
        df_5m.loc[i, 'close'] = 1.1001
        df_5m.loc[i, 'low'] = 1.0999
        df_5m.loc[i, 'high'] = 1.1002

    # 1m data setup - Zoom in on the end to create an EMA cross
    periods_1m = 250
    dates_1m = [datetime(2024, 1, 1) + timedelta(minutes=i) for i in range(periods_1m)]
    ts_1m = [d.timestamp() for d in dates_1m]
    
    # Interpolate 1m prices from 5m
    prices_1m = np.interp(ts_1m, ts_5m, prices_5m)
    
    # Force an EMA cross at the very end
    # Fast needs to cross UP
    for i in range(240, 250):
        prices_1m[i] = prices_1m[i] + (i - 239) * 0.0020
        
    df_1m = pd.DataFrame({
        'timestamp': ts_1m,
        'open': [p - 0.0005 for p in prices_1m],
        'high': [p + 0.0005 for p in prices_1m],
        'low': [p - 0.0005 for p in prices_1m],
        'close': prices_1m,
        'volume': 200
    })
    
    df_5m.to_csv("test_data_5m.csv", index=False)
    df_1m.to_csv("test_data_1m.csv", index=False)
    print("Perfect signal CSV data generated.")

if __name__ == "__main__":
    generate_perfect_signal_data()
