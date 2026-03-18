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

stats = {
    'EMA_Cross': 0,
    'Zone_Hit': 0,
    'OB_Hit': 0,
    'Fib_Hit': 0,
    'Full_Signal': 0
}

print(f"Checking {len(df_1m)} candles for confluences...")

for i in range(100, min(10000, len(df_1m))):
    current_time = df_1m['timestamp'].iloc[i]
    w1 = df_1m.iloc[i-100:i+1]
    w5 = df_5m[df_5m['timestamp'] <= current_time].iloc[-50:]
    
    if len(w5) < 20: continue
    
    # Manually check components
    # 1. EMA
    ema_f_1m = engine.manual_ema(w1['close'], engine.ema_fast)
    ema_s_1m = engine.manual_ema(w1['close'], engine.ema_slow)
    crossover = (ema_f_1m.iloc[-1] > ema_s_1m.iloc[-1] and ema_f_1m.iloc[-2] <= ema_s_1m.iloc[-2]) or \
                (ema_f_1m.iloc[-1] < ema_s_1m.iloc[-1] and ema_f_1m.iloc[-2] >= ema_s_1m.iloc[-2])
    
    if crossover:
        stats['EMA_Cross'] += 1
        
        # Check confluences
        engine.zone_detector.detect(w5, symbol)
        engine.ob_detector.detect(w5, symbol)
        sh, sl = engine.fib_calc.get_swing_points(w5)
        
        price = w1['close'].iloc[-1]
        
        zone_hit = any(z['low'] <= price <= z['high'] for z in engine.zone_detector.zones)
        ob_hit = any(ob['low'] <= price <= ob['high'] for ob in engine.ob_detector.order_blocks)
        fib_hit = False
        if sh and sl:
            diff = sh - sl
            fib_61 = sh - diff * 0.618
            fib_78 = sh - diff * 0.786
            fib_hit = (fib_78 <= price <= fib_61) or (sl + diff * 0.618 <= price <= sl + diff * 0.786)
            
        if zone_hit: stats['Zone_Hit'] += 1
        if ob_hit: stats['OB_Hit'] += 1
        if fib_hit: stats['Fib_Hit'] += 1
        
        sig = engine.evaluate(symbol, w1, w5)
        if sig.direction != 'FLAT':
            stats['Full_Signal'] += 1

print("\nCONFLUENCE STATS (First 10,000 candles):")
for k, v in stats.items():
    print(f"{k:<15}: {v}")
