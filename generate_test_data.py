import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_sample_data():
    # 5m data - Trending up with a pullback
    dates_5m = pd.date_range(end=datetime.now(), periods=100, freq="5min")
    close_5m = 1.1000 + np.cumsum(np.random.normal(0.0001, 0.0002, 100))
    # Ensure a trend
    for i in range(50, 80): close_5m[i] += 0.0050 # Strong impulse
    for i in range(80, 100): close_5m[i] -= 0.0020 # Pullback
    
    df_5m = pd.DataFrame({
        'timestamp': dates_5m.view(np.int64) // 10**9,
        'open': close_5m - 0.0002,
        'high': close_5m + 0.0005,
        'low': close_5m - 0.0005,
        'close': close_5m,
        'volume': 1000
    })
    df_5m.to_csv("test_data_5m.csv", index=False)
    
    # 1m data - Zoom in on the last 5 periods
    dates_1m = pd.date_range(end=datetime.now(), periods=500, freq="1min")
    close_1m = np.interp(dates_1m.view(np.int64), dates_5m.view(np.int64), close_5m)
    # Add some noise
    close_1m += np.random.normal(0, 0.0001, 500)
    
    df_1m = pd.DataFrame({
        'timestamp': dates_1m.view(np.int64) // 10**9,
        'open': close_1m - 0.0001,
        'high': close_1m + 0.0002,
        'low': close_1m - 0.0002,
        'close': close_1m,
        'volume': 200
    })
    df_1m.to_csv("test_data_1m.csv", index=False)
    print("Sample CSV data generated.")

if __name__ == "__main__":
    generate_sample_data()
