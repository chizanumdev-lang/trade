import pytest
import pandas as pd
import numpy as np
from signal_engine import ZoneDetector, OrderBlockDetector

@pytest.fixture
def sample_df():
    dates = pd.date_range(start="2024-01-01", periods=50, freq="5min")
    return pd.DataFrame({
        'timestamp': dates.view(np.int64) // 10**9,
        'open': [1.1000] * 50,
        'high': [1.1005] * 50,
        'low': [1.0995] * 50,
        'close': [1.1002] * 50,
        'volume': [100] * 50
    })

def test_zone_detector_demand(sample_df):
    # Create an impulse move
    sample_df.loc[40, 'close'] = 1.1050 # Strong bullish impulse
    sample_df.loc[40, 'open'] = 1.1000
    
    # Create a base before it
    for i in range(36, 40):
        sample_df.loc[i, 'open'] = 1.0998
        sample_df.loc[i, 'close'] = 1.1000
        sample_df.loc[i, 'high'] = 1.1002
        sample_df.loc[i, 'low'] = 1.0996

    detector = ZoneDetector({'impulse_multiplier': 1.5, 'zone_expiry_candles': 50})
    detector.detect(sample_df.iloc[:41], "EURUSD")
    
    assert len(detector.zones) > 0
    zone = detector.zones[0]
    assert zone['direction'] == 'DEMAND'
    assert zone['low'] == 1.0996
    assert zone['high'] == 1.1002

def test_order_block_bullish(sample_df):
    # Last bearish candle (OB)
    sample_df.loc[48, 'open'] = 1.1000
    sample_df.loc[48, 'close'] = 1.0990 # Bearish
    sample_df.loc[48, 'low'] = 1.0985
    sample_df.loc[48, 'high'] = 1.1005
    
    # Strong bullish move
    sample_df.loc[49, 'open'] = 1.1000
    sample_df.loc[49, 'close'] = 1.1050 # Strong bullish
    
    detector = OrderBlockDetector({'strength_multiplier': 2.0})
    detector.detect(sample_df, "EURUSD")
    
    assert len(detector.order_blocks) > 0
    ob = detector.order_blocks[0]
    assert ob['direction'] == 'BULLISH'
    assert ob['low'] == 1.0985
    assert ob['high'] == 1.1005
