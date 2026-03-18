import pytest
from data_feed import Tick, Candle, CandleBuilder

def test_build_time_candle():
    symbol = "EURUSD"
    timeframe = "1m"
    timestamp = 1600000000.0
    
    ticks = [
        Tick(symbol, timestamp + 1, 1.1000, 1.1002, 1.1001),
        Tick(symbol, timestamp + 10, 1.1005, 1.1007, 1.1006),
        Tick(symbol, timestamp + 30, 1.0995, 1.0997, 1.0996),
        Tick(symbol, timestamp + 50, 1.1002, 1.1004, 1.1003),
    ]
    
    candle = CandleBuilder.build_time_candle(ticks, symbol, timeframe, timestamp)
    
    assert candle.symbol == symbol
    assert candle.timeframe == timeframe
    assert candle.timestamp == timestamp
    assert candle.open == 1.1001
    assert candle.high == 1.1006
    assert candle.low == 1.0996
    assert candle.close == 1.1003
    assert candle.volume == 4.0

def test_build_tick_candle():
    symbol = "V75"
    timestamp = 1600000000.0
    
    ticks = [
        Tick(symbol, timestamp + 1, 500.0, 500.2, 500.1),
        Tick(symbol, timestamp + 2, 500.5, 500.7, 500.6),
        Tick(symbol, timestamp + 3, 499.5, 499.7, 499.6),
    ]
    
    candle = CandleBuilder.build_tick_candle(ticks, symbol, timestamp + 3)
    
    assert candle.symbol == symbol
    assert candle.timeframe == 'tick'
    assert candle.timestamp == timestamp + 3
    assert candle.open == 500.1
    assert candle.high == 500.6
    assert candle.low == 499.6
    assert candle.close == 499.6
    assert candle.volume == 3.0

def test_build_candle_empty_ticks():
    assert CandleBuilder.build_time_candle([], "EURUSD", "1m", 1600000000.0) is None
    assert CandleBuilder.build_tick_candle([], "EURUSD", 1600000000.0) is None
