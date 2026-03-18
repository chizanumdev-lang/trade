import pytest
import pandas as pd
from datetime import datetime
from risk_manager import KellySizer, StopLossCalculator, TradeResult, RiskManager
from signal_engine import Signal

@pytest.fixture
def kelly_config():
    return {
        'min_risk_fraction': 0.005,
        'max_risk_fraction': 0.03,
        'kelly_multiplier': 0.5
    }

def test_kelly_sizer_evolution(kelly_config):
    sizer = KellySizer(kelly_config)
    
    # 1. No history -> min risk
    assert sizer.calculate_Kelly([]) == 0.005
    
    # 2. 100% win rate -> max risk
    history = [
        TradeResult("EURUSD", "BUY", 100, 50, True, datetime.now())
    ]
    # win_rate=1.0, b is undefined but the formula handles it or we handle the avg_loss=avg_win case
    # b = 100/100 = 1. raw_kelly = 1 - (0/1) = 1. adjusted = 0.5. clamped = 0.03
    assert sizer.calculate_Kelly(history) == 0.03
    
    # 3. 50% win rate, 2:1 R:R
    history = [
        TradeResult("EURUSD", "BUY", 200, 100, True, datetime.now()),
        TradeResult("EURUSD", "BUY", -100, 100, False, datetime.now())
    ]
    # win_rate=0.5, b=2. raw_kelly = 0.5 - (0.5/2) = 0.25. adjusted = 0.125. clamped = 0.03
    assert sizer.calculate_Kelly(history) == 0.03
    
    # 4. Poor performance -> min risk
    history = [
        TradeResult("EURUSD", "BUY", 10, 100, True, datetime.now()),
        TradeResult("EURUSD", "BUY", -200, 100, False, datetime.now())
    ]
    # win_rate=0.5, b=0.05. raw_kelly = 0.5 - (0.5/0.05) = -9.5. clamped = 0.005
    assert sizer.calculate_Kelly(history) == 0.005

def test_sl_calculator_structure_snap():
    config = {'atr_multiplier': 1.5, 'structure_buffer_pips': 2}
    instr = {'EURUSD': {'pip_value': 0.0001}}
    calc = StopLossCalculator(config, instr)
    
    # BUY: ATR SL=1.0985, Zone Low=1.0990. Pip value buffer=0.0002.
    # struct_sl = 1.0990 - 0.0002 = 1.0988. ATR SL is 1.0985. ATR SL is further.
    entry = 1.1000
    atr = 0.0010 # atr_dist = 0.0015
    sl = calc.calculate_sl("EURUSD", "BUY", entry, atr, zone_low=1.0990, zone_high=None)
    assert sl == pytest.approx(1.0985) # ATR SL chosen
    
    # BUY: Zone Low is much lower
    # Zone Low=1.0980 -> struct_sl = 1.0978. ATR SL is 1.0985. Struct SL is further.
    sl = calc.calculate_sl("EURUSD", "BUY", entry, atr, zone_low=1.0980, zone_high=None)
    assert sl == pytest.approx(1.0978)
    
    # SELL: ATR SL=1.1015, Zone High=1.1010.
    # struct_sl = 1.1010 + 0.0002 = 1.1012. ATR SL is 1.1015 (further).
    sl = calc.calculate_sl("EURUSD", "SELL", entry, atr, zone_low=None, zone_high=1.1010)
    assert sl == pytest.approx(1.1015)
    
    # SELL: Zone High=1.1020 -> struct_sl = 1.1022. Struct SL further.
    sl = calc.calculate_sl("EURUSD", "SELL", entry, atr, zone_low=None, zone_high=1.1020)
    assert sl == pytest.approx(1.1022)

def test_risk_manager_rejection():
    # We'll need a real config.yaml or mock it.
    # For simplicity, we can just test the evaluate method with a manager instance.
    manager = RiskManager("config.yaml")
    
    signal = Signal("EURUSD", "BUY", 0.9, 1.1000, "1m", datetime.now(), ['EMA'])
    df_1m = pd.DataFrame({'high': [1.101, 1.110], 'low': [1.099, 1.090], 'close': [1.100, 1.100]})
    # ATR will be roughly (1.11-1.09)=0.02.
    
    # Case 1: Max open trades
    order = manager.evaluate(signal, df_1m, open_trades=3, trade_history=[], account_balance=1000)
    assert order.rejected == True
    assert "Max open trades" in order.rejection_reason
    
    # Case 2: Drawdown limit
    history = [TradeResult("EURUSD", "BUY", -60, 10, False, datetime.now())] # 6% loss
    order = manager.evaluate(signal, df_1m, open_trades=0, trade_history=history, account_balance=1000)
    assert order.rejected == True
    assert "Daily drawdown" in order.rejection_reason
