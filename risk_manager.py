import pandas as pd
import yaml
import logging
from dataclasses import dataclass
from typing import List, Optional, Literal, Dict
from datetime import datetime

logger = logging.getLogger("RiskManager")

@dataclass
class TradeOrder:
    symbol: str
    direction: Literal['BUY', 'SELL']
    lot_size: float
    entry_price: float
    sl_price: float
    tp1_price: float           # 50% close price
    tp2_price: float           # full exit target
    be_trigger_price: float
    trail_distance: float
    risk_fraction: float
    kelly_fraction: float
    atr_value: float
    rejected: bool
    rejection_reason: Optional[str] = None
    tp1_hit: bool = False
    trail_active: bool = False

@dataclass
class TradeResult:
    symbol: str
    direction: str
    profit_loss: float         # in account currency
    risk_amount: float
    won: bool
    timestamp: datetime

class KellySizer:
    """Computes position size using the Kelly Criterion (Half Kelly)."""
    def __init__(self, config: Dict):
        self.min_risk = config.get('min_risk_fraction', 0.005)
        self.max_risk = config.get('max_risk_fraction', 0.03)
        self.multiplier = config.get('kelly_multiplier', 0.5)
        self.warmup_trades = config.get('kelly_warmup_trades', 20)
        self.warmup_fraction = config.get('kelly_warmup_fraction', 0.01)

    def calculate_Kelly(self, trade_history: List[TradeResult]) -> float:
        if len(trade_history) < self.warmup_trades:
            return self.warmup_fraction
            
        if not trade_history:
            return self.min_risk # Starting risk
            
        wins = [t for t in trade_history if t.won]
        win_rate = len(wins) / len(trade_history)
        
        if win_rate == 0:
            return self.min_risk
            
        # Average Win / Average Loss
        avg_win = sum(t.profit_loss for t in wins) / len(wins) if wins else 0
        losses = [t for t in trade_history if not t.won]
        avg_loss = abs(sum(t.profit_loss for t in losses) / len(losses)) if losses else avg_win # fallback if no losses
        
        if avg_loss == 0:
            return self.max_risk
            
        b = avg_win / avg_loss # Win/Loss ratio
        
        # Kelly % = W - [(1-W)/b]
        raw_kelly = win_rate - ((1 - win_rate) / b)
        
        # Apply Half Kelly and clamp
        adjusted_kelly = raw_kelly * self.multiplier
        return max(self.min_risk, min(adjusted_kelly, self.max_risk))

class StopLossCalculator:
    """Calculates stop loss based on ATR and market structure."""
    def __init__(self, config: Dict, instruments_config: Dict, risk_config: Dict = None):
        self.atr_multiplier = risk_config.get('atr_multiplier', 0.75) if risk_config else config.get('atr_multiplier', 1.5)
        self.max_sl_pct_adr = risk_config.get('max_sl_pct_adr', 0.01) if risk_config else 0.01
        self.buffer_pips = config.get('structure_buffer_pips', 2)
        self.instruments = instruments_config

    def calculate_sl(self, symbol: str, direction: str, entry_price: float, atr: float, 
                     zone_low: Optional[float], zone_high: Optional[float], adr: float) -> float:
        pip_size = self.instruments.get(symbol, {}).get('pip_size', 0.0001)
        # fallback to pip_value if pip_size not available
        if 'pip_size' not in self.instruments.get(symbol, {}):
             pip_size = self.instruments.get(symbol, {}).get('pip_value', 0.0001)
             
        buffer = self.buffer_pips * pip_size
        
        atr_sl_distance = atr * self.atr_multiplier
        adr_cap = adr * self.max_sl_pct_adr
        sl_distance = min(atr_sl_distance, adr_cap)
        
        if direction == 'BUY':
            atr_sl = entry_price - sl_distance
            if zone_low:
                struct_sl = zone_low - buffer
                final_sl = min(atr_sl, struct_sl)
                if abs(entry_price - final_sl) > adr_cap:
                    return entry_price - adr_cap
                return final_sl
            return atr_sl
        else: # SELL
            atr_sl = entry_price + sl_distance
            if zone_high:
                struct_sl = zone_high + buffer
                final_sl = max(atr_sl, struct_sl)
                if abs(entry_price - final_sl) > adr_cap:
                    return entry_price + adr_cap
                return final_sl
            return atr_sl

class RiskManager:
    def __init__(self, config_path: str = None, config_dict: Dict = None):
        if config_dict:
            self.config = config_dict['risk_manager']
            self.instruments = config_dict.get('instruments', {})
            self.risk_cfg = config_dict.get('risk', {})
        elif config_path:
            with open(config_path, 'r') as f:
                full_config = yaml.safe_load(f)
                self.config = full_config['risk_manager']
                self.instruments = full_config.get('instruments', {})
                self.risk_cfg = full_config.get('risk', {})
        else:
            raise ValueError("Either config_path or config_dict must be provided")
            
        self.max_trades_per_direction = self.risk_cfg.get('max_trades_per_direction', 999)
        self.tp1_ratio = self.risk_cfg.get('tp1_ratio', 0.5)
        self.tp2_ratio = self.risk_cfg.get('tp2_ratio', 2.0)  # R:R multiplier for TP2
        self.trail_atr_multiplier = self.risk_cfg.get('trail_atr_multiplier', 0.5)
        self.trail_activate_after_tp1 = self.risk_cfg.get('trail_activate_after_tp1', True)

        self.max_risk_fraction = self.config['kelly'].get('max_risk_fraction', 0.05)
        self.min_risk_fraction = self.config['kelly'].get('min_risk_fraction', 0.01)

        self.sizer = KellySizer(self.config['kelly'])
        self.sl_calc = StopLossCalculator(self.config['stop_loss'], self.instruments, self.risk_cfg)
        self.tp_config = self.config['take_profit']
        self.be_config = self.config['break_even']
        self.trail_config = self.config['trailing_stop']
        self.controls = self.config['controls']

    @staticmethod
    def calculate_atr(df: pd.DataFrame, period: int = 14) -> float:
        if len(df) < period + 1:
            return 0.0
        high = df['high']
        low = df['low']
        prev_close = df['close'].shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1/period, adjust=False).mean()
        return atr.iloc[-1]

    def evaluate(self, signal, candles_1m: pd.DataFrame, open_trades: List[TradeOrder], 
                  trade_history: List[TradeResult], account_balance: float) -> TradeOrder:
        symbol = signal.symbol
        direction = signal.direction
        entry_price = signal.entry_price

        # Check same direction limit
        open_same_direction = [t for t in open_trades if t.symbol == symbol and t.direction == direction]
        if len(open_same_direction) >= self.max_trades_per_direction:
            return self._reject(signal, 'SAME_DIRECTION_LIMIT')

        if len(open_trades) >= self.controls['max_open_trades']:
            return self._reject(signal, "Max open trades reached")
            
        today = datetime.now().date()
        today_loss = sum(t.profit_loss for t in trade_history 
                         if t.timestamp.date() == today and t.profit_loss < 0)
        if abs(today_loss) > account_balance * self.controls['daily_drawdown_limit']:
            return self._reject(signal, "Daily drawdown limit hit")

        atr_period = self.config['stop_loss']['atr_period']
        atr = self.calculate_atr(candles_1m, atr_period)
        if atr == 0:
            return self._reject(signal, "Wait for more data (ATR is 0)")

        kelly_fraction = self.sizer.calculate_Kelly(trade_history)
        risk_amount = account_balance * kelly_fraction
        
        # AMD signals carry their own structural SL — use it directly
        if getattr(signal, 'source', 'OHLC') == 'AMD' and getattr(signal, 'sl_price', 0) > 0:
            sl_price = signal.sl_price
        else:
            sl_price = self.sl_calc.calculate_sl(
                symbol, direction, entry_price, atr, signal.zone_low, signal.zone_high, signal.adr
            )
        sl_dist = abs(entry_price - sl_price)
        if sl_dist == 0:
            return self._reject(signal, "Invalid SL distance (0)")
            
        # 4. Lot sizing
        instr_config = self.instruments.get(symbol, {})
        min_lot = instr_config.get('min_lot', 0.01)
        max_lot = instr_config.get('max_lot', 100.0)
        pip_value = instr_config.get('pip_value', 1.0)
        pip_size = instr_config.get('pip_size', 0.0001)
        
        sl_pips = sl_dist / pip_size
        lot_size = risk_amount / (sl_pips * pip_value)
        
        # Clamp to broker limits
        lot_size = round(max(min_lot, min(lot_size, max_lot)), 2)
        
        # Check if min_lot exceeds our risk budget
        min_risk_dollars = min_lot * sl_pips * pip_value
        if min_risk_dollars > (account_balance * self.max_risk_fraction):
            return self._reject(signal, f"Risk too high (min_lot risks > {self.max_risk_fraction*100}%)")

        # 5. TP levels — based on SL distance × R:R ratio
        tp2_dist = sl_dist * self.tp2_ratio
        tp1_dist = tp2_dist * self.tp1_ratio
            
        if direction == 'BUY':
            tp2_price = entry_price + tp2_dist
            tp1_price = entry_price + tp1_dist
            be_trigger_price = entry_price + sl_dist
        else:
            tp2_price = entry_price - tp2_dist
            tp1_price = entry_price - tp1_dist
            be_trigger_price = entry_price - sl_dist

        return TradeOrder(
            symbol=symbol,
            direction=direction,
            lot_size=lot_size,
            entry_price=entry_price,
            sl_price=sl_price,
            tp1_price=tp1_price,
            tp2_price=tp2_price,
            be_trigger_price=be_trigger_price,
            trail_distance=atr * self.trail_atr_multiplier,
            risk_fraction=kelly_fraction,
            kelly_fraction=kelly_fraction,
            atr_value=atr,
            rejected=False
        )

    def _reject(self, signal, reason: str) -> TradeOrder:
        return TradeOrder(
            symbol=signal.symbol, direction=signal.direction, lot_size=0,
            entry_price=signal.entry_price, sl_price=0, tp1_price=0, tp2_price=0,
            be_trigger_price=0, trail_distance=0, risk_fraction=0, kelly_fraction=0,
            atr_value=0, rejected=True, rejection_reason=reason
        )

    def update_trailing_stop(self, order: TradeOrder, current_price: float, candles_1m: pd.DataFrame, tp1_hit: bool = True) -> float:
        """Trail the full position SL after TP1 is hit."""
        if not self.trail_activate_after_tp1 or not tp1_hit:
            return order.sl_price
        atr = self.calculate_atr(candles_1m, self.config['stop_loss']['atr_period'])
        trail_distance = atr * self.trail_atr_multiplier
        if order.direction == 'BUY':
            new_sl = max(current_price - trail_distance, order.entry_price)
            return max(new_sl, order.sl_price)
        else:
            new_sl = min(current_price + trail_distance, order.entry_price)
            return min(new_sl, order.sl_price)

    def check_break_even(self, order: TradeOrder, current_price: float) -> bool:
        if order.direction == 'BUY': return current_price >= order.be_trigger_price
        return current_price <= order.be_trigger_price
