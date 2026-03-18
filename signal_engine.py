import pandas as pd
import numpy as np
import yaml
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Literal, Tuple
from datetime import datetime, time, timedelta, timezone

logger = logging.getLogger("SignalEngine")

@dataclass(frozen=True)
class Signal:
    symbol: str
    direction: Literal['BUY', 'SELL', 'FLAT']
    confidence: float          # 0.0 to 1.0
    entry_price: float
    timeframe: str
    timestamp: datetime
    triggered_by: List[str]    # ['BREAKOUT', 'MOMENTUM', 'EMA', 'AMD']
    zone_high: Optional[float] = None
    zone_low: Optional[float] = None
    target_pct_adr: Optional[float] = None
    adr: Optional[float] = None
    day_high: Optional[float] = None
    day_low: Optional[float] = None
    day_open: Optional[float] = None
    source: str = 'OHLC'       # 'AMD' or 'OHLC'
    sl_price: float = 0.0      # AMD sets this directly; OHLC leaves 0 for RiskManager


# ─────────────────────────────────────────────────────────────────
# AMD (Accumulation, Manipulation, Distribution) dataclasses
# ─────────────────────────────────────────────────────────────────

@dataclass
class AccumulationRange:
    high: float
    low: float
    midpoint: float
    range_size: float
    start_idx: int
    end_idx: int


@dataclass
class AMDState:
    active: bool = False
    direction: str = ''
    accumulation: Optional[AccumulationRange] = None
    manipulation_candle_idx: int = 0
    manipulation_wick_extreme: float = 0.0
    candles_since_manipulation: int = 0
    confidence: float = 0.0


class _CandleRow:
    """Lightweight row wrapper so AMDDetector works with both dataframes and dicts."""
    __slots__ = ('open', 'high', 'low', 'close', 'symbol', 'timestamp')
    def __init__(self, o, h, l, c, sym='', ts=None):
        self.open, self.high, self.low, self.close = o, h, l, c
        self.symbol, self.timestamp = sym, ts


class AMDDetector:
    """Detects AMD setups on 1m candles."""

    def __init__(self, config: Dict):
        amd = config.get('amd_strategy', {})
        self.enabled          = amd.get('enabled', False)
        self.lookback         = amd.get('accumulation_lookback', 40)
        self.window_size      = amd.get('accumulation_window_size', 20)
        self.max_range_pct    = amd.get('max_range_pct_adr', 0.5)
        self.manip_min_wick   = amd.get('manipulation_min_wick_atr', 0.3)
        self.manip_max_body   = amd.get('manipulation_max_body_atr', 0.5)
        self.dist_min_body    = amd.get('distribution_min_body_atr', 0.4)
        self.active_candles   = amd.get('amd_active_candles', 10)
        self.sl_buffer        = amd.get('sl_buffer_atr', 0.2)
        self.min_confidence   = amd.get('min_confidence', 0.60)
        self._states: Dict[str, AMDState] = {}
        self.reset_stats()

    def reset_stats(self):
        self.stats = {
            'accumulation_scanned': 0,
            'ranges_rejected': 0,
            'manipulation_confirmed': 0,
            'distribution_confirmed': 0,
            'expired': 0
        }

    def get_stats(self) -> Dict:
        return self.stats

    def _state(self, symbol: str) -> AMDState:
        if symbol not in self._states:
            self._states[symbol] = AMDState()
        return self._states[symbol]

    # ── Phase 1 ──────────────────────────────────────────────────
    def find_accumulation(self, rows: list, adr: float) -> Optional[AccumulationRange]:
        if len(rows) < self.lookback:
            return None
        
        self.stats['accumulation_scanned'] += 1
        
        recent = rows[-self.lookback:]
        best_range = float('inf')
        best_i = 0
        for i in range(len(recent) - self.window_size):
            window = recent[i: i + self.window_size]
            total  = sum(c.high - c.low for c in window)
            if total < best_range:
                best_range = total
                best_i     = i
        w       = recent[best_i: best_i + self.window_size]
        acc_h   = max(c.high for c in w)
        acc_l   = min(c.low  for c in w)
        acc_rng = acc_h - acc_l
        if acc_rng > self.max_range_pct * adr:
            self.stats['ranges_rejected'] += 1
            return None
        return AccumulationRange(
            high=acc_h, low=acc_l,
            midpoint=(acc_h + acc_l) / 2,
            range_size=acc_rng,
            start_idx=best_i, end_idx=best_i + self.window_size
        )

    # ── Phase 2 ──────────────────────────────────────────────────
    def check_manipulation(self, c: _CandleRow, acc: AccumulationRange, atr: float) -> str:
        body_size = abs(c.close - c.open)
        body_high = max(c.open, c.close)
        body_low  = min(c.open, c.close)
        if body_size > self.manip_max_body * atr:
            return ''
        if (c.low < acc.low - self.manip_min_wick * atr
                and body_low >= acc.low
                and c.close > acc.low):
            return 'BUY'
        if (c.high > acc.high + self.manip_min_wick * atr
                and body_high <= acc.high
                and c.close < acc.high):
            return 'SELL'
        return ''

    # ── Phase 3 ──────────────────────────────────────────────────
    def check_distribution(self, c: _CandleRow, state: AMDState, atr: float, ema: float) -> bool:
        body_size = abs(c.close - c.open)
        if body_size < self.dist_min_body * atr:
            return False
        acc = state.accumulation
        if state.direction == 'BUY':
            return (c.close > c.open and c.close > acc.midpoint and c.close > ema)
        elif state.direction == 'SELL':
            return (c.close < c.open and c.close < acc.midpoint and c.close < ema)
        return False

    def compute_confidence(self, c: _CandleRow, state: AMDState, atr: float) -> float:
        acc = state.accumulation
        wick_ext    = (acc.low  - c.low)  if state.direction == 'BUY' else (c.high - acc.high)
        manip_score = min(1.0, wick_ext / (self.manip_min_wick * atr + 1e-9))
        body_size   = abs(c.close - c.open)
        body_score  = min(1.0, body_size / (self.dist_min_body * atr + 1e-9))
        return round(manip_score * 0.5 + 1.0 * 0.3 + body_score * 0.2, 3)

    def compute_sl(self, state: AMDState, atr: float) -> float:
        if state.direction == 'BUY':
            return state.manipulation_wick_extreme - self.sl_buffer * atr
        return state.manipulation_wick_extreme + self.sl_buffer * atr

    # ── Main entry ───────────────────────────────────────────────
    def evaluate(self, symbol: str, df_1m: pd.DataFrame, adr: float, atr: float, ema: float
                 ) -> Optional[Signal]:
        if not self.enabled or adr is None or adr <= 0:
            return None

        state = self._state(symbol)

        # Build _CandleRow list from df
        rows = [_CandleRow(r.open, r.high, r.low, r.close)
                for _, r in df_1m.iterrows()]
        if len(rows) < self.lookback + 2:
            return None

        candle = rows[-1]       # current closed candle
        acc    = self.find_accumulation(rows[:-1], adr)

        # Tick active-candle counter / expiry
        if state.active:
            state.candles_since_manipulation += 1
            if state.candles_since_manipulation > self.active_candles:
                self.stats['expired'] += 1
                self._states[symbol] = AMDState()   # expired
                state = self._states[symbol]

        if acc is None:
            return None

        # Phase 2 — detect manipulation
        if not state.active:
            direction = self.check_manipulation(candle, acc, atr)
            if direction:
                self.stats['manipulation_confirmed'] += 1
                self._states[symbol] = AMDState(
                    active=True, direction=direction,
                    accumulation=acc,
                    manipulation_candle_idx=len(rows),
                    manipulation_wick_extreme=candle.low if direction == 'BUY' else candle.high,
                    candles_since_manipulation=0
                )
            return None   # never enter on the manipulation candle itself

        # Phase 3 — distribution / entry
        if state.active and self.check_distribution(candle, state, atr, ema):
            conf = self.compute_confidence(candle, state, atr)
            if conf >= self.min_confidence:
                self.stats['distribution_confirmed'] += 1
                sl = self.compute_sl(state, atr)
                ts = df_1m['timestamp'].iloc[-1]
                self._states[symbol] = AMDState()  # reset after entry
                return Signal(
                    symbol=symbol,
                    direction=state.direction,
                    confidence=conf,
                    entry_price=candle.close,
                    timeframe='1m',
                    timestamp=pd.to_datetime(ts),
                    triggered_by=['AMD'],
                    source='AMD',
                    sl_price=sl,
                )
        return None

class DailyOHLCTracker:
    """Tracks daily OHLC context and ADR for synthetic indices."""
    def __init__(self, config: Dict):
        self.reset_hour = config.get('deriv_day_reset_hour', 0)
        self.reset_minute = config.get('deriv_day_reset_minute', 0)
        self.adr_lookback = config.get('adr_lookback_days', 20)
        self.symbols_data = {} # {symbol: {current_day: {}, history: []}}

    def _get_day_start(self, dt: datetime) -> datetime:
        """Calculate the start time of the current synthetic day."""
        reset_time = dt.replace(hour=self.reset_hour, minute=self.reset_minute, second=0, microsecond=0)
        if dt < reset_time:
            reset_time -= timedelta(days=1)
        return reset_time

    def update(self, symbol: str, candle: pd.Series):
        """Update daily OHLC with a new 1m candle."""
        timestamp = pd.to_datetime(candle['timestamp'])
        # Ensure timestamp is UTC if it's aware, or treat as UTC if naive
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        
        day_start = self._get_day_start(timestamp)
        
        if symbol not in self.symbols_data:
            self.symbols_data[symbol] = {
                'day_start': day_start,
                'open': candle['open'],
                'high': candle['high'],
                'low': candle['low'],
                'close': candle['close'],
                'history': [] # List of completed day ranges (high - low)
            }
            return

        data = self.symbols_data[symbol]
        
        # Check for day reset
        if day_start > data['day_start']:
            # Close prior day
            range_val = data['high'] - data['low']
            data['history'].append({
                'high': data['high'],
                'low': data['low'],
                'range': range_val,
                'end_time': day_start
            })
            if len(data['history']) > self.adr_lookback + 5: # Keep a bit more for safety
                data['history'].pop(0)
            
            # Reset for new day
            data['day_start'] = day_start
            data['open'] = candle['open']
            data['high'] = candle['high']
            data['low'] = candle['low']
            data['close'] = candle['close']
        else:
            # Update current day
            data['high'] = max(data['high'], candle['high'])
            data['low'] = min(data['low'], candle['low'])
            data['close'] = candle['close']

    def get_daily_context(self, symbol: str) -> Dict:
        """Returns tracked OHLC, prior day range, and ADR."""
        if symbol not in self.symbols_data:
            return {}
            
        data = self.symbols_data[symbol]
        history = data['history']
        
        prior_day_high = history[-1]['high'] if history else None
        prior_day_low = history[-1]['low'] if history else None
        prior_day_range = history[-1]['range'] if history else None
        
        # Calculate ADR from last 20 completed days
        adr = None
        if len(history) >= self.adr_lookback:
            recent_ranges = [h['range'] for h in history[-self.adr_lookback:]]
            adr = sum(recent_ranges) / len(recent_ranges)
        elif history:
            # Fallback if we don't have enough history yet
            adr = sum(h['range'] for h in history) / len(history)

        return {
            'day_open': data['open'],
            'day_high': data['high'],
            'day_low': data['low'],
            'day_close': data['close'],
            'prior_day_high': prior_day_high,
            'prior_day_low': prior_day_low,
            'prior_day_range': prior_day_range,
            'adr': adr
        }

class MomentumTrigger:
    """Evaluates entry triggers and computes confidence scores."""
    def __init__(self, config: Dict):
        self.config = config
        self.weights = config.get('confidence_weights', {'momentum': 0.5, 'ema': 0.3, 'breakout': 0.2})
        self.targets = config.get('confidence_targets', [])
        
    def evaluate(self, symbol: str, context: Dict, df_1m: pd.DataFrame, df_5m: pd.DataFrame, emas: Dict) -> Tuple[bool, List[str], float, float]:
        if not context or not context['adr'] or context['prior_day_high'] is None:
            return False, [], 0.0, 0.0
            
        price = df_1m['close'].iloc[-1]
        adr = context['adr']
        triggered_by = []
        
        # EMA Gate (Trigger C)
        ema_ok, direction = self._check_ema_alignment(df_1m, df_5m, emas)
        if not ema_ok:
            return False, [], 0.0, 0.0
            
        # Trigger A - Range Breakout
        breakout_ok = False
        breakout_dist = 0
        buffer = self.config.get('breakout_buffer_pct', 0.005) * adr
        
        if direction == 'BUY' and price > context['prior_day_high'] + buffer:
            breakout_ok = True
            breakout_dist = price - context['prior_day_high']
        elif direction == 'SELL' and price < context['prior_day_low'] - buffer:
            breakout_ok = True
            breakout_dist = context['prior_day_low'] - price
            
        if breakout_ok: triggered_by.append('BREAKOUT')
        
        # Trigger B - Momentum Threshold
        momentum_ok = False
        dist_from_open = 0
        threshold = self.config.get('momentum_threshold_pct', 0.015) * adr
        
        if direction == 'BUY' and price >= context['day_open'] + threshold:
            momentum_ok = True
            dist_from_open = price - context['day_open']
        elif direction == 'SELL' and price <= context['day_open'] - threshold:
            momentum_ok = True
            dist_from_open = context['day_open'] - price
            
        if momentum_ok: triggered_by.append('MOMENTUM')
        
        # Mandatory: both triggers firing
        if not (breakout_ok and momentum_ok):
            return False, [], 0.0, 0.0
            
        triggered_by.append('EMA')
        
        # Confidence Scoring
        momentum_score = min(dist_from_open / (0.3 * adr), 1.0)
        
        # EMA Score
        gap_1m = abs(emas['ema_f_1m'].iloc[-1] - emas['ema_s_1m'].iloc[-1])
        gap_threshold = self.config.get('ema_gap_threshold_pct', 0.001) * price
        ema_score = 1.0 if gap_1m > gap_threshold else 0.6
        
        breakout_score = min(breakout_dist / (0.1 * adr), 1.0)
        
        confidence = (momentum_score * self.weights['momentum']) + \
                     (ema_score * self.weights['ema']) + \
                     (breakout_score * self.weights['breakout'])
        
        # Target Selection
        target_pct_adr = 0.20 # Default minimum
        for entry in self.targets:
            if confidence >= entry['min']:
                target_pct_adr = entry['target_pct_adr']
                break
                
        return True, triggered_by, confidence, target_pct_adr

    def _check_ema_alignment(self, df_1m: pd.DataFrame, df_5m: pd.DataFrame, emas: Dict) -> Tuple[bool, str]:
        ema_f_5m = emas['ema_f_5m'].iloc[-1]
        ema_s_5m = emas['ema_s_5m'].iloc[-1]
        
        trend = 'BUY' if ema_f_5m > ema_s_5m else 'SELL'
        
        # 5m Check
        if trend == 'BUY' and ema_f_5m <= ema_s_5m: return False, 'FLAT'
        if trend == 'SELL' and ema_f_5m >= ema_s_5m: return False, 'FLAT'
        
        # 1m Check (Closed candle correct side of BOTH)
        close_1m = df_1m['close'].iloc[-1]
        ema_f_1m = emas['ema_f_1m'].iloc[-1]
        ema_s_1m = emas['ema_s_1m'].iloc[-1]
        
        if trend == 'BUY':
            if close_1m > ema_f_1m and close_1m > ema_s_1m: return True, 'BUY'
        else:
            if close_1m < ema_f_1m and close_1m < ema_s_1m: return True, 'SELL'
            
        return False, 'FLAT'

class SignalEngine:
    def __init__(self, config_path: str = None, config_dict: Dict = None):
        if config_dict:
            self.config = config_dict
        elif config_path:
            with open(config_path, 'r') as f:
                full_config = yaml.safe_load(f)
                self.config = full_config
        else:
            raise ValueError("Either config_path or config_dict must be provided")
            
        self.strategy_config = self.config['ohlc_strategy']
        conf_config = self.strategy_config.get('min_confidence', 0.0)
        if isinstance(conf_config, dict):
            self.min_confidence = conf_config
            self.min_confidence_default = self.strategy_config.get('min_confidence_default', 0.0)
        else:
            self.min_confidence = {}
            self.min_confidence_default = float(conf_config)
        self.tracker = DailyOHLCTracker(self.strategy_config)
        self.trigger = MomentumTrigger(self.strategy_config)
        self.amd     = AMDDetector(self.config)

        self.ema_fast = self.config['signal_engine']['ema']['fast']
        self.ema_slow = self.config['signal_engine']['ema']['slow']

    def on_candle_close(self, symbol: str, candle: pd.Series):
        """Update daily OHLC state."""
        self.tracker.update(symbol, candle)

    @staticmethod
    def manual_ema(series: pd.Series, length: int) -> pd.Series:
        return series.ewm(span=length, adjust=False).mean()

    def evaluate(self, symbol: str, df_1m: pd.DataFrame, df_5m: pd.DataFrame) -> Signal:
        # Update tracker with latest 1m data
        if not df_1m.empty:
            self.on_candle_close(symbol, df_1m.iloc[-1])

        if df_1m.empty or df_5m.empty:
            return Signal(symbol, 'FLAT', 0.0, 0.0, '1m', datetime.now(), [])

        # Pre-calc EMAs
        emas = {
            'ema_f_1m': self.manual_ema(df_1m['close'], self.ema_fast),
            'ema_s_1m': self.manual_ema(df_1m['close'], self.ema_slow),
            'ema_f_5m': self.manual_ema(df_5m['close'], self.ema_fast),
            'ema_s_5m': self.manual_ema(df_5m['close'], self.ema_slow),
        }
        context  = self.tracker.get_daily_context(symbol)
        adr      = context.get('adr')

        # ── ATR (14-period) ──────────────────────────────────────
        close = df_1m['close']
        prev  = close.shift(1)
        tr    = pd.concat([
            df_1m['high'] - df_1m['low'],
            (df_1m['high'] - prev).abs(),
            (df_1m['low']  - prev).abs(),
        ], axis=1).max(axis=1)
        atr = tr.ewm(alpha=1/14, adjust=False).mean().iloc[-1]

        # ── AMD takes priority ───────────────────────────────────
        if adr and atr:
            ema_1m = emas['ema_f_1m'].iloc[-1]   # fast EMA-9 for AMD distribution check
            amd_signal = self.amd.evaluate(symbol, df_1m, adr, atr, ema_1m)
            if amd_signal is not None:
                return amd_signal

        # ── Fall through to OHLC logic ───────────────────────────
        fired, triggered_by, confidence, target_pct = self.trigger.evaluate(
            symbol, context, df_1m, df_5m, emas
        )

        direction = 'FLAT'
        if fired:
            direction = 'BUY' if emas['ema_f_5m'].iloc[-1] > emas['ema_s_5m'].iloc[-1] else 'SELL'
            threshold = (
                self.min_confidence.get(symbol, self.min_confidence_default)
                if isinstance(self.min_confidence, dict)
                else self.min_confidence_default
            )
            if confidence < threshold:
                direction = 'FLAT'

        return Signal(
            symbol=symbol,
            direction=direction,
            confidence=confidence,
            entry_price=df_1m['close'].iloc[-1],
            timeframe='1m',
            timestamp=pd.to_datetime(df_1m['timestamp'].iloc[-1]),
            triggered_by=triggered_by if direction != 'FLAT' else [],
            zone_high=context.get('prior_day_high'),
            zone_low=context.get('prior_day_low'),
            target_pct_adr=target_pct,
            adr=adr,
            day_high=context.get('day_high'),
            day_low=context.get('day_low'),
            day_open=context.get('day_open'),
            source='OHLC',
            sl_price=0.0,
        )

if __name__ == "__main__":
    # Test block simulation
    import os
    logging.basicConfig(level=logging.INFO)
    
    # Try to load real history or generate dummy
    symbol = "1HZ10V"
    engine = SignalEngine("config.yaml")
    
    # Load historical data
    try:
        df_1m = pd.read_csv("cache/history/1HZ10V_1m.csv")
        df_5m = pd.read_csv("cache/history/1HZ10V_5m.csv")
        
        print(f"--- Simulating OHLC Momentum Strategy ---")
        print(f"Lookback ADR setting: {engine.strategy_config['adr_lookback_days']} days")
        
        # Simulate last 5 days
        end_idx = len(df_1m)
        start_idx = max(0, end_idx - (5 * 24 * 60)) # 5 days of 1m
        
        for i in range(start_idx, end_idx):
            current_time = pd.to_datetime(df_1m['timestamp'].iloc[i])
            w1 = df_1m.iloc[:i+1]
            w5 = df_5m[pd.to_datetime(df_5m['timestamp']) <= current_time]
            
            sig = engine.evaluate(symbol, w1, w5)
            if sig.direction != 'FLAT':
                print(f"[{current_time}] SIGNAL: {sig.direction} | Conf: {sig.confidence:.2f} | Target: {sig.target_pct_adr*100:.1f}% ADR | By: {sig.triggered_by}")

    except FileNotFoundError:
        print("Historical data not found. Run backtester first to cache data.")
