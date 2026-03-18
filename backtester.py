import pandas as pd
import numpy as np
import yaml
import os
import logging
from dataclasses import dataclass
from typing import List, Optional, Literal, Dict
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Internal module imports
from signal_engine import SignalEngine, Signal
from risk_manager import RiskManager, TradeOrder, TradeResult
from execution import TradeEvent

logger = logging.getLogger("Backtester")

@dataclass
class TradeLog:
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    sl_price: float
    tp1_price: float
    tp2_price: float
    lot_size: float
    profit_loss: float
    risk_amount: float
    realised_rr: float
    entry_time: datetime
    exit_time: datetime
    exit_reason: Literal['TP1', 'TP2', 'SL', 'TRAIL', 'BE', 'SIGNAL_FLIP']
    triggered_by: List[str]
    mode: Literal['VECTORIZED', 'EVENT_DRIVEN']

class HistoricalDataLoader:
    def __init__(self, config: Dict):
        self.cache_dir = config.get('cache_dir', 'cache/history')
        os.makedirs(self.cache_dir, exist_ok=True)

    def load_data(self, symbol: str, timeframe: str, days: int = None, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        if days:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
        date_str = f"_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}" if start_date else ""
        cache_path = os.path.join(self.cache_dir, f"{symbol}_{timeframe}{date_str}.csv")
        
        # In a real environment, we'd check if cache is too old
        if os.path.exists(cache_path):
            df = pd.read_csv(cache_path, parse_dates=['timestamp'])
            return df
            
        # Broker Loading with Fallback
        df = pd.DataFrame()
        if self._is_mt5_symbol(symbol):
            df = self._load_mt5_data(symbol, timeframe, days, start_date, end_date)
        elif self._is_deriv_symbol(symbol):
            df = self._load_deriv_data(symbol, timeframe, days, start_date, end_date)
            
        if df.empty:
            df = self._generate_mock_data(symbol, timeframe, days, start_date, end_date)
            
        if not df.empty:
            df.to_csv(cache_path, index=False)
        return df

    def _is_mt5_symbol(self, symbol: str) -> bool:
        return any(x in symbol for x in ['EURUSD', 'GBPUSD', 'XAUUSD', 'US30'])

    def _is_deriv_symbol(self, symbol: str) -> bool:
        return any(x in symbol.upper() for x in ['V75', 'BOOM', 'CRASH', 'STEP', 'R_', '1HZ'])

    def _load_mt5_data(self, symbol: str, timeframe: str, days: int = None, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        try:
            import MetaTrader5 as mt5
            if not mt5.initialize():
                return pd.DataFrame()
            
            tf = mt5.TIMEFRAME_M1 if timeframe == '1m' else mt5.TIMEFRAME_M5
            if days:
                rates = mt5.copy_rates_from_pos(symbol, tf, 0, days * 24 * 60 if timeframe == '1m' else days * 24 * 12)
            else:
                rates = mt5.copy_rates_range(symbol, tf, start_date, end_date)
            if rates is None or len(rates) == 0:
                return pd.DataFrame()
                
            df = pd.DataFrame(rates)
            df['timestamp'] = pd.to_datetime(df['time'], unit='s')
            return df[['timestamp', 'open', 'high', 'low', 'close', 'real_volume']]
        except ImportError:
            logger.warning("MetaTrader5 not installed or failed to initialize. Cannot load MT5 data.")
            return pd.DataFrame()

    def _load_deriv_data(self, symbol: str, timeframe: str, days: int = None, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        # Placeholder for Deriv REST API call (/v3/ticks_history)
        logger.warning(f"Deriv data loading not implemented. Generating mock data for {symbol}.")
        return pd.DataFrame()

    def _generate_mock_data(self, symbol: str, timeframe: str, days: int = None, start_date: datetime = None, end_date: datetime = None) -> pd.DataFrame:
        """Fallback to generating mock data if broker is unavailable."""
        freq = '1min' if timeframe == '1m' else '5min'
        if not start_date:
            start_date = datetime.now() - timedelta(days=days)
        if not end_date:
            end_date = datetime.now()
            
        index = pd.date_range(start=start_date, end=end_date, freq=freq)
        
        np.random.seed(hash(symbol) % 2**32) # Seed based on symbol for consistent mock data
        close = 1.1000 + np.cumsum(np.random.normal(0, 0.0002, len(index)))
        df = pd.DataFrame({
            'timestamp': index,
            'open': close + np.random.normal(0, 0.0001, len(index)),
            'high': close + abs(np.random.normal(0, 0.0002, len(index))),
            'low': close - abs(np.random.normal(0, 0.0002, len(index))),
            'close': close,
            'volume': np.random.randint(100, 1000, len(index))
        })
        
        # Ensure high is max, low is min
        df['high'] = df[['open', 'close', 'high']].max(axis=1)
        df['low'] = df[['open', 'close', 'low']].min(axis=1)
        
        return df

class MetricsCalculator:
    @staticmethod
    def calculate(trade_log: List[TradeLog], initial_balance: float) -> Dict:
        if not trade_log:
            return {}
            
        df = pd.DataFrame([t.__dict__ for t in trade_log])
        total_trades = len(df)
        wins = df[df['profit_loss'] > 0]
        win_rate = len(wins) / total_trades if total_trades > 0 else 0
        
        gross_profit = df[df['profit_loss'] > 0]['profit_loss'].sum()
        gross_loss = abs(df[df['profit_loss'] < 0]['profit_loss'].sum())
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else gross_profit
        
        # Equity curve calculation
        df['equity'] = initial_balance + df['profit_loss'].cumsum()
        max_equity = df['equity'].cummax()
        drawdowns = (df['equity'] - max_equity)
        max_drawdown_abs = abs(drawdowns.min())
        max_drawdown_pct = abs((drawdowns / max_equity).min())
        
        total_return_abs = df['equity'].iloc[-1] - initial_balance
        total_return_pct = total_return_abs / initial_balance
        
        avg_rr = df['realised_rr'].mean()
        
        duration = (df['exit_time'] - df['entry_time']).dt.total_seconds() / 60
        avg_trade_duration = duration.mean()
        
        # Sharpe Ratio (simplified: based on trade returns)
        # Using 0.02 as default risk-free rate if not provided
        rf = 0.02 / 252 # Daily to per-trade approx or just 0 for simplicity in this context
        returns = df['profit_loss'] / initial_balance
        sharpe = (returns.mean() - rf) / returns.std() * np.sqrt(252) if returns.std() > 0 else 0
        
        # Exit reason breakdown
        exit_counts = df['exit_reason'].value_counts(normalize=True).to_dict()
        exit_reasons_pct = {k: float(v * 100) for k, v in exit_counts.items()}
        
        return {
            'total_trades': total_trades,
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'max_drawdown_pct': max_drawdown_pct,
            'max_drawdown_abs': max_drawdown_abs,
            'total_return_pct': total_return_pct,
            'total_return_abs': total_return_abs,
            'average_rr': avg_rr,
            'sharpe_ratio': sharpe,
            'exit_reasons': exit_reasons_pct,
            'avg_trade_duration_minutes': avg_trade_duration,
            'best_trade': df.loc[df['profit_loss'].idxmax()].to_dict(),
            'worst_trade': df.loc[df['profit_loss'].idxmin()].to_dict()
        }

class EquityCurvePlotter:
    @staticmethod
    def plot(trade_log: List[TradeLog], initial_balance: float, symbol: str, output_dir: str):
        if not trade_log: return
        
        df = pd.DataFrame([t.__dict__ for t in trade_log])
        df['equity'] = initial_balance + df['profit_loss'].cumsum()
        
        plt.figure(figsize=(12, 7))
        plt.plot(df['exit_time'], df['equity'], label='Equity Curve', color='#2ecc71', linewidth=2)
        
        # Shade drawdown
        max_equity = df['equity'].cummax()
        plt.fill_between(df['exit_time'], df['equity'], max_equity, color='red', alpha=0.2)
        
        # Markers
        for _, row in df.iterrows():
            color = 'green' if row['direction'] == 'BUY' else 'red'
            marker = '^' if row['direction'] == 'BUY' else 'v'
            plt.scatter(row['entry_time'], initial_balance + (df.loc[df['entry_time'] >= row['entry_time'], 'profit_loss'].cumsum().iloc[0] if not df.loc[df['entry_time'] >= row['entry_time']].empty else 0), 
                        color=color, marker=marker, s=100)
        
        plt.title(f"Equity Curve - {symbol}")
        plt.xlabel("Time")
        plt.ylabel("Account Balance")
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        os.makedirs(output_dir, exist_ok=True)
        filename = f"equity_curve_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        path = os.path.join(output_dir, filename)
        plt.savefig(path)
        logger.info(f"Equity curve saved to {path}")
        plt.close()

class VectorizedBacktester:
    def __init__(self, config_path: str = None, config_dict: Dict = None):
        self.engine = SignalEngine(config_path, config_dict)
        self.risk = RiskManager(config_path, config_dict)
        if config_dict:
            self.config = config_dict['backtester']
        else:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)['backtester']
            
    def run(self, symbol: str, df_1m: pd.DataFrame, df_5m: pd.DataFrame, initial_balance: float) -> List[TradeLog]:
        trade_log = []
        balance = initial_balance
        open_trades = []
        trade_history = []
        
        # Fetch instrument config
        instr_config = self.risk.instruments.get(symbol, {})
        pip_size = instr_config.get('pip_size', 0.0001)
        pip_value = instr_config.get('pip_value', 1.0)
        
        # Pre-align timeframes if needed, but here we just iterate 1m and look up corresponding 5m
        for i in range(100, len(df_1m)):
            current_time = df_1m['timestamp'].iloc[i]
            
            # Subsets for calculations
            window_1m = df_1m.iloc[i-100:i+1] # Lookback for EMA/ATR
            # Find corresponding 5m window
            window_5m = df_5m[df_5m['timestamp'] <= current_time].iloc[-50:]
            
            if len(window_5m) < 20: continue
            
            # 1. Manage existing trades
            current_price = df_1m['close'].iloc[i]
            for trade in open_trades[:]:
                exit_reason = None
                exit_price = 0

                # Check TP1 if not hit yet — activates trail on full position
                if not trade.tp1_hit:
                    hit_tp1 = (trade.direction == 'BUY' and current_price >= trade.tp1_price) or \
                              (trade.direction == 'SELL' and current_price <= trade.tp1_price)
                    if hit_tp1:
                        trade.tp1_hit = True
                        trade.sl_price = trade.entry_price  # move SL to break-even

                # Update trailing stop if TP1 hit
                if trade.tp1_hit:
                    new_sl = self.risk.update_trailing_stop(trade, current_price, window_1m, True)
                    if new_sl != trade.sl_price:
                        trade.sl_price = new_sl

                # Check exits: SL, TP2, or trailing SL
                hit_sl = (trade.direction == 'BUY' and current_price <= trade.sl_price) or \
                         (trade.direction == 'SELL' and current_price >= trade.sl_price)
                hit_tp2 = (trade.direction == 'BUY' and current_price >= trade.tp2_price) or \
                          (trade.direction == 'SELL' and current_price <= trade.tp2_price)

                if hit_sl:
                    exit_reason = 'TRAIL' if trade.tp1_hit else 'SL'
                    exit_price = trade.sl_price
                elif hit_tp2:
                    exit_reason = 'TP2'
                    exit_price = trade.tp2_price

                if exit_reason:
                    if trade.tp1_hit:
                        pips1 = (trade.tp1_price - trade.entry_price) if trade.direction == 'BUY' else (trade.entry_price - trade.tp1_price)
                        pips2 = (exit_price - trade.entry_price) if trade.direction == 'BUY' else (trade.entry_price - exit_price)
                        profit = (pips1 / pip_size) * pip_value * trade.lot_size * 0.5 + \
                                 (pips2 / pip_size) * pip_value * trade.lot_size * 0.5
                    else:
                        pips = (exit_price - trade.entry_price) if trade.direction == 'BUY' else (trade.entry_price - exit_price)
                        profit = (pips / pip_size) * pip_value * trade.lot_size

                    initial_sl_dist = abs(trade.entry_price - trade.sl_price) or 1e-9
                    log = TradeLog(
                        symbol, trade.direction, trade.entry_price, exit_price,
                        trade.sl_price, trade.tp1_price, trade.tp2_price,
                        trade.lot_size, profit, trade.risk_fraction * balance,
                        abs(exit_price - trade.entry_price) / initial_sl_dist,
                        trade.entry_time, current_time, exit_reason, trade.triggered_by, 'VECTORIZED'
                    )
                    trade_log.append(log)
                    balance += profit
                    open_trades.remove(trade)
                    trade_history.append(TradeResult(symbol, trade.direction, profit, trade.risk_fraction * balance, profit > 0, current_time))

            # 2. Check for new signals
            if len(open_trades) < 3:
                signal = self.engine.evaluate(symbol, window_1m, window_5m)
                if signal.direction != 'FLAT':
                    order = self.risk.evaluate(signal, window_1m, open_trades, trade_history, balance)
                    if not order.rejected:
                        setattr(order, 'entry_time', current_time)
                        setattr(order, 'triggered_by', signal.triggered_by)
                        open_trades.append(order)

        return trade_log

class EventDrivenBacktester:
    """Full implementation would reuse live modules. Here we provide a simplified 'candle replay' logic."""
    def __init__(self, config_path: str = None, config_dict: Dict = None):
        from execution import TradeManager
        self.engine = SignalEngine(config_path, config_dict)
        self.risk = RiskManager(config_path, config_dict)
        # Mocking logger_callback for TradeManager
        self.trade_log: List[TradeLog] = []
        def log_cb(event: TradeEvent):
            if event.event_type == 'CLOSED':
                self.trade_log.append(TradeLog(
                    symbol=event.symbol,
                    direction=event.direction,
                    entry_price=event.entry_price,
                    exit_price=event.exit_price,
                    sl_price=event.sl_price,
                    tp1_price=event.tp1_price,
                    tp2_price=event.tp2_price,
                    lot_size=event.lot_size,
                    profit_loss=event.profit_loss,
                    risk_amount=event.risk_amount,
                    realised_rr=event.realised_rr,
                    entry_time=event.entry_time,
                    exit_time=event.exit_time,
                    exit_reason=event.exit_reason,
                    triggered_by=event.triggered_by,
                    mode='EVENT_DRIVEN'
                ))
        self.exec_man = TradeManager(config_path, log_cb, paper_mode=True)
    def run(self, symbol: str, df_1m: pd.DataFrame, df_5m: pd.DataFrame, initial_balance: float) -> List[TradeLog]:
        # Implementation would replay candles and feed them to TradeManager.on_candle_close
        logger.info("Event-driven replay starting...")
        return []

class Backtester:
    def __init__(self, config_path: str = "config.yaml", config_dict: Dict = None):
        if config_dict:
            self.config = config_dict['backtester']
        else:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)['backtester']
        self.loader = HistoricalDataLoader(self.config)
        self.vec_tester = VectorizedBacktester(config_path, config_dict)
        self.event_tester = EventDrivenBacktester(config_path, config_dict)
        self.metrics_calc = MetricsCalculator()
        self.plotter = EquityCurvePlotter()

    def run(self, symbol: str, days: int = None, start_date: str = None, end_date: str = None, initial_balance: float = 10000, mode: str = 'VECTORIZED', plot: bool = True):
        logger.info(f"Starting {mode} backtest for {symbol}")
        
        st = pd.to_datetime(start_date) if start_date else None
        et = pd.to_datetime(end_date) if end_date else None
        
        df_1m = self.loader.load_data(symbol, '1m', days, st, et)
        df_5m = self.loader.load_data(symbol, '5m', days, st, et)
        
        if mode == 'VECTORIZED':
            trade_log = self.vec_tester.run(symbol, df_1m, df_5m, initial_balance)
        else:
            trade_log = self.event_tester.run(symbol, df_1m, df_5m, initial_balance)
            
        metrics = self.metrics_calc.calculate(trade_log, initial_balance)
        
        # --- AMD Diagnostic Printing ---
        amd_detector = self.vec_tester.engine.amd
        if amd_detector.enabled:
            stats = amd_detector.get_stats()
            total = stats['accumulation_scanned']
            if total > 0:
                print("\n" + "="*40)
                print("       AMD STRATEGY DIAGNOSTIC")
                print("="*40)
                print(f"Accumulation windows found : {total:,} (100%)")
                print(f"Ranges rejected (too wide) : {stats['ranges_rejected']:,}")
                print(f"Manipulation confirmed     : {stats['manipulation_confirmed']:,} ({stats['manipulation_confirmed']/total*100:.1f}%)")
                print(f"Distribution confirmed     : {stats['distribution_confirmed']:,} ({stats['distribution_confirmed']/total*100:.1f}%)")
                print(f"Expired without entry      : {stats['expired']:,}")
                
                amd_trades = sum(1 for t in trade_log if 'AMD' in t.triggered_by)
                total_trades = len(trade_log)
                if total_trades > 0:
                    print(f"AMD trade count            : {amd_trades} ({amd_trades/total_trades*100:.1f}% of total)")
                print("="*40 + "\n")
            
        if plot:
            self.plotter.plot(trade_log, initial_balance, symbol, self.config['output_dir'])
        
        self.save_trade_log(trade_log, symbol)
        
        return metrics, trade_log

    def save_trade_log(self, trade_log: List[TradeLog], symbol: str):
        if not trade_log:
            return
            
        df = pd.DataFrame([t.__dict__ for t in trade_log])
        
        # Ensure triggered_by is a string for CSV compatibility
        if 'triggered_by' in df.columns:
            df['triggered_by'] = df['triggered_by'].apply(lambda x: ",".join(x) if isinstance(x, list) else x)
            
        os.makedirs(self.config['output_dir'], exist_ok=True)
        filename = f"trades_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        path = os.path.join(self.config['output_dir'], filename)
        df.to_csv(path, index=False)
        logger.info(f"Trade log saved to {path}")

    def run_all(self, symbols: List[str], days: int = 30, initial_balance: float = 10000, mode: str = 'VECTORIZED'):
        all_metrics = {}
        for symbol in symbols:
            metrics, _ = self.run(symbol, days=days, initial_balance=initial_balance, mode=mode)
            if metrics:
                all_metrics[symbol] = metrics
        
        self.print_comparison_table(all_metrics)
        return all_metrics

    def print_comparison_table(self, all_metrics: Dict[str, Dict]):
        if not all_metrics:
            print("No metrics to display.")
            return
            
        header = f"{'Metric':<25}"
        for symbol in all_metrics.keys():
            header += f"| {symbol:<15}"
        
        print("\n" + "="*len(header))
        print("      MULTI-SYMBOL BACKTEST RESULTS")
        print("="*len(header))
        print(header)
        print("-" * len(header))
        
        metrics_to_show = [
            'total_trades', 'win_rate', 'profit_factor', 
            'max_drawdown_pct', 'total_return_pct', 'average_rr'
        ]
        
        for m in metrics_to_show:
            row = f"{m.replace('_', ' ').title():<25}"
            for symbol, metrics in all_metrics.items():
                val = metrics.get(m, 0)
                if isinstance(val, float):
                    row += f"| {val:<15.4f}"
                else:
                    row += f"| {val:<15}"
            print(row)
        print("="*len(header))

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    backtester = Backtester()
    symbols = ["1HZ10V", "1HZ25V", "1HZ75V"]
    backtester.run_all(symbols, days=30, mode='VECTORIZED')
