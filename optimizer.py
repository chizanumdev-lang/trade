import pandas as pd
import numpy as np
import yaml
import os
import logging
import multiprocessing
from itertools import product
from copy import deepcopy
from datetime import datetime
from typing import List, Dict, Any, Tuple

# Internal modules
from backtester import Backtester

logger = logging.getLogger("Optimizer")

def scoring_function(metrics: Dict) -> float:
    """Fitness function to maximize profit factor while respecting constraints."""
    wr = metrics.get('win_rate', 0)
    dd = metrics.get('max_drawdown_pct', 0)
    trades = metrics.get('total_trades', 0)
    pf = metrics.get('profit_factor', 0)
    sharpe = metrics.get('sharpe_ratio', 0)
    
    if wr < 0.43: return 0
    if dd > 0.10: return 0
    if trades < 100: return 0
    
    # Fitness = PF * (1 - DD) * Sharpe
    return pf * (1 - dd) * sharpe

def run_single_backtest(args: Tuple[Dict, str, str, str]) -> Dict:
    """Worker function for multiprocessing."""
    base_config, symbol, start_date, end_date = args
    try:
        # Create backtester with the specific config
        bt = Backtester(config_dict=base_config)
        # Run vectorized backtest (plot=False for speed)
        metrics, _ = bt.run(symbol, start_date=start_date, end_date=end_date, mode='VECTORIZED', plot=False)
        
        if not metrics:
            return None
            
        metrics['score'] = scoring_function(metrics)
        metrics['params'] = base_config
        return metrics
    except Exception as e:
        # logging.error(f"Error in backtest: {e}")
        return None

class Optimizer:
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.base_config = yaml.safe_load(f)
        
    def generate_grid(self) -> List[Dict]:
        """Generates a streamlined parameter grid for quick diagnostic."""
        ema_fast_range = [7, 9, 11]
        ema_slow_range = [21, 26, 30]
        atr_period_range = [14]
        atr_multiplier_range = [1.25, 1.5, 1.75]
        min_body_ratio_range = [0.2, 0.3]
        confluence_mode_range = ['EMA_PLUS_1']
        swing_n_range = [3]
        
        all_combinations = list(product(
            ema_fast_range, ema_slow_range, 
            atr_period_range, atr_multiplier_range,
            min_body_ratio_range, confluence_mode_range, 
            swing_n_range
        ))
        
        valid_configs = []
        for combo in all_combinations:
            ef, es, ap, am, mbr, cm, sn = combo
            
            # Constraint: ema_fast < ema_slow with min gap of 6
            if es - ef < 6:
                continue
            
            # Create config clone
            config = deepcopy(self.base_config)
            # Update Signal Engine
            config['signal_engine']['ema']['fast'] = ef
            config['signal_engine']['ema']['slow'] = es
            config['signal_engine']['supply_demand']['min_body_ratio'] = mbr
            config['signal_engine']['supply_demand']['confluence_mode'] = cm
            config['signal_engine']['supply_demand']['adx_enabled'] = False # As requested for synthetics
            config['signal_engine']['fibonacci']['swing_n'] = sn
            
            # Update Risk Manager
            config['risk_manager']['stop_loss']['atr_period'] = ap
            config['risk_manager']['stop_loss']['atr_multiplier'] = am
            
            valid_configs.append(config)
            
        return valid_configs

    def optimize(self, symbol: str, start_date: str, end_date: str):
        configs = self.generate_grid()
        total = len(configs)
        print(f"Starting Grid Search: {total} combinations found.")
        
        args = [(cfg, symbol, start_date, end_date) for cfg in configs]
        
        results = []
        best_score = 0
        
        # Run synchronously for stability in this environment
        for i, cfg in enumerate(configs):
            result = run_single_backtest((cfg, symbol, start_date, end_date))
            if result:
                results.append(result)
                if result['score'] > best_score:
                    best_score = result['score']
            
            if (i + 1) % 10 == 0 or (i + 1) == total:
                print(f"Testing {i+1}/{total}... best score so far: {best_score:.4f}")
        
        # Sort by score or by PF if all scores are 0
        all_results = [r for r in results if r is not None]
        all_results.sort(key=lambda x: (x['score'], x['profit_factor']), reverse=True)
        
        if all_results:
            self.print_leaderboard(all_results, symbol)
            best = all_results[0]
            
            # Print Exit Reason Diagnostic for the best combo
            print("\nBEST COMBO EXIT REASON BREAKDOWN:")
            print("-" * 35)
            reasons = best.get('exit_reasons', {})
            for reason, pct in reasons.items():
                print(f"{reason:<15}: {pct:>6.1f}%")
            print("-" * 35)
            
            # Only save and guard if it's actually "good" (score > 0)
            # or if the user just wants the best of the bunch
            self.save_best_config(best['params'])
            self.save_all_results(all_results)
            self.run_overfitting_guard(best, symbol)
        else:
            print(f"\nNo parameter combinations were successful for {symbol}.")

    def print_leaderboard(self, results: List[Dict], symbol: str):
        print(f"\nOPTIMISATION RESULTS — {symbol}")
        print("=" * 110)
        header = f"{'Rank':<5} | {'EMA':<10} | {'ATR':<12} | {'Body':<6} | {'Conf':<6} | {'WR':<7} | {'PF':<6} | {'DD':<6} | {'Return':<8} | {'Score':<6}"
        print(header)
        print("-" * 110)
        
        for i, r in enumerate(results[:10]):
            p = r['params']['signal_engine']
            rm = r['params']['risk_manager']['stop_loss']
            ema_str = f"{p['ema']['fast']}/{p['ema']['slow']}"
            atr_str = f"{rm['atr_period']}/{rm['atr_multiplier']}"
            body = p['supply_demand']['min_body_ratio']
            conf = "E+1" if p['supply_demand']['confluence_mode'] == 'EMA_PLUS_1' else "E+2"
            
            print(f"{i+1:<5} | {ema_str:<10} | {atr_str:<12} | {body:<6} | {conf:<6} | {r['win_rate']*100:>5.1f}% | {r['profit_factor']:<6.2f} | {r['max_drawdown_pct']*100:>5.1f}% | {r['total_return_pct']*100:>+7.1f}% | {r['score']:<6.2f}")
        
        print("=" * 110)

    def save_best_config(self, best_config: Dict):
        with open("config.yaml", 'w') as f:
            yaml.dump(best_config, f, default_flow_style=False)
        print("\nBest config written to config.yaml")

    def save_all_results(self, results: List[Dict]):
        if not results: return
        data = []
        for r in results:
            p = r['params']['signal_engine']
            rm = r['params']['risk_manager']['stop_loss']
            row = {
                'ema_fast': p['ema']['fast'],
                'ema_slow': p['ema']['slow'],
                'atr_period': rm['atr_period'],
                'atr_multiplier': rm['atr_multiplier'],
                'min_body_ratio': p['supply_demand']['min_body_ratio'],
                'confluence_mode': p['supply_demand']['confluence_mode'],
                'swing_n': p['fibonacci']['swing_n'],
                'win_rate': r['win_rate'],
                'profit_factor': r['profit_factor'],
                'max_drawdown_pct': r['max_drawdown_pct'],
                'total_return_pct': r['total_return_pct'],
                'total_trades': r['total_trades'],
                'sharpe_ratio': r['sharpe_ratio'],
                'score': r['score']
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        os.makedirs("outputs", exist_ok=True)
        filename = f"outputs/optimization_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False)
        print(f"Full results saved to {filename}")

    def run_overfitting_guard(self, best_result: Dict, symbol: str):
        print("\nRUNNING OVERFITTING GUARD (Out-of-Sample)...")
        oos_start = '2025-10-01'
        oos_end = '2025-12-31'
        
        bt = Backtester(config_dict=best_result['params'])
        oos_metrics, _ = bt.run(symbol, start_date=oos_start, end_date=oos_end, mode='VECTORIZED', plot=False)
        
        if not oos_metrics:
            print("Could not run OOS backtest.")
            return

        is_pf = best_result['profit_factor']
        oos_pf = oos_metrics['profit_factor']
        
        print("-" * 50)
        print(f"{'Metric':<20} | {'In-Sample':<12} | {'Out-of-Sample':<12}")
        print("-" * 50)
        print(f"{'Profit Factor':<20} | {is_pf:<12.2f} | {oos_pf:<12.2f}")
        print(f"{'Win Rate':<20} | {best_result['win_rate']*100:>11.1f}% | {oos_metrics['win_rate']*100:>11.1f}%")
        print(f"{'Return':<20} | {best_result['total_return_pct']*100:>+11.1f}% | {oos_metrics['total_return_pct']*100:>+11.1f}%")
        print("-" * 50)
        
        if oos_pf < is_pf * 0.7:
            print("⚠️ WARNING: OUT-OF-SAMPLE PROFIT FACTOR DROPPED > 30%. STRATEGY MAY BE OVERFITTED!")
        else:
            print("✅ Out-of-Sample performance is stable. Overfitting risk appears low.")

if __name__ == "__main__":
    # Ensure multiprocessing works on Mac/Windows
    multiprocessing.freeze_support()
    
    opt = Optimizer()
    # Optimize on 1HZ10V for the requested range
    opt.optimize('1HZ10V', start_date='2026-01-01', end_date='2026-03-17')
