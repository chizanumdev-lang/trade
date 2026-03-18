import json
import os
from datetime import datetime


class MT5Bridge:
    """Mirrors every TradeEvent as a human-readable log that matches MT5 trade history."""

    def __init__(self, config: dict):
        self.enabled = config.get('mt5_bridge', {}).get('enabled', False)
        self.log_path = 'logs/mt5_mirror.json'
        self.trades = []
        os.makedirs('logs', exist_ok=True)

    def on_trade_event(self, event):
        if not self.enabled:
            return
        record = {
            'timestamp': datetime.now().isoformat(),
            'symbol': event.symbol,
            'direction': event.direction,
            'event_type': event.event_type,
            'price': event.price,
            'sl': event.sl_price,
            'tp': event.tp_price,
            'lot_size': event.lot_size,
            'pnl': event.profit_loss,
            'broker': event.broker,
            'reason': event.reason or '',
        }
        self.trades.append(record)
        with open(self.log_path, 'w') as f:
            json.dump(self.trades, f, indent=2)
        self._print_mt5_style(record)

    def _print_mt5_style(self, r):
        ts = r['timestamp'][:19]
        sym = r['symbol']
        ev = r['event_type']
        n = len(self.trades)
        if ev == 'OPENED':
            sl = f"{r['sl']:.5f}" if r['sl'] else '—'
            tp = f"{r['tp']:.5f}" if r['tp'] else '—'
            print(f"[{ts}] {sym} {r['direction']} #{n} "
                  f"OPEN  @ {r['price']:.5f}  "
                  f"SL:{sl}  TP:{tp}  "
                  f"Lot:{r['lot_size']:.2f}")
        elif ev == 'CLOSED':
            pnl = r['pnl'] or 0
            sign = '+' if pnl >= 0 else ''
            print(f"[{ts}] {sym} {r['direction']} #{n} "
                  f"CLOSE @ {r['price']:.5f}  "
                  f"P&L: {sign}{pnl:.4f}  Reason: {r['reason']}")
        elif ev == 'SL_UPDATED':
            sl = f"{r['sl']:.5f}" if r['sl'] else '—'
            print(f"[{ts}] {sym} SL moved → {sl}")
        elif ev == 'PARTIAL_CLOSE':
            print(f"[{ts}] {sym} PARTIAL CLOSE @ {r['price']:.5f}  "
                  f"(TP1 hit — half 1 closed)")
        elif ev == 'BE_MOVED':
            sl = f"{r['sl']:.5f}" if r['sl'] else '—'
            print(f"[{ts}] {sym} BE triggered — SL → {sl}")
        elif ev == 'REJECTED':
            print(f"[{ts}] {sym} REJECTED — {r['reason']}")
