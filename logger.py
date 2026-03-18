import json
import os
from datetime import datetime, date


class Logger:
    def __init__(self, config: dict):
        self.config = config
        os.makedirs('logs', exist_ok=True)
        self.trade_log = []
        self.signal_log = []
        self.error_log = []
        self.daily_stats = {}
        self.session_start = datetime.now()
        self.session_start_balance = None
        self._print_header()

    def _print_header(self):
        syms = self.config.get('trading', {}).get('symbols', ['1HZ10V', '1HZ75V'])
        print("\n" + "="*55)
        print("  OHLC MOMENTUM BOT — DEMO")
        print(f"  Started: {self.session_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Symbols: {', '.join(syms)}")
        print("="*55 + "\n")

    def set_opening_balance(self, balance: float):
        self.session_start_balance = balance
        print(f"Opening balance: ${balance:,.2f}\n")

    def log_signal(self, signal):
        record = {
            'time': datetime.now().isoformat(),
            'symbol': signal.symbol,
            'direction': signal.direction,
            'confidence': round(signal.confidence, 3),
            'triggered_by': signal.triggered_by,
            'entry_price': signal.entry_price
        }
        self.signal_log.append(record)
        if signal.direction != 'FLAT':
            print(f"[SIGNAL] {signal.symbol} {signal.direction} "
                  f"conf={signal.confidence:.2f} "
                  f"@ {signal.entry_price:.2f}")
        self._save('logs/signals.json', self.signal_log)

    def log_trade_event(self, event):
        record = {
            'time': datetime.now().isoformat(),
            'symbol': event.symbol,
            'direction': event.direction,
            'event_type': event.event_type,
            'price': event.price,
            'sl': event.sl_price,
            'tp': event.tp_price,
            'lot_size': event.lot_size,
            'pnl': event.profit_loss,
            'reason': event.reason,
            'broker': event.broker
        }
        self.trade_log.append(record)
        self._print_trade(record)
        self._update_daily(record)
        self._save('logs/trades.json', self.trade_log)

    def _print_trade(self, r):
        t = r['time'][11:19]
        sym = r['symbol']
        ev = r['event_type']
        if ev == 'OPENED':
            sl = f"{r['sl']:.2f}" if r['sl'] else '—'
            tp = f"{r['tp']:.2f}" if r['tp'] else '—'
            print(f"[{t}] OPEN  {sym} {r['direction']} "
                  f"@ {r['price']:.2f}  "
                  f"SL:{sl}  TP:{tp}  "
                  f"Lot:{r['lot_size']:.2f}")
        elif ev == 'CLOSED':
            pnl = r['pnl'] or 0
            sign = '+' if pnl >= 0 else ''
            emoji = 'WIN ' if pnl >= 0 else 'LOSS'
            print(f"[{t}] {emoji} {sym} CLOSED "
                  f"@ {r['price']:.2f}  "
                  f"P&L: {sign}${pnl:.2f}  "
                  f"({r['reason']})")
        elif ev == 'SL_UPDATED':
            print(f"[{t}] TRAIL {sym} SL → {r['sl']:.2f}")
        elif ev == 'PARTIAL_CLOSE':
            print(f"[{t}] PART  {sym} half closed @ {r['price']:.2f} (TP1)")
        elif ev == 'REJECTED':
            print(f"[{t}] SKIP  {sym} — {r['reason']}")

    def _update_daily(self, r):
        today = date.today().isoformat()
        if today not in self.daily_stats:
            self.daily_stats[today] = {'trades': 0, 'wins': 0, 'losses': 0, 'pnl': 0.0}
        d = self.daily_stats[today]
        if r['event_type'] == 'CLOSED':
            pnl = r['pnl'] or 0
            d['trades'] += 1
            d['pnl'] += pnl
            if pnl >= 0:
                d['wins'] += 1
            else:
                d['losses'] += 1
            self._print_daily_summary(today, d)
        self._save('logs/daily.json', self.daily_stats)

    def _print_daily_summary(self, day, d):
        wr = d['wins'] / d['trades'] * 100 if d['trades'] > 0 else 0
        sign = '+' if d['pnl'] >= 0 else ''
        print(f"\n  Today ({day}): "
              f"{d['trades']} trades | "
              f"WR {wr:.0f}% | "
              f"P&L {sign}${d['pnl']:.2f}\n")

    def log_error(self, source: str, message: str):
        record = {'time': datetime.now().isoformat(), 'source': source, 'message': message}
        self.error_log.append(record)
        print(f"[ERROR] {source}: {message}")
        self._save('logs/errors.json', self.error_log)

    def get_trade_history(self) -> list:
        return [t for t in self.trade_log if t['event_type'] == 'CLOSED']

    def print_session_summary(self, current_balance: float):
        closed = [t for t in self.trade_log if t['event_type'] == 'CLOSED']
        if not closed:
            print("\nNo closed trades this session.")
            return
        wins = [t for t in closed if (t['pnl'] or 0) >= 0]
        total_pnl = sum(t['pnl'] or 0 for t in closed)
        wr = len(wins) / len(closed) * 100
        duration = datetime.now() - self.session_start
        print("\n" + "="*55)
        print("  SESSION SUMMARY")
        print("="*55)
        print(f"  Duration     : {str(duration).split('.')[0]}")
        print(f"  Closed trades: {len(closed)}")
        print(f"  Win rate     : {wr:.1f}%")
        sign = '+' if total_pnl >= 0 else ''
        print(f"  Net P&L      : {sign}${total_pnl:.2f}")
        if self.session_start_balance:
            ret = (total_pnl / self.session_start_balance) * 100
            sign2 = '+' if ret >= 0 else ''
            print(f"  Return       : {sign2}{ret:.2f}%")
        print(f"  Open balance : ${current_balance:,.2f}")
        print("="*55 + "\n")

    def _save(self, path: str, data):
        with open(path, 'w') as f:
            json.dump(data, f, indent=2, default=str)
