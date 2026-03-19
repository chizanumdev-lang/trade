import asyncio
import logging
import yaml
import pandas as pd
from dataclasses import dataclass
from typing import List, Optional, Literal, Dict, Callable
from datetime import datetime

# Import TradeOrder from risk_manager to use it in types
from risk_manager import TradeOrder

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

try:
    from deriv_api import DerivAPI
except ImportError:
    DerivAPI = None

@dataclass(frozen=True)
class TradeEvent:
    event_type: Literal['OPENED', 'PARTIAL_CLOSE', 'BE_MOVED', 'SL_UPDATED', 'CLOSED', 'REJECTED']
    symbol: str
    direction: str
    lot_size: float
    price: float
    sl_price: Optional[float]
    tp_price: Optional[float]
    profit_loss: Optional[float] = None
    reason: Optional[str] = None
    timestamp: datetime = datetime.now()
    broker: Literal['DERIV', 'MT5', 'PAPER'] = 'PAPER'

class BaseExecutor:
    async def place_limit_order(self, order: TradeOrder) -> bool:
        raise NotImplementedError
        
    async def place_market_order(self, order: TradeOrder) -> bool:
        raise NotImplementedError
        
    async def modify_order(self, order: TradeOrder, new_sl: float, new_tp: float) -> bool:
        raise NotImplementedError
        
    async def close_position(self, order: TradeOrder, lot_size: float, exit_price: float) -> Optional[float]:
        raise NotImplementedError
        
    def get_current_spread(self, symbol: str) -> float:
        raise NotImplementedError

class DerivExecutor(BaseExecutor):
    def __init__(self, api_token: str):
        self.api_token = api_token
        self.api = None
        self.contract_ids = {} # symbol -> contract_id

    async def connect(self):
        if DerivAPI:
            # Note: This is an async constructor in deriv_api usually
            # self.api = await DerivAPI(app_id=...)
            pass

    async def place_market_order(self, order: TradeOrder) -> bool:
        if not self.api: return False
        
        payload = {
            "buy": 1,
            "price": order.lot_size,
            "parameters": {
                "amount": order.risk_amount,
                "basis": "stake",
                "contract_type": "CALL" if order.direction == "BUY" else "PUT",
                "currency": "USD",
                "duration": 1,
                "duration_unit": "m",
                "symbol": order.symbol
            }
        }
        # In a real implementation: result = await self.api.send(payload)
        # For now, we simulate success if api is "connected"
        return True

    async def close_position(self, order: TradeOrder, lot_size: float, exit_price: float) -> Optional[float]:
        # Placeholder: await self.api.sell({'sell': self.contract_ids[order.symbol]})
        return 0.0

    def get_current_spread(self, symbol: str) -> float:
        return 0.0 # Placeholder: fetch from ticks

class MT5Executor(BaseExecutor):
    def __init__(self):
        if mt5 and not mt5.initialize():
            logger.error("MT5 initialize failed")

    async def place_limit_order(self, order: TradeOrder) -> bool:
        if not mt5: return False
        request = {
            "action": mt5.TRADE_ACTION_PENDING,
            "symbol": order.symbol,
            "volume": order.lot_size,
            "type": mt5.ORDER_TYPE_BUY_LIMIT if order.direction == 'BUY' else mt5.ORDER_TYPE_SELL_LIMIT,
            "price": order.entry_price,
            "sl": order.sl_price,
            "tp": order.tp2_price,
            "magic": 123456,
            "comment": "Antigravity Bot",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        return result.retcode == mt5.TRADE_RETCODE_DONE if result else False

    async def place_market_order(self, order: TradeOrder) -> bool:
        if not mt5: return False
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": order.symbol,
            "volume": order.lot_size,
            "type": mt5.ORDER_TYPE_BUY if order.direction == 'BUY' else mt5.ORDER_TYPE_SELL,
            "price": mt5.symbol_info_tick(order.symbol).ask if order.direction == 'BUY' else mt5.symbol_info_tick(order.symbol).bid,
            "sl": order.sl_price,
            "tp": order.tp2_price,
            "magic": 123456,
            "comment": "Antigravity Bot",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        return result.retcode == mt5.TRADE_RETCODE_DONE if result else False

    async def modify_order(self, order: TradeOrder, new_sl: float, new_tp: float) -> bool:
        # Placeholder for TRADE_ACTION_SLTP
        return True

    async def close_position(self, order: TradeOrder, lot_size: float, exit_price: float) -> Optional[float]:
        # Placeholder for TRADE_ACTION_DEAL (closing)
        return 0.0

    def get_current_spread(self, symbol: str) -> float:
        if not mt5: return 0.0
        info = mt5.symbol_info(symbol)
        return info.spread if info else 0.0

class PaperExecutor(BaseExecutor):
    def __init__(self):
        self.open_prices = {}

    async def place_limit_order(self, order: TradeOrder) -> bool:
        self.open_prices[order.symbol] = order.entry_price
        return True
        
    async def place_market_order(self, order: TradeOrder) -> bool:
        self.open_prices[order.symbol] = order.entry_price
        return True
        
    async def modify_order(self, order: TradeOrder, new_sl: float, new_tp: float) -> bool:
        return True
        
    async def close_position(self, order: TradeOrder, lot_size: float, exit_price: float) -> Optional[float]:
        entry = self.open_prices.get(order.symbol, order.entry_price)
        pips = (exit_price - entry) if order.direction == 'BUY' else (entry - exit_price)
        # Assuming 100 multiplier for simplicity in paper mode, matching previous logic but fixed
        return pips * 100 * lot_size

    def get_current_spread(self, symbol: str) -> float:
        return 0.5 # Mock spread

class OrderRouter:
    def __init__(self, routing_config: Dict):
        self.routing = routing_config
        
    def get_broker(self, symbol: str) -> str:
        for broker, symbols in self.routing.items():
            if symbol in symbols:
                return broker
        if any(idx in symbol for idx in ['Volatility', 'Boom', 'Crash', 'Step', 'V75']):
            return 'DERIV'
        return 'MT5'

class TradeManager:
    def __init__(self, config_path: str, logger_callback: Callable[[TradeEvent], None], paper_mode: bool = False):
        with open(config_path, 'r') as f:
            full_config = yaml.safe_load(f)
            self.config = full_config['execution']
            self.routing_config = full_config.get('routing', {})
            self.instr_config = full_config.get('instruments', {})
            
        self.logger_callback = logger_callback
        self.router = OrderRouter(self.routing_config)
        self.paper_mode = paper_mode
        self.lock = asyncio.Lock()
        
        from mt5_bridge import MT5Bridge
        self.mt5 = MT5Bridge(full_config)
        
        self.executors = {
            'PAPER': PaperExecutor(),
        }
        if not paper_mode:
            self.executors['DERIV'] = DerivExecutor("DUMMY_TOKEN")
            self.executors['MT5'] = MT5Executor()
        
        self.active_trades: Dict[str, TradeOrder] = {}
        self.partial_closed_symbols = set()
        self.trade_count = 0

    def _get_executor(self, symbol: str) -> BaseExecutor:
        if self.paper_mode:
            return self.executors['PAPER']
        broker = self.router.get_broker(symbol)
        return self.executors.get(broker, self.executors['PAPER'])

    async def execute(self, order: TradeOrder) -> Optional[TradeEvent]:
        symbol = order.symbol
        executor = self._get_executor(symbol)
        limit_timeout = self.config.get('limit_timeout_seconds', 10)

        async with self.lock:
            if symbol in self.active_trades:
                self.logger_callback(TradeEvent('REJECTED', symbol, order.direction, 0, 0, None, None, reason="Symbol already active"))
                return None

            # Spread Check
            spread = executor.get_current_spread(symbol)
            max_spread = self.instr_config.get(symbol, {}).get('max_spread_pips', 999)
            pip_val = self.instr_config.get(symbol, {}).get('pip_value', 0.0001)
            if spread * pip_val > max_spread * pip_val:
                self.logger_callback(TradeEvent('REJECTED', symbol, order.direction, 0, 0, None, None, reason=f"Spread too wide: {spread}"))
                return None

            # 2. Limit Order Entry with Timeout
            # Note: In real scenarios, this would wait for a fill event. 
            # We simulate the "fallback" logic by checking success.
            success = await executor.place_limit_order(order)
            
            # Simulate timeout fallback (if still active but not filled)
            # if not success or timeout_hit:
            if not success:
                # Fallback to market
                success = await executor.place_market_order(order)
                if not success:
                    self.logger_callback(TradeEvent('REJECTED', symbol, order.direction, 0, 0, None, None, reason="Broker rejected entry"))
                    return None

            # 3. Successful entry
            self.active_trades[symbol] = order
            self.trade_count += 1
            event = TradeEvent(
                'OPENED', symbol, order.direction, order.lot_size, 
                order.entry_price, order.sl_price, order.tp2_price, 
                broker=self.router.get_broker(symbol) if not self.paper_mode else 'PAPER'
            )
            self.logger_callback(event)
            self.mt5.on_trade_event(event)
            return event

    async def on_candle_close(self, symbol: str, current_price: float, candles_1m: pd.DataFrame, risk_manager):
        if symbol not in self.active_trades:
            return

        order = self.active_trades[symbol]
        executor = self._get_executor(symbol)
        broker_name = self.router.get_broker(symbol) if not self.paper_mode else 'PAPER'

        # 1. SL / TP / BE Logic
        # Break Even
        if risk_manager.check_break_even(order, current_price):
            # Update SL to BE (Entry + 1 pip)
            pip_val = self.instr_config.get(symbol, {}).get('pip_value', 0.0001)
            new_sl = order.entry_price + (pip_val if order.direction == 'BUY' else -pip_val)
            if (order.direction == 'BUY' and new_sl > order.sl_price) or (order.direction == 'SELL' and new_sl < order.sl_price):
                await executor.modify_order(order, new_sl, order.tp2_price)
                # Update local state (dataclasses are immutable so we replace)
                self.active_trades[symbol] = dataclass_replace(order, sl_price=new_sl)
                be_event = TradeEvent('BE_MOVED', symbol, order.direction, order.lot_size, current_price, new_sl, order.tp2_price, broker=broker_name)
                self.logger_callback(be_event)
                self.mt5.on_trade_event(be_event)

        # Trailing Stop
        new_sl = risk_manager.update_trailing_stop(order, current_price, candles_1m, order.tp1_hit)
        if new_sl != order.sl_price:
            await executor.modify_order(order, new_sl, order.tp2_price)
            self.active_trades[symbol] = dataclass_replace(self.active_trades[symbol], sl_price=new_sl)
            sl_event = TradeEvent('SL_UPDATED', symbol, order.direction, order.lot_size, current_price, new_sl, order.tp2_price, broker=broker_name)
            self.logger_callback(sl_event)
            self.mt5.on_trade_event(sl_event)

        # Partial Close (TP1)
        if symbol not in self.partial_closed_symbols:
            hit_tp1 = (order.direction == 'BUY' and current_price >= order.tp1_price) or \
                      (order.direction == 'SELL' and current_price <= order.tp1_price)
            if hit_tp1:
                p_lot = order.lot_size * 0.5
                p_profit = await executor.close_position(order, p_lot, current_price)
                self.partial_closed_symbols.add(symbol)
                # Update order to set tp1_hit
                self.active_trades[symbol] = dataclass_replace(self.active_trades[symbol], tp1_hit=True)
                partial_event = TradeEvent('PARTIAL_CLOSE', symbol, order.direction, p_lot, current_price, order.sl_price, order.tp2_price, profit_loss=p_profit, broker=broker_name)
                self.logger_callback(partial_event)
                self.mt5.on_trade_event(partial_event)

        # Full Close (TP2 or SL)
        hit_tp2 = (order.direction == 'BUY' and current_price >= order.tp2_price) or \
                  (order.direction == 'SELL' and current_price <= order.tp2_price)
        hit_sl = (order.direction == 'BUY' and current_price <= order.sl_price) or \
                 (order.direction == 'SELL' and current_price >= order.sl_price)
        
        if hit_tp2 or hit_sl:
            remaining_lot = order.lot_size * 0.5 if symbol in self.partial_closed_symbols else order.lot_size
            profit = await executor.close_position(order, remaining_lot, current_price)
            reason = "TP2 Hit" if hit_tp2 else "SL Hit"
            close_event = TradeEvent('CLOSED', symbol, order.direction, remaining_lot, current_price, order.sl_price, order.tp2_price, profit_loss=profit, reason=reason, broker=broker_name)
            self.logger_callback(close_event)
            self.mt5.on_trade_event(close_event)
            del self.active_trades[symbol]
            if symbol in self.partial_closed_symbols:
                self.partial_closed_symbols.remove(symbol)

    async def on_signal(self, signal):
        """Handle opposite signal: close current trade before opening new one."""
        symbol = signal.symbol
        if symbol in self.active_trades:
            order = self.active_trades[symbol]
            if order.direction != signal.direction:
                executor = self._get_executor(symbol)
                remaining_lot = order.lot_size * 0.5 if symbol in self.partial_closed_symbols else order.lot_size
                profit = await executor.close_position(order, remaining_lot, signal.entry_price)
                self.logger_callback(TradeEvent('CLOSED', symbol, order.direction, remaining_lot, signal.entry_price, order.sl_price, order.tp2_price, profit_loss=profit, reason="Opposite Signal", broker='PAPER'))
                del self.active_trades[symbol]
                if symbol in self.partial_closed_symbols:
                    self.partial_closed_symbols.remove(symbol)

def dataclass_replace(obj, **kwargs):
    from dataclasses import replace
    return replace(obj, **kwargs)

if __name__ == "__main__":
    def test_logger(event: TradeEvent):
        pnl_str = f" | P&L: ${event.profit_loss:.2f}" if event.profit_loss is not None else ""
        print(f"[{event.timestamp.strftime('%H:%M:%S')}] {event.event_type}: {event.symbol} {event.direction} @ {event.price:.5f} | Lot: {event.lot_size} | Reason: {event.reason or '-'}{pnl_str}")

    async def run_sim():
        manager = TradeManager("config.yaml", test_logger, paper_mode=True)
        from risk_manager import RiskManager
        risk = RiskManager("config.yaml")
        
        # 1. Simulate Signal & Entry
        mock_order = TradeOrder(
            symbol="EURUSD", direction="BUY", lot_size=1.0, entry_price=1.1000,
            sl_price=1.0990, tp1_price=1.1010, tp2_price=1.1020,
            be_trigger_price=1.1005, trail_distance=0.0005,
            risk_fraction=0.01, kelly_fraction=0.02, atr_value=0.0005, rejected=False
        )
        
        print("--- Step 1: Entry ---")
        await manager.execute(mock_order)
        
        # 2. Simulate Price Move to BE Trigger
        print("\n--- Step 2: Price moves to 1.1006 (BE Trigger) ---")
        df_mock = pd.DataFrame({'close': [1.1000]*20, 'high': [1.1000]*20, 'low': [1.1000]*20})
        await manager.on_candle_close("EURUSD", 1.1006, df_mock, risk)
        
        # 3. Simulate Price Move to TP1 (Partial Close)
        print("\n--- Step 3: Price moves to 1.1011 (TP1 Hit) ---")
        await manager.on_candle_close("EURUSD", 1.1011, df_mock, risk)
        
        # 4. Simulate Price Move to SL (at BE) or TP2
        print("\n--- Step 4: Price moves to 1.1021 (TP2 Hit) ---")
        await manager.on_candle_close("EURUSD", 1.1021, df_mock, risk)

        # 5. Simulate Opposite Signal
        print("\n--- Step 5: Opposite Signal Test ---")
        await manager.execute(mock_order)
        opposite_signal = dataclass_replace(mock_order, direction="SELL", entry_price=1.1015)
        await manager.on_signal(opposite_signal)

    asyncio.run(run_sim())
