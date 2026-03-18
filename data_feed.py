import asyncio
import logging
import time
import yaml
from dataclasses import dataclass
from collections import deque
from typing import Dict, List, Optional, Callable
import pandas as pd
# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("DataFeed")

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None
    logger.warning("MetaTrader5 library not found. MT5Feed will be disabled unless running on Windows.")
from deriv_api import DerivAPI
import websockets

@dataclass(frozen=True)
class Tick:
    symbol: str
    timestamp: float
    bid: float
    ask: float
    mid: float

@dataclass(frozen=True)
class Candle:
    symbol: str
    timeframe: str  # '1m', '5m', 'tick'
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float

class CandleBuilder:
    """Stateless utility to build candles from ticks."""
    
    @staticmethod
    def build_time_candle(ticks: List[Tick], symbol: str, timeframe: str, timestamp: float) -> Candle:
        if not ticks:
            return None
        
        prices = [t.mid for t in ticks]
        return Candle(
            symbol=symbol,
            timeframe=timeframe,
            timestamp=timestamp,
            open=prices[0],
            high=max(prices),
            low=min(prices),
            close=prices[-1],
            volume=float(len(ticks))
        )

    @staticmethod
    def build_tick_candle(ticks: List[Tick], symbol: str, timestamp: float) -> Candle:
        if not ticks:
            return None
            
        prices = [t.mid for t in ticks]
        return Candle(
            symbol=symbol,
            timeframe='tick',
            timestamp=timestamp,
            open=prices[0],
            high=max(prices),
            low=min(prices),
            close=prices[-1],
            volume=float(len(ticks))
        )

class DerivFeed:
    def __init__(self, app_id: int, token: str, symbols: List[str], callback: Callable[[Tick], None]):
        self.app_id = app_id
        self.token = token
        self.symbols = symbols
        self.callback = callback
        self.api = None
        self.ws = None
        self.is_running = False

    async def connect(self):
        retries = 0
        while retries < 5:
            try:
                uri = f"wss://ws.binaryws.com/websockets/v3?app_id={self.app_id}"
                # Disable internal pings to let manual heartbeat handle it
                self.ws = await websockets.connect(uri, ping_interval=None, ping_timeout=None)
                self.api = DerivAPI(connection=self.ws)
                auth_res = await self.api.authorize(self.token)
                logger.info(f"Authorized: {auth_res.get('authorize', {}).get('fullname', 'Unknown User')}")
                return True
            except Exception as e:
                retries += 1
                wait_time = 2 ** retries
                logger.error(f"Deriv connection/auth failed ({retries}/5). Error: {e}")
                await asyncio.sleep(wait_time)
        return False

    async def _heartbeat(self):
        """Keep the connection alive with periodic pings."""
        while self.is_running and self.ws and not self.ws.closed:
            try:
                await self.api.ping({'ping': 1})
                await asyncio.sleep(20)
            except Exception as e:
                logger.warning(f"Deriv heartbeat failed: {e}")
                break

    async def subscribe_ticks(self):
        # Start heartbeat when subscribing
        asyncio.create_task(self._heartbeat())
        for symbol in self.symbols:
            try:
                logger.info(f"Attempting to subscribe to {symbol}...")
                observable = await self.api.subscribe({'ticks': symbol})
                observable.subscribe(
                    on_next=lambda data, s=symbol: self._on_tick_data(data, s),
                    on_error=lambda e, s=symbol: logger.error(f"Subscription error for {s}: {e}")
                )
            except Exception as e:
                logger.error(f"Failed to subscribe to {symbol}: {e}")

    def _on_tick_data(self, data, symbol):
        logger.info(f"Received from {symbol}: {data}")
        if 'error' in data:
            logger.error(f"Deriv API error for {symbol}: {data['error']['message']}")
            return

        if 'tick' in data:
            t = data['tick']
            tick = Tick(
                symbol=symbol,
                timestamp=float(t['epoch']),
                bid=float(t['bid']),
                ask=float(t['ask']),
                mid=(float(t['bid']) + float(t['ask'])) / 2
            )
            self.callback(tick)

    async def run(self):
        self.is_running = True
        if await self.connect():
            await self.subscribe_ticks()
            while self.is_running:
                if self.ws.closed:
                    logger.warning("Deriv WebSocket closed. Reconnecting...")
                    if not await self.connect():
                        break
                    await self.subscribe_ticks()
                await asyncio.sleep(5)

class MT5Feed:
    def __init__(self, config: Dict, symbols: List[str], callback: Callable[[Tick], None]):
        self.config = config
        self.symbols = symbols
        self.callback = callback
        self.is_running = False

    def connect(self):
        if not mt5.initialize(
            login=self.config['login'],
            password=self.config['password'],
            server=self.config['server']
        ):
            logger.error(f"MT5 initialization failed: {mt5.last_error()}")
            return False
        logger.info("Connected to MT5")
        return True

    async def run(self):
        if mt5 is None:
            logger.error("MT5Feed cannot run because the MetaTrader5 library is not installed.")
            return

        self.is_running = True
        while self.is_running:
            if not mt5.terminal_info():
                logger.warning("MT5 connection lost. Reconnecting...")
                if not self.connect():
                    await asyncio.sleep(30)
                    continue

            for symbol in self.symbols:
                ticks = mt5.copy_ticks_from(symbol, time.time() - 1, 10, mt5.COPY_TICKS_ALL)
                if ticks is not None:
                    for t in ticks:
                        tick = Tick(
                            symbol=symbol,
                            timestamp=float(t['time']),
                            bid=float(t['bid']),
                            ask=float(t['ask']),
                            mid=(float(t['bid']) + float(t['ask'])) / 2
                        )
                        self.callback(tick)
            
            await asyncio.sleep(1) # Poll every second

class DataFeed:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.maxlen = self.config['storage'].get('maxlen', 500)
        self.tick_candle_size = self.config['candles'].get('tick_count', 100)
        
        self.ticks_buffer: Dict[str, deque] = {}
        self.candles_1m_buffer: Dict[str, deque] = {}
        self.candles_5m_buffer: Dict[str, deque] = {}
        self.candles_tick_buffer: Dict[str, deque] = {}
        
        # Temp storage for building candles
        self.raw_ticks_1m: Dict[str, List[Tick]] = {}
        self.raw_ticks_5m: Dict[str, List[Tick]] = {}
        self.raw_ticks_count: Dict[str, List[Tick]] = {}
        
        self.subscriptions: Dict[str, List[Callable]] = {}
        
        self.builder = CandleBuilder()
        
        self._init_buffers()

    def _init_buffers(self):
        all_symbols = self.config['deriv']['symbols'] + self.config['mt5']['symbols']
        for s in all_symbols:
            self.ticks_buffer[s] = deque(maxlen=self.maxlen)
            self.candles_1m_buffer[s] = deque(maxlen=self.maxlen)
            self.candles_5m_buffer[s] = deque(maxlen=self.maxlen)
            self.candles_tick_buffer[s] = deque(maxlen=self.maxlen)
            self.raw_ticks_1m[s] = []
            self.raw_ticks_5m[s] = []
            self.raw_ticks_count[s] = []

    def _handle_new_tick(self, tick: Tick):
        s = tick.symbol
        self.ticks_buffer[s].append(tick)
        
        # Logic for time-based candles (1m, 5m)
        # Simplified: uses system time rounded to minute. For production, use tick timestamp.
        now = time.time()
        minute_start = (now // 60) * 60
        
        # Check if 1m candle closed
        if self.raw_ticks_1m[s] and (tick.timestamp // 60) > (self.raw_ticks_1m[s][0].timestamp // 60):
            candle = self.builder.build_time_candle(self.raw_ticks_1m[s], s, '1m', (self.raw_ticks_1m[s][0].timestamp // 60) * 60)
            self.candles_1m_buffer[s].append(candle)
            self._notify_subscribers(candle)
            self.raw_ticks_1m[s] = []
        self.raw_ticks_1m[s].append(tick)

        # Check if 5m candle closed
        if self.raw_ticks_5m[s] and (tick.timestamp // 300) > (self.raw_ticks_5m[s][0].timestamp // 300):
            candle = self.builder.build_time_candle(self.raw_ticks_5m[s], s, '5m', (self.raw_ticks_5m[s][0].timestamp // 300) * 300)
            self.candles_5m_buffer[s].append(candle)
            self._notify_subscribers(candle)
            self.raw_ticks_5m[s] = []
        self.raw_ticks_5m[s].append(tick)

        # Logic for tick-based candles
        self.raw_ticks_count[s].append(tick)
        if len(self.raw_ticks_count[s]) >= self.tick_candle_size:
            candle = self.builder.build_tick_candle(self.raw_ticks_count[s], s, tick.timestamp)
            self.candles_tick_buffer[s].append(candle)
            self._notify_subscribers(candle)
            self.raw_ticks_count[s] = []

    def _notify_subscribers(self, candle: Candle):
        if candle.symbol in self.subscriptions:
            for cb in self.subscriptions[candle.symbol]:
                cb(candle)

    def subscribe(self, symbol: str, callback: Callable[[Candle], None]):
        if symbol not in self.subscriptions:
            self.subscriptions[symbol] = []
        self.subscriptions[symbol].append(callback)

    def get_ticks(self, symbol: str, n: int) -> List[Tick]:
        buffer = self.ticks_buffer.get(symbol, [])
        return list(buffer)[-n:]

    def get_candles(self, symbol: str, timeframe: str, n: int) -> pd.DataFrame:
        if timeframe == '1m':
            buffer = self.candles_1m_buffer.get(symbol, [])
        elif timeframe == '5m':
            buffer = self.candles_5m_buffer.get(symbol, [])
        elif timeframe == 'tick':
            buffer = self.candles_tick_buffer.get(symbol, [])
        else:
            return pd.DataFrame()
            
        data = [vars(c) for c in list(buffer)[-n:]]
        return pd.DataFrame(data)

    async def start(self):
        deriv_feed = DerivFeed(
            self.config['deriv']['app_id'],
            self.config['deriv']['api_token'],
            self.config['deriv']['symbols'],
            self._handle_new_tick
        )
        mt5_feed = MT5Feed(
            self.config['mt5'],
            self.config['mt5']['symbols'],
            self._handle_new_tick
        )
        
        await asyncio.gather(
            deriv_feed.run(),
            mt5_feed.run()
        )

if __name__ == "__main__":
    async def test():
        feed = DataFeed("config.yaml")
        
        def on_candle(c):
            print(f"New Candle: {c}")

        def on_tick(t):
            print(f"Tick: {t.symbol} | Bid: {t.bid} | Ask: {t.ask}")
            
        for s in feed.config['deriv']['symbols'] + feed.config['mt5']['symbols']:
            feed.subscribe(s, on_candle)
            
        # Manually hook into new tick for testing feedback
        original_handle = feed._handle_new_tick
        def wrapped_handle(t):
            on_tick(t)
            original_handle(t)
        feed._handle_new_tick = wrapped_handle
            
        print("Starting data feeds...")
        try:
            await feed.start()
        except KeyboardInterrupt:
            print("Shutting down...")

    asyncio.run(test())
