import asyncio
import sys
sys.path.append('/Users/chizanumidemili/Projects/bot')

from data_feed import DataFeed

async def test():
    feed = DataFeed('config.yaml')
    print("Testing Deriv connection...")
    print(f"  App ID  : {feed.config['deriv']['app_id']}")
    print(f"  Symbol  : 1HZ10V")
    print()

    ticks = []

    def on_candle(candle):
        ticks.append(candle)
        print(f"  Candle/Tick #{len(ticks):02d}: {candle.symbol}  "
              f"O={candle.open:.5f}  H={candle.high:.5f}  "
              f"L={candle.low:.5f}  C={candle.close:.5f}")
        if len(ticks) >= 5:
            print()
            print("✓ Connection OK — 5 data points received")
            import os; os._exit(0)  # cleanly stop the async loop

    feed.subscribe('1HZ10V', on_candle)

    print("Connecting to Deriv WebSocket...")
    await feed.start()

if __name__ == '__main__':
    asyncio.run(test())
