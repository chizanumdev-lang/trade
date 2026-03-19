import asyncio
import yaml
import os
from execution import TradeManager, TradeOrder, TradeEvent
from logger import Logger

CONFIG_PATH = 'config.yaml'

async def test_execution():
    print("--- Testing Deriv Trade Execution ---")
    
    # 1. Load config
    if not os.path.exists(CONFIG_PATH):
        print(f"Error: {CONFIG_PATH} not found")
        return

    with open(CONFIG_PATH, 'r') as f:
        config = yaml.safe_load(f)

    # 2. Setup Logger
    logger = Logger(config)
    
    def on_trade_event(event: TradeEvent):
        logger.log_trade_event(event)
        print(f"Trade Event: {event.event_type} - {event.symbol} {event.direction}")

    # 3. Initialize TradeManager in LIVE mode (paper_mode=False)
    trade_manager = TradeManager(CONFIG_PATH, on_trade_event, paper_mode=False)
    
    # 4. Create a test order (Smallest possible lot)
    # Volatility 10 (1HZ10V) min lot is 0.01 or similar
    # We use a dummy entry price, SL, TP
    symbol = '1HZ10V'
    order = TradeOrder(
        symbol=symbol,
        direction='BUY',
        lot_size=0.1,  # Smallest stake usually
        entry_price=9730.0,
        sl_price=9700.0,
        tp1_price=9750.0,
        tp2_price=9770.0,
        be_trigger_price=9740.0,
        trail_distance=5.0,
        risk_fraction=0.01,
        kelly_fraction=0.01,
        risk_amount=1.0, # Just for display/stake
        source='TEST',
        atr_value=5.0,
        rejected=False
    )

    print(f"Attempting to place TEST BUY order for {symbol}...")
    
    # 5. Execute
    event = await trade_manager.execute(order)
    
    if event and event.event_type == 'OPENED':
        print("\nSUCCESS: Trade placed successfully on Deriv!")
        print(f"Details: {event}")
        
        # Wait a bit then close it immediately for the test
        print("Waiting 5 seconds before closing the test trade...")
        await asyncio.sleep(5)
        
        print(f"Closing test trade for {symbol}...")
        pnl = await trade_manager.close_position(symbol, 0.1, 9735.0) # Dummy close
        print(f"Trade closed. Estimated P&L: ${pnl:.2f}")
    else:
        print("\nFAILED: Could not place trade. Check terminal for errors.")

if __name__ == "__main__":
    asyncio.run(test_execution())
