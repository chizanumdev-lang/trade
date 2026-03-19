import asyncio
import os
import yaml
import json
from datetime import datetime
from execution import TradeManager, TradeEvent
from logger import Logger
from risk_manager import TradeOrder

async def test_fix():
    # Load config
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize Logger
    logger = Logger(config)
    
    # Callback
    def on_trade_event(event):
        logger.log_trade_event(event)
    
    # Initialize TradeManager in paper mode
    manager = TradeManager('config.yaml', on_trade_event, paper_mode=True)
    
    # 1. Simulate a winning trade
    print("\n--- Simulating a WINNING trade ---")
    event_win = TradeEvent(
        event_type='CLOSED', symbol='1HZ10V', direction='BUY', lot_size=1.0,
        price=110.0, sl_price=90.0, tp_price=120.0, profit_loss=10.0,
        reason='TP Hit', broker='PAPER'
    )
    logger.log_trade_event(event_win)

    # 2. Simulate a losing trade
    print("\n--- Simulating a LOSING trade ---")
    event_loss = TradeEvent(
        event_type='CLOSED', symbol='1HZ10V', direction='BUY', lot_size=1.0,
        price=90.0, sl_price=90.0, tp_price=120.0, profit_loss=-10.0,
        reason='SL Hit', broker='PAPER'
    )
    logger.log_trade_event(event_loss)
    
    # Check daily.json
    print("\n--- Checking logs/daily.json ---")
    with open('logs/daily.json', 'r') as f:
        daily = json.load(f)
        today = list(daily.keys())[-1]
        stats = daily[today]
        print(f"Stats for {today}: {stats}")
        
        if stats['wins'] == 1 and stats['losses'] == 1:
            print("\nSUCCESS: Logger correctly distinguished between win and loss!")
        else:
            print("\nFAILURE: Logger did not distinguish correctly.")

if __name__ == "__main__":
    asyncio.run(test_fix())
