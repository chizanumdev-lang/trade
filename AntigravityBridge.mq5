//+------------------------------------------------------------------+
//|                                              AntigravityBridge.mq5|
//|                                  Copyright 2026, Antigravity AI   |
//|                                             https://antigravity.ai |
//+------------------------------------------------------------------+
#property copyright "Copyright 2026, Antigravity AI"
#property link      "https://antigravity.ai"
#property version   "1.00"
#property strict

// Input parameters
input string   InpFileName = "mt5_commands.csv"; // Command file name (must be in MQL5/Files)
input int      InpTimerMS  = 500;                // Check every X milliseconds

// Global variables
int            file_handle = INVALID_HANDLE;
datetime       last_check  = 0;

struct TradeCommand {
    string   symbol;
    string   direction;
    double   volume;
    double   sl;
    double   tp;
    string   id;
};

//+------------------------------------------------------------------+
//| Expert initialization function                                   |
//+------------------------------------------------------------------+
int OnInit()
{
    Print("Antigravity Bridge Started. Watching: ", InpFileName);
    EventSetMillisecondTimer(InpTimerMS);
    return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization function                                 |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
    EventKillTimer();
}

//+------------------------------------------------------------------+
//| Timer function                                                   |
//+------------------------------------------------------------------+
void OnTimer()
{
    ReadCommands();
}

//+------------------------------------------------------------------+
//| Read commands from file and execute                              |
//+------------------------------------------------------------------+
void ReadCommands()
{
    // Check if file exists in the common or local folder
    if(!FileIsExist(InpFileName)) return;

    file_handle = FileOpen(InpFileName, FILE_READ|FILE_CSV|FILE_ANSI, ',');
    if(file_handle == INVALID_HANDLE) return;

    // We expect: SYMBOL,DIRECTION,VOLUME,SL,TP,ID
    while(!FileIsEnding(file_handle))
    {
        string sym = FileReadString(file_handle);
        string dir = FileReadString(file_handle);
        double vol = FileReadNumber(file_handle);
        double sl  = FileReadNumber(file_handle);
        double tp  = FileReadNumber(file_handle);
        string id  = FileReadString(file_handle);

        if(sym != "" && dir != "")
        {
            ExecuteTrade(sym, dir, vol, sl, tp, id);
        }
    }

    FileClose(file_handle);
    
    // Delete file after reading to avoid duplicates (safest for simple bridge)
    FileDelete(InpFileName);
}

//+------------------------------------------------------------------+
//| Execute the trade in MT5                                          |
//+------------------------------------------------------------------+
void ExecuteTrade(string sym, string dir, double vol, double sl, double tp, string id)
{
    MqlTradeRequest request={};
    MqlTradeResult  result={};
    
    request.action    = TRADE_ACTION_DEAL;
    request.symbol    = sym;
    request.volume    = vol;
    request.sl        = sl;
    request.tp        = tp;
    request.magic     = 123456;
    request.comment   = "Bot ID: " + id;
    request.type_filling = ORDER_FILLING_FOK;
    request.deviation = 10;

    if(dir == "BUY")
    {
        request.type = ORDER_TYPE_BUY;
        request.price = SymbolInfoDouble(sym, SYMBOL_ASK);
    }
    else if(dir == "SELL")
    {
        request.type = ORDER_TYPE_SELL;
        request.price = SymbolInfoDouble(sym, SYMBOL_BID);
    }
    else return;

    if(!OrderSend(request, result))
    {
        Print("OrderSend error: ", GetLastError());
    }
    else
    {
        Print("Trade executed successfully via Bridge: ", id);
    }
}
//+------------------------------------------------------------------+
