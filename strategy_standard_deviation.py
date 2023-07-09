from glob import glob
import pandas as pd
import numpy as np
from os.path import basename

def get_trading_signal(df_kline, sigma): # Get trading signal
    # Get standard deviation
    df_kline["change"] = df_kline["close"] / df_kline["open"] - 1
    df_kline["standard_deviation"] = sigma * df_kline["change"].std(ddof = 0)
    df_kline = df_kline.iloc[11:] # Drop first 11 rows as their standard deviation includes less than 10 samples

    # Get all candles that are "abnormal" (exceeding standard deviation)
    conditions = [
        df_kline["change"] >= df_kline["standard_deviation"]
        , (df_kline["change"] < df_kline["standard_deviation"]) & (df_kline["change"] > -df_kline["standard_deviation"])
        , df_kline["change"] <= -df_kline["standard_deviation"]
    ]
    choices = [
        True
        , None
        , False
    ]
    df_kline["side"] = np.select(conditions, choices, default = None)
    return df_kline

def filter_trades(df_group): # Filter trades of the same trading time. Remove positions if they cancel out (Long/Short)
    longs = sum(df_group["side"])
    shorts = len(df_group) - longs
    if longs > shorts:
        return df_group[df_group["side"] == True].iloc[:1]
    elif longs < shorts:
        return df_group[df_group["side"] == False].iloc[:1]
    else:
        return None

def get_trades(df_kline, df_kline_holding, prepare_interval): # Get all trades
    # Merge formation period with holding period
    df_kline = df_kline[["start_time", "side"]]

    df_kline["earliest_holding_time"] = df_kline["start_time"] + (prepare_interval * 60 * 1000)
    df_kline_holding["open_time_holding"] = df_kline_holding["start_time"]
    df_kline_holding["close_time_holding"] = df_kline_holding["start_time"].shift(-1)
    df_kline_holding = df_kline_holding.dropna()

    df_kline = pd.merge_asof(df_kline, df_kline_holding, left_on = "earliest_holding_time", right_on = "start_time", direction = "forward")
    df_kline = df_kline.dropna()
    
    # Check for multiple trades at the same time and only keep one position. Do not trade at all if they cancel out.
    df_kline = df_kline.groupby("open_time_holding", group_keys = True).apply(filter_trades).reset_index(drop = True)
    
    try:
        # Calculate max drawdown
        conditions = [
            df_kline["side"] == True
            , df_kline["side"] == False
        ]
        choices = [
            df_kline["low"] / df_kline["open"] - 1
            , 1 - df_kline["high"] / df_kline["open"]
        ]

        df_kline["max_drawdown"] = np.select(conditions, choices, default = None)

        # Calculate return
        trading_fee = 0.0006
        funding_fee = 0.0001 # Base funding fee every 8 hours
        
        df_kline["funding_fee"] = 1 - (funding_fee * ((df_kline["close_time_holding"] - df_kline["open_time_holding"]) / (1000 * 60 * 60 * 8)))

        choices = [
            ((df_kline["close"] * (1 - trading_fee)) / (df_kline["open"] * ((1 + trading_fee))) - 1) * df_kline["funding_fee"]
            , 1 - (df_kline["close"] * (1 + trading_fee)) / (df_kline["open"] * (1 - trading_fee)) * df_kline["funding_fee"]
        ]
        df_kline["return"] = np.select(conditions, choices, default = None)

        df_kline["max_drawdown"][df_kline["max_drawdown"] < -1] = -1 # Max drawdown can only be 100% loss.
        df_kline["return"][df_kline["max_drawdown"] == -1] = -1 # In case of max drawdown of more than 100%, position would get liquidated, even if it eventually retraces.
        df_kline["return"][df_kline["return"] < -1] = -1 # Position would get liquidated before having negative balance.
    except: # No trade
        return pd.DataFrame()

    df_kline = df_kline.copy().dropna()
    
    # Calculate trade-relevant metrics and show each trade as one row
    df_kline["entry_price"] = df_kline["open"]
    df_kline["exit_price"] = df_kline["close"]
    df_trades = df_kline[["open_time_holding", "entry_price", "close_time_holding", "exit_price", "max_drawdown", "return", "side"]]
    df_trades = df_trades.rename(columns = {"open_time_holding": "entry_time", "close_time_holding": "exit_time"})
    df_trades = df_trades.dropna()
    return df_trades

def calculate_trades(df_kline, df_kline_holding, sigma, prepare_interval):
    df_kline = get_trading_signal(df_kline, sigma) # Get trading signal
    df_trades = get_trades(df_kline, df_kline_holding, prepare_interval) # Get trades
    return df_trades

def main():
    files = glob("01_raw/*.csv")
    sigmas = [
        1.0
        , 1.5
        , 2.0
        , 2.5
        , 3.0
    ]
    intervals = [
        5
        , 15
        , 30
        , 60
        , 120
        , 240
        , 360
        , 720
        , 1440
    ]
    
    for file in files: # Read all files
        df_kline = pd.read_csv(file, encoding = "utf-8") # Turn all files into Pandas Dataframes
        df_kline = df_kline.drop(columns = ["volume", "turnover"])
        print(file)
        for sigma in sigmas:
            for holding_interval in intervals:
                df_kline_holding = pd.read_csv(f"01_raw/{basename(file)[:basename(file).find('_')]}_{holding_interval}.csv", encoding = "utf-8")
                df_kline_holding = df_kline_holding.drop(columns = ["volume", "turnover"])
                prepare_interval = int(file[file.rfind("_") + 1:-4])
                df_trades = calculate_trades(df_kline, df_kline_holding, sigma, prepare_interval)
                df_trades.to_csv(f"02_strategy/sd/{sigma}/{basename(file)[:-4]}_{holding_interval}.csv", index_label = "trade")
    
    print(f"[SD] Successfully calculated all trades.")

if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    main()