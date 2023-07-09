from glob import glob
import pandas as pd
import numpy as np
from os.path import basename, exists

def get_top_performers(df_kline): # Get top performers (best and worst 10%)
    # Get price change
    df_kline["change"] = df_kline["close"] / df_kline["open"] - 1

    # Build portfolio
    df_kline = df_kline.sort_values(by = ["start_time"])
    portfolio_size = 10
    df_kline = df_kline[df_kline.groupby("start_time")["start_time"].transform("size") >= portfolio_size] # Set minimum portfolio size
    
    # Find percentile values
    cutoffs = df_kline.groupby("start_time", group_keys = True)["change"].apply(lambda x: pd.Series({"worst": x.quantile(0.1), "best": x.quantile(0.9)})).reset_index() # Get cutoff values
    cutoffs = cutoffs.pivot(index = "start_time", columns = "level_1", values = "change").reset_index() # Pivot Series to have "best" and "worst" columns with their respective values

    # Get the worst 10% performers
    df_worst_performers = pd.merge(df_kline, cutoffs, left_on = "start_time", right_on = "start_time")
    df_worst_performers = df_worst_performers[df_worst_performers["change"] <= df_worst_performers["worst"]]
    df_worst_performers["side"] = False

    # Get the best 10% performers
    df_best_performers = pd.merge(df_kline, cutoffs, left_on = "start_time", right_on = "start_time")
    df_best_performers = df_best_performers[df_best_performers["change"] >= df_best_performers["best"]]
    df_best_performers["side"] = True
    
    # Concat all to one dataframe
    df_top_performers = pd.concat([df_worst_performers, df_best_performers])
    df_top_performers = df_top_performers.sort_values(by = ["start_time", "side"]).drop(columns = ["change", "best", "worst"])
    return df_top_performers

def isolate_cryptocurrencies(df_top_performers, symbol, file): # Isolate each cryptocurrency
    # Merge the signal data with the raw data
    df_top_performers = df_top_performers[["start_time", "side"]]

    df_kline = pd.read_csv(f"01_raw/{symbol}_{basename(file)}", encoding = "utf-8")
    df_kline = df_kline.drop(columns = ["volume", "turnover"])
    df_kline = pd.merge(df_kline, df_top_performers, how = "left", left_on = "start_time", right_on = "start_time")

    return df_kline

def get_trades(df_kline, df_kline_holding, prepare_interval): # Get all trades
    # Merge formation period with holding period
    df_kline = df_kline[["start_time", "side"]]

    df_kline["earliest_holding_time"] = df_kline["start_time"] + (prepare_interval * 60 * 1000)
    df_kline_holding["open_time_holding"] = df_kline_holding["start_time"]
    df_kline_holding["close_time_holding"] = df_kline_holding["start_time"].shift(-1)
    df_kline_holding = df_kline_holding.dropna()

    df_kline = pd.merge_asof(df_kline, df_kline_holding, left_on = "earliest_holding_time", right_on = "start_time", direction = "forward")
    df_kline = df_kline.dropna()

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

        choices = [
            (df_kline["close"] * (1 - trading_fee)) / (df_kline["open"] * (1 + trading_fee)) - 1
            , 1 - (df_kline["close"] * (1 + trading_fee)) / (df_kline["open"] * (1 - trading_fee))
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
    df_trades = df_trades.dropna().reset_index(drop = True)
    return df_trades

def calculate_trades(df_kline, file, intervals, prepare_interval):
    df_top_performers = get_top_performers(df_kline) # Get trading signal
    
    for symbol, group in df_top_performers.groupby("symbol"):
        print(symbol, file)
        for holding_interval in intervals:
            df_kline_holding = pd.read_csv(f"01_raw/{symbol}_{holding_interval}.csv", encoding = "utf-8")
            df_kline_holding = df_kline_holding.drop(columns = ["volume", "turnover"])
            df_kline = isolate_cryptocurrencies(group, symbol, file) # Isolate each cryptocurrency
            df_trades = get_trades(df_kline, df_kline_holding, prepare_interval) # Get trades
            df_trades.to_csv(f"02_strategy/ts/{symbol}_{basename(file)[:-4]}_{holding_interval}.csv", index_label = "trade")

def main():
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
    
    for interval in intervals:
        files = glob(f"01_raw/*_{interval}.csv")
        for file in files: # Read all files
            df_kline = pd.read_csv(file, encoding = "utf-8") # Turn all files into Pandas Dataframes
            df_kline = df_kline.drop(columns = ["volume", "turnover"])
            df_kline["symbol"] = basename(file)[:basename(file).find(f"_{interval}.csv")]
            path = f"02_strategy/ts/interval/{interval}.csv"
            df_kline.to_csv(path, mode = "a", header = not exists(path), index = False) # Create a portfolio of cryptocurrencies for one interval
    
    files = glob(f"02_strategy/ts/interval/*.csv")
    for file in files:
        df_kline = pd.read_csv(file, encoding = "utf-8") # Turn all files into Pandas Dataframes
        prepare_interval = int(basename(file)[:-4])
        calculate_trades(df_kline, file, intervals, prepare_interval)
        
    print(f"[TS] Successfully calculated all trades.")

if __name__ == "__main__":
    pd.options.mode.chained_assignment = None
    main()