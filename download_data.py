from requests import request
from time import time
import pandas as pd
import os

def get_symbols(): # Get all active Bybit USDT perpetual trading pairs
    response = request("GET", "https://api.bybit.com/v5/market/tickers?category=linear").json()

    symbols = []
    for symbol in response["result"]["list"]:
        if symbol["symbol"][-4:] == "USDT": symbols.append(symbol["symbol"]) # Only append all perpetual contracts that use USDT as collateral (instead of USDC as collateral or regular derivatives)
    
    return symbols

def get_initial_data(symbols, interval): # Get historical data of all symbols for the smallest interval
    unix_first = 1262304000 * 1000 # 2010-01-01 00:00 UTC
    unix_last = int(86400 * int(time() / 86400)) * 1000 # Get current date at 00:00:00 midnight in unix.
    interval_size = (interval * 60 * 1000) # Time between two candles
    rows_total = 0

    for symbol in symbols:
        if not os.path.exists(f"{os.getcwd()}/01_raw/{symbol}_{interval}.csv"):
            print(f"GET: {symbol} @{interval}")
            unix_start = unix_first # Set first unix far in the past
            df = pd.DataFrame(columns = ["start_time", "open", "high", "low", "close", "volume", "turnover"])
            while unix_start < unix_last - interval_size:
                if interval == 1440: interval = "D" # API only accepts "D" and not 1440
                response = request("GET", f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={interval}&start={unix_start}").json()
                
                returnCode = response["retCode"]
                if returnCode != 0: # Quit if returnCode is not 0 (OK)
                    print(f"[!] {returnCode}: {response['retMsg']}")
                    quit()
                
                kline = response["result"]["list"]
                df = pd.concat([df, pd.DataFrame(columns=["start_time", "open", "high", "low", "close", "volume", "turnover"], data = kline)]) # Append all new data to old data
                unix_start = int(kline[0][0]) + interval_size
                
            df = df.set_index("start_time").sort_index() # Sort data
            df = df[:-1] # Delete last row (current candle)
            df.to_csv(f"01_raw/{symbol}_{interval}.csv", encoding = "utf-8") # Save data as csv
            rows = df.shape[0]
            rows_total = rows_total + rows
            print(f"   + {rows} rows saved.\n   > {rows_total} total rows.")

def resample_data(df, symbol, interval): # Resample data of df with interval
    df["index"] = df["start_time"].astype("datetime64[ms]")
    df = df.set_index("index")
    df_resampled = df.resample(f"{interval}min").agg(
        {
            "start_time": "first"
            , "open": "first"
            , "high": "max"
            , "low": "min"
            , "close": "last"
            , "volume": "sum"
            , "turnover": "sum"
        }
    )
    df_resampled["start_time"] = df_resampled["start_time"].astype("int64")
    df_resampled.to_csv(f"01_raw/{symbol}_{interval}.csv", encoding = "utf-8", index = False) # Save data as csv
    return df_resampled
    
def get_remaining_data(symbols, smallest_interval): # Get historical data of all symbols for the all the other intervals
    intervals = [
        15
        , 30
        , 60
        , 120
        , 240
        , 360
        , 720
        , 1440
    ]

    for symbol in symbols:
        try:
            df = pd.read_csv(f"01_raw/{symbol}_{smallest_interval}.csv", encoding = "utf-8") # Read data of smallest interval
            for interval in intervals:
                print(f"GET: {symbol} @{interval}")
                df = resample_data(df, symbol, interval)
        except:
            print(f"{symbol} could not be fetched. Probably sorted out beforehand.")

def main():
    try:
        smallest_interval = 5
        symbols = get_symbols()
        get_initial_data(symbols, smallest_interval)
        get_remaining_data(symbols, smallest_interval)
    except:
        os.system("download_data.py")

if __name__ == "__main__":
    main()