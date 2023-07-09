from glob import glob
import pandas as pd
from os.path import basename, isfile

def get_percentage_benchmark_return(file, first_entry_time, last_exit_time): # Get benchmark return for specified timeframe of a benchmark that specifies percentages instead of prices (like US treasury bill)
    df_benchmark = pd.read_csv(file, encoding = "utf-8")
    
    # Get relevant timeframe. Buy every day since open time with 1/30th the size (to average out over the whole period). End 30 days before last close time.
    df_benchmark = df_benchmark[(df_benchmark["time"] >= first_entry_time / 1000) & (df_benchmark["time"] <= last_exit_time / 1000)]
    df_benchmark = df_benchmark[:df_benchmark.shape[0] - 30]

    return df_benchmark["open"].mean() * 0.01 + 1


def get_benchmark_return(file, first_entry_time, last_exit_time): # Get benchmark return for specified timeframe
    df_benchmark = pd.read_csv(file, encoding = "utf-8")

    # Get relevant timeframe. Use last close time and first open time
    df_benchmark = df_benchmark[(df_benchmark["time"] >= first_entry_time / 1000) & (df_benchmark["time"] <= last_exit_time / 1000)]

    try:
        return df_benchmark["close"].iloc[df_benchmark.shape[0] - 1] / df_benchmark["open"].iloc[0]
    except:
        return 0


def get_single_returns(strategy): # Calculate single returns (for each symbol and period)
    files = glob(f"02_strategy/{strategy}/*.csv")
    returns = []

    for file in files: # Read all files
        print(file)
        
        df_trades = pd.read_csv(file, encoding = "utf-8")
        if df_trades.shape[0] == 0:
            continue
        
        file_name = basename(file)

        symbol = file_name[:file_name.find("_")]
        prepare_interval = file_name[file_name.find("_") + 1:file_name.rfind("_")]
        holding_interval = file_name[file_name.rfind("_") + 1:-4]
        max_drawdown = df_trades["max_drawdown"].min()
        cumulated_return = (df_trades["return"] + 1).prod() - 1

        standard_deviation = df_trades["return"].std(ddof = 0)
        if standard_deviation == 0:
            continue

        first_entry_time = df_trades["entry_time"].iloc[0]
        last_exit_time = df_trades["exit_time"].iloc[df_trades.shape[0] - 1]
        us_30d_tbill_return = get_percentage_benchmark_return("03_returns/benchmark/US_30D_TBILL_D.csv", first_entry_time, last_exit_time)
        us_30d_tbill_sharpe = (cumulated_return - us_30d_tbill_return) / standard_deviation
        sp500_return = get_benchmark_return("03_returns/benchmark/SP500_D.csv", first_entry_time, last_exit_time)
        sp500_sharpe = (cumulated_return - sp500_return) / standard_deviation
        total_crypto_return = get_benchmark_return("03_returns/benchmark/CRYPTOMARKETCAP_D.csv", first_entry_time, last_exit_time)
        total_crypto_sharpe = (cumulated_return - total_crypto_return) / standard_deviation

        returns.append([
            symbol
            , strategy
            , prepare_interval
            , holding_interval
            , first_entry_time
            , last_exit_time
            , max_drawdown
            , cumulated_return
            , standard_deviation
            , us_30d_tbill_sharpe
            , sp500_sharpe
            , total_crypto_sharpe
        ])

    df_returns = pd.DataFrame(returns, columns = [
        "symbol"
        , "strategy"
        , "prepare_interval"
        , "holding_interval"
        , "first_entry_time"
        , "last_exit_time"
        , "max_drawdown"
        , "return"
        , "standard_deviation"
        , "us_30d_tbill_sharpe"
        , "sp500_sharpe"
        , "total_crypto_sharpe"
    ])

    filename = "03_returns/returns.csv"
    if not isfile(filename):
        df_returns.to_csv(filename, index = False)
    else:
        df_returns.to_csv(filename, mode = "a", header = False, index = False)
    
    print(df_returns.sort_values("total_crypto_sharpe"))


def create_portfolio(strategy): # Create portfolios for strategy
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

    for prepare_interval in intervals:
        for holding_interval in intervals:
            print(prepare_interval, holding_interval)
            files = glob(f"02_strategy/{strategy}/*_{prepare_interval}_{holding_interval}.csv")
            df_trades = pd.DataFrame(columns = ["entry_time", "entry_price", "exit_time", "exit_price", "max_drawdown", "return", "side"])
            df_portfolio = pd.DataFrame(columns = ["exit_time", "max_drawdown", "return"])

            for file in files: # Read all files
                df = pd.read_csv(file, encoding = "utf-8")
                df = df.drop(columns = ["trade"])
                df["side"] = df["side"].astype(bool)
                df["entry_time"] = df["entry_time"].astype("int64")
                df["strategy"] = strategy
                df["prepare_interval"] = prepare_interval
                df["holding_interval"] = holding_interval
                if df_trades.shape[0] == 0:
                    df_trades = df
                else:
                    df_trades = pd.concat([df_trades, df])
            
            df_trades = df_trades.sort_values(by = ["entry_time"])
            df_portfolio = df_trades.groupby(by = ["entry_time"]).agg({"exit_time": "min", "return": "mean", "max_drawdown": "min"})

            df_portfolio.to_csv(f"03_returns/portfolio/{strategy}/{prepare_interval}_{holding_interval}.csv", index = True)


def get_portfolio_returns(strategy): # Calculate portfolio returns (if all trades at the same time were to be equally weighted)
    create_portfolio(strategy)

    files = glob(f"03_returns/portfolio/{strategy}/*.csv")
    returns = []

    for file in files: # Read all files
        print(file)
        df_trades = pd.read_csv(file, encoding = "utf-8")
        file_name = basename(file)

        prepare_interval = file_name[:file_name.rfind("_")]
        holding_interval = file_name[file_name.rfind("_") + 1:-4]
        max_drawdown = df_trades["max_drawdown"].min()
        cumulated_return = (df_trades["return"] + 1).prod() - 1

        standard_deviation = df_trades["return"].std(ddof = 0)
        if df_trades.shape[0] == 0 or standard_deviation == 0:
            continue
        first_entry_time = df_trades["entry_time"].iloc[0]
        last_exit_time = df_trades["exit_time"].iloc[df_trades.shape[0] - 1]
        us_30d_tbill_return = get_percentage_benchmark_return("03_returns/benchmark/US_30D_TBILL_D.csv", first_entry_time, last_exit_time)
        us_30d_tbill_sharpe = (cumulated_return - us_30d_tbill_return) / standard_deviation
        sp500_return = get_benchmark_return("03_returns/benchmark/SP500_D.csv", first_entry_time, last_exit_time)
        sp500_sharpe = (cumulated_return - sp500_return) / standard_deviation
        total_crypto_return = get_benchmark_return("03_returns/benchmark/CRYPTOMARKETCAP_D.csv", first_entry_time, last_exit_time)
        total_crypto_sharpe = (cumulated_return - total_crypto_return) / standard_deviation

        returns.append([
            strategy
            , prepare_interval
            , holding_interval
            , first_entry_time
            , last_exit_time
            , max_drawdown
            , cumulated_return
            , standard_deviation
            , us_30d_tbill_sharpe
            , sp500_sharpe
            , total_crypto_sharpe
        ])

    df_returns = pd.DataFrame(returns, columns = [
        "strategy"
        , "prepare_interval"
        , "holding_interval"
        , "first_entry_time"
        , "last_exit_time"
        , "max_drawdown"
        , "return"
        , "standard_deviation"
        , "us_30d_tbill_sharpe"
        , "sp500_sharpe"
        , "total_crypto_sharpe"
    ])

    df_returns.to_csv(f"03_returns/portfolio_returns.csv", index = False)
    print(df_returns.sort_values("total_crypto_sharpe"))


def main():
    strategies = [
        "ts"
        , "sd/1.0"
        , "sd/1.5"
        , "sd/2.0"
        , "sd/2.5"
        , "sd/3.0"
    ]

    for strategy in strategies:
        get_single_returns(strategy)
    
    get_portfolio_returns("ts")
    

if __name__ == "__main__":
    main()