import yfinance as yf 
import pandas as pd
from datetime import datetime
import os

# List of actively traded tickers
tickers = ["SPY", "NVDA", "TSLA", "AAPL", "MSFT"]

# Define option data columns
header = ["Timestamp", "Underlying Price", "strike",
    "Call_lastPrice", "Call_bid", "Call_ask", "Call_volume",
    "Call_impliedVolatility", "Call_openInterest",
    "Put_lastPrice", "Put_bid", "Put_ask", "Put_volume",
    "Put_impliedVolatility", "Put_openInterest"]

# Utility to get latest price
def get_latest_price(ticker_obj):
    hist = ticker_obj.history(period="1d")
    if hist.empty:
        raise Exception("No data found")
    return round(hist.iloc[-1]["Close"], 2)

# Process options for a single ticker
def get_options_data(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    try:
        current_price = get_latest_price(ticker)
        expiration = ticker.options[0]  # Nearest expiry
        options_chain = ticker.option_chain(expiration)

        # Center strikes around current price ± $20 in $5 intervals
        base = round(current_price / 5) * 5
        selected_strikes = [base + 5 * i for i in range(-4, 5)]

        def filter_and_rename(df, kind):
            df = df[df['strike'].isin(selected_strikes)]
            return df[['strike', 'lastPrice', 'bid', 'ask', 'volume', 'impliedVolatility', 'openInterest']] \
                .rename(columns=lambda x: f"{kind}_{x}" if x != 'strike' else x)

        calls = filter_and_rename(options_chain.calls, "Call")
        puts = filter_and_rename(options_chain.puts, "Put")

        # Merge and label
        combined_df = pd.merge(calls, puts, on="strike", how="inner")
        combined_df.insert(0, "Timestamp", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        combined_df.insert(1, "Underlying Price", current_price)

        # File name: TICKER_options_YYYY-MM-DD.csv
        filename = f"{ticker_symbol}_options_{expiration}.csv"
        file_exists = os.path.exists(filename)

        combined_df.to_csv(filename, mode='a', header=not file_exists, index=False)
        print(f"[{ticker_symbol}] Saved {filename} at {combined_df.iloc[0]['Timestamp']}")

    except Exception as e:
        print(f"[{ticker_symbol}] Error: {e}")

# Loop through tickers
for t in tickers:
    get_options_data(t)
