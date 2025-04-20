import yfinance as yf 
import pandas as pd
from datetime import datetime
import os 

# Create ticker object once
spy = yf.Ticker("SPY")

# Get latest SPY price
def get_latest_spy_price():
    hist = spy.history(period="1d")
    if hist.empty:
        raise Exception("No SPY data found")
    return round(hist.iloc[-1]["Close"], 2)

# Get first available expiration date
expiration_date = spy.options[0]

# Define headers
header = ["Timestamp", "SPY price", "strike",
    "Call_lastPrice", "Call_bid", "Call_ask", "Call_volume",
    "Call_impliedVolatility", "Call_openInterest",
    "Put_lastPrice", "Put_bid", "Put_ask", "Put_volume",
    "Put_impliedVolatility", "Put_openInterest"]

# Get today's date for file name
today_str = datetime.now().strftime('%Y-%m-%d')
csv_file = f"SPY_option_{expiration_date}-01.csv"

def get_spy_options(expiration_date):
    spy_price = get_latest_spy_price()

    # Fixed 9 strike prices centered around 520 with $5 intervals
    selected_strikes = [500 + 5 * i for i in range(12)]

    # Get options chain
    options_chain = spy.option_chain(expiration_date)
    calls = options_chain.calls
    puts = options_chain.puts

    # Filter and rename
    def filter_and_rename(df, kind):
        df = df[df['strike'].isin(selected_strikes)]
        return df[['strike', 'lastPrice', 'bid', 'ask', 'volume', 'impliedVolatility', 'openInterest']] \
            .rename(columns=lambda x: f"{kind}_{x}" if x != 'strike' else x)

    calls_filtered = filter_and_rename(calls, "Call")
    puts_filtered = filter_and_rename(puts, "Put")

    # Merge
    combined_df = pd.merge(calls_filtered, puts_filtered, on="strike", how="inner")
    combined_df.insert(0, "Timestamp", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    combined_df.insert(1, "SPY price", spy_price)

    # Save
    file_exists = os.path.exists(csv_file)
    combined_df.to_csv(csv_file, mode='a', header=not file_exists, index=False)
    print(f"Saved: {csv_file} at {combined_df.iloc[0]['Timestamp']}")

# Run the function
try:
    get_spy_options(expiration_date)
except Exception as e:
    print(f"Error: {e}")
