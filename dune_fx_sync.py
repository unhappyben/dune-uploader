## yahoo finance python script -- moving to api based from trader made

import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import requests
import io
import os


# Configuration
CONFIG = {
    "base_currency": "USD",
    "currencies": [
        'AED', 'AUD', 'CAD', 'CHF', 'CNY', 'EUR', 'GBP', 'HKD', 'IDR',
        'ILS', 'JPY', 'KES', 'MXN', 'MYR', 'NZD', 'PLN', 'SAR',
        'SGD', 'THB', 'TRY', 'USD', 'VND', 'ZAR'
    ],
    "table_name": "fx_rates",
    "namespace": "unhappyben",
    "api_key": os.environ["DUNE_API_KEY"]
}

# Set date range
yesterday = (datetime.now().strftime("%Y-%m-%d")) #- timedelta(days=1)).strftime("%Y-%m-%d")
CONFIG["start_date"] = "2025-01-01"
CONFIG["end_date"] = yesterday

def fetch_yahoo_finance_rates(currency, base_currency, start_date, end_date):
    if currency == base_currency:
        dates = pd.date_range(start=start_date, end=end_date)
        df = pd.DataFrame(index=dates)
        df['Date'] = df.index.date
        df['Close'] = 1.0
        return df

    if base_currency == "USD":
        ticker = f"USD{currency}=X"
        inverse_needed = True
    else:
        ticker = f"{currency}{base_currency}=X"
        inverse_needed = False

    try:
        data = yf.download(ticker, start=start_date, end=end_date, progress=False)

        if data.empty:
            print(f"No data for {ticker}")
            if inverse_needed:
                ticker = f"{currency}{base_currency}=X"
                print(f"Trying fallback: {ticker}")
                data = yf.download(ticker, start=start_date, end=end_date, progress=False)
                inverse_needed = False

                if data.empty:
                    return pd.DataFrame()
            else:
                return pd.DataFrame()

        df = data[['Close']].copy()
        df.reset_index(inplace=True)
        df['Date'] = df['Date'].dt.date

        if inverse_needed:
            df['Close'] = 1 / df['Close']

        return df

    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return pd.DataFrame()

def main():
    all_rates = []

    for currency in CONFIG["currencies"]:
        print(f"Fetching {currency}...")
        df = fetch_yahoo_finance_rates(
            currency,
            CONFIG["base_currency"],
            CONFIG["start_date"],
            CONFIG["end_date"]
        )

        if df.empty:
            continue

        df['Currency'] = currency
        df['Inverse_Rate'] = 1 / df['Close']
        df = df[['Date', 'Currency', 'Close', 'Inverse_Rate']]
        df.columns = ['date', 'currency', 'fx_rate', 'inverse_fx_rate']
        all_rates.append(df)
        time.sleep(1)

    if not all_rates:
        print("No data to upload.")
        return

    combined_df = pd.concat(all_rates).sort_values(['date', 'currency'])

    print("✅ Data ready:")
    print(combined_df.head())

    headers_json = {
        "X-DUNE-API-KEY": CONFIG["api_key"],
        "Content-Type": "application/json"
    }
    headers_csv = {
        "X-DUNE-API-KEY": CONFIG["api_key"],
        "Content-Type": "text/csv"
    }

    # Step 1: Create table (if not exists)
    create_url = "https://api.dune.com/api/v1/table/create"
    create_payload = {
        "namespace": CONFIG["namespace"],
        "table_name": CONFIG["table_name"],
        "description": "Daily FX rates against USD from Yahoo Finance",
        "schema": [
            {"name": "date", "type": "date", "nullable": False},
            {"name": "currency", "type": "varchar", "nullable": False},
            {"name": "fx_rate", "type": "double", "nullable": False},
            {"name": "inverse_fx_rate", "type": "double", "nullable": False}
        ],
        "is_private": False
    }

    create_resp = requests.post(create_url, json=create_payload, headers=headers_json)
    if create_resp.status_code == 200:
        print("✅ Table created on Dune.")
    elif "already exists" in create_resp.text:
        print("⚠️ Table already exists, continuing.")
    else:
        print(f"❌ Create failed: {create_resp.status_code} {create_resp.text}")

    # Step 2: Clear old data
    clear_url = f"https://api.dune.com/api/v1/table/{CONFIG['namespace']}/{CONFIG['table_name']}/clear"
    clear_resp = requests.post(clear_url, headers={"X-DUNE-API-KEY": CONFIG["api_key"]})

    if clear_resp.status_code == 200:
        print("🧹 Cleared existing data.")
    else:
        print(f"⚠️ Failed to clear: {clear_resp.status_code} {clear_resp.text}")

    # Step 3: Upload fresh data
    csv_buffer = io.StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    upload_url = f"https://api.dune.com/api/v1/table/{CONFIG['namespace']}/{CONFIG['table_name']}/insert"
    upload_resp = requests.post(
        upload_url,
        headers=headers_csv,
        data=csv_buffer.getvalue().encode("utf-8")
    )

    if upload_resp.status_code == 200:
        print("🚀 Upload successful!")
        print(upload_resp.json())
    else:
        print(f"❌ Upload failed: {upload_resp.status_code}")
        print(upload_resp.text)

if __name__ == "__main__":
    main()
