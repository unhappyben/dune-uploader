import pandas as pd
import requests
import io
from datetime import datetime, timedelta
import os

# === CONFIG ===
EXCHANGE_RATE_API_KEY = os.getenv("EXCHANGE_RATE_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
BASE = "USD"
CURRENCIES = [
    'AED', 'ARS', 'AUD', 'CAD', 'CHF', 'CNY', 'EUR', 'GBP', 'HKD', 'IDR',
    'ILS', 'JPY', 'KES', 'MXN', 'MYR', 'NZD', 'PLN', 'SAR',
    'SGD', 'THB', 'TRY', 'USD', 'VND', 'ZAR'
]
TABLE_NAME = "fx_rates"
NAMESPACE = "unhappyben"

# === FETCH TODAY'S FX DATA ===
##today_obj = datetime(2025, 9, 4)  
today_obj = datetime.today()
today = today_obj.strftime("%Y-%m-%d")
url = f"https://v6.exchangerate-api.com/v6/{EXCHANGE_RATE_API_KEY}/history/{BASE}/{today_obj.year}/{today_obj.month:02}/{today_obj.day:02}"

try:
    response = requests.get(url)
    data = response.json()

    if data.get("result") != "success":
        raise ValueError(f"API returned error: {data.get('error-type', 'unknown')}")

    rates = data.get("conversion_rates", {})
    print(f"✅ Successfully fetched data for {today}")
except Exception as e:
    print("❌ ExchangeRate-API fetch error:", e)
    exit(1)

# === TRANSFORM TO ROWS ===
rows = []
for currency in CURRENCIES:
    fx = rates.get(currency)
    if fx is None or fx == 0:
        print(f"⚠️ Skipping {currency} due to missing or zero value")
        continue

    inverse_fx = round(1 / fx, 8)
    rows.append({
        "date": today,
        "currency": currency,
        "fx_rate": round(1 / fx, 8),          # e.g. 0.0069 (USD per JPY)
        "inverse_fx_rate": round(fx, 8)            # e.g. 145.12 (JPY per USD)
    })

# === BUILD INITIAL DATAFRAME ===
df_final = pd.DataFrame(rows)

# === BACKFILL WEEKEND RATES ===
def backfill_weekend_rates(df):
    df["date"] = pd.to_datetime(df["date"])
    currencies = df["currency"].unique()
    backfilled_rows = []

    for currency in currencies:
        currency_df = df[df["currency"] == currency].sort_values("date").reset_index(drop=True)

        for i in range(1, len(currency_df) - 1):
            prev_row = currency_df.iloc[i - 1]
            curr_row = currency_df.iloc[i]
            next_row = currency_df.iloc[i + 1]

            # Fill Saturday (Friday to Sunday gap)
            if (curr_row["date"] - prev_row["date"]).days == 2 and prev_row["date"].weekday() == 4 and curr_row["date"].weekday() == 6:
                saturday = prev_row["date"] + timedelta(days=1)
                fx_rate = round((2/3) * prev_row["fx_rate"] + (1/3) * curr_row["fx_rate"], 8)
                inverse_fx = round(1 / fx_rate, 8)
                backfilled_rows.append({
                    "date": saturday.strftime("%Y-%m-%d"),
                    "currency": currency,
                    "fx_rate": fx_rate,
                    "inverse_fx_rate": inverse_fx
                })

            # Fill Sunday (before Monday, with Friday behind)
            if (next_row["date"] - curr_row["date"]).days == 2 and curr_row["date"].weekday() == 0 and prev_row["date"].weekday() == 4:
                sunday = curr_row["date"] - timedelta(days=1)
                fx_rate = round((1/3) * prev_row["fx_rate"] + (2/3) * curr_row["fx_rate"], 8)
                inverse_fx = round(1 / fx_rate, 8)
                backfilled_rows.append({
                    "date": sunday.strftime("%Y-%m-%d"),
                    "currency": currency,
                    "fx_rate": fx_rate,
                    "inverse_fx_rate": inverse_fx
                })

    df_backfilled = pd.concat([df, pd.DataFrame(backfilled_rows)], ignore_index=True)
    df_backfilled = df_backfilled.sort_values(by=["currency", "date"]).reset_index(drop=True)
    return df_backfilled

df_final = backfill_weekend_rates(df_final)

# Final clean-up
df_final = df_final.dropna(subset=["fx_rate", "inverse_fx_rate"])
df_final = df_final[(df_final["fx_rate"] != 0) & (df_final["inverse_fx_rate"] != 0)]

print(f"✅ Processed {len(df_final)} currency pairs")

# === UPLOAD TO DUNE ===
if len(df_final) == 0:
    print("❌ No valid data to upload")
    exit(1)

csv_buffer = io.StringIO()
df_final.to_csv(csv_buffer, index=False)

upload_url = f"https://api.dune.com/api/v1/table/{NAMESPACE}/{TABLE_NAME}/insert"
headers_csv = {
    "X-DUNE-API-KEY": DUNE_API_KEY,
    "Content-Type": "text/csv"
}

try:
    resp = requests.post(upload_url, headers=headers_csv, data=csv_buffer.getvalue().encode("utf-8"))
    if resp.status_code == 200:
        print("✅ Upload successful!")
    else:
        print(f"❌ Upload failed: {resp.status_code} {resp.text}")
except Exception as e:
    print(f"❌ Upload error: {e}")
