import tradermade as tm
import pandas as pd
import requests
import io
from datetime import datetime
import os

# === CONFIG ===
TRADERMADE_API_KEY = os.getenv("TRADERMADE_API_KEY")
DUNE_API_KEY = os.getenv("DUNE_API_KEY")
BASE = "USD"
CURRENCIES = [
    'AED', 'AUD', 'CAD', 'CHF', 'CNY', 'EUR', 'GBP', 'HKD', 'IDR',
    'ILS', 'JPY', 'KES', 'MXN', 'MYR', 'NZD', 'PLN', 'SAR',
    'SGD', 'THB', 'TRY', 'USD', 'VND', 'ZAR'
]
TABLE_NAME = "fx_rates"
NAMESPACE = "unhappyben"

tm.set_rest_api_key(TRADERMADE_API_KEY)

# === FETCH DATA ===
today = datetime.today().strftime("%Y-%m-%d")
# Create pairlist excluding BASE currency
pairlist = ",".join([f"{c}{BASE}" for c in CURRENCIES if c != BASE])

try:
    df = tm.historical(currency=pairlist, date=today, interval="daily", fields=["close"])
    print(f"✅ Successfully fetched data for {today}")
except Exception as e:
    print("❌ TraderMade fetch error:", e)
    exit(1)

rows = []
for i, row in df.iterrows():
    pair = row["instrument"]
    base_cur = pair[:3]
    quote_cur = pair[3:]
    
    # Get the FX rate
    if quote_cur == BASE:
        fx = row["close"]
    else:
        fx = 1 / row["close"]
    
    # Debug print
    print(f"Processing {pair}: FX rate = {fx}")
    
    # Skip zero or invalid values
    if fx == 0 or pd.isna(fx):
        print(f"⚠️ Skipping {pair} due to zero or invalid rate")
        continue
    
    # Calculate inverse with safety check
    inverse_fx = 0
    try:
        inverse_fx = round(1 / fx, 8)
    except ZeroDivisionError:
        print(f"⚠️ Cannot calculate inverse for {pair}, FX rate is zero")
        continue
    
    rows.append({
        "date": today,
        "currency": base_cur,
        "fx_rate": round(fx, 8),
        "inverse_fx_rate": inverse_fx
    })

# Add base (USD = 1)
rows.append({
    "date": today,
    "currency": BASE,
    "fx_rate": 1.0,
    "inverse_fx_rate": 1.0
})

# Create final dataframe with checks
df_final = pd.DataFrame(rows)

# Safety check - remove any NaN or zero values
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
    resp = requests.post(
        upload_url, 
        headers=headers_csv, 
        data=csv_buffer.getvalue().encode("utf-8")
    )
    
    if resp.status_code == 200:
        print("✅ Upload successful!")
    else:
        print(f"❌ Upload failed: {resp.status_code} {resp.text}")
except Exception as e:
    print(f"❌ Upload error: {e}")
