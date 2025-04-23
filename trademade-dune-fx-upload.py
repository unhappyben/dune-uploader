# daily_fx_update.py
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
pairlist = ",".join([f"{c}{BASE}" for c in CURRENCIES if c != BASE])

try:
    df = tm.historical(currency=pairlist, date=today, interval="daily", fields=["close"])
except Exception as e:
    print("❌ TraderMade fetch error:", e)
    exit(1)

rows = []

for i, row in df.iterrows():
    pair = row["instrument"]
    base_cur = pair[:3]
    quote_cur = pair[3:]

    if quote_cur == BASE:
        fx = row["close"]
    else:
        fx = 1 / row["close"]

    rows.append({
        "date": today,
        "currency": base_cur,
        "fx_rate": round(fx, 8),
        "inverse_fx_rate": round(1 / fx, 8)
    })

# Add base (USD = 1)
rows.append({
    "date": today,
    "currency": BASE,
    "fx_rate": 1.0,
    "inverse_fx_rate": 1.0
})

df_final = pd.DataFrame(rows).dropna(subset=["fx_rate"])
df_final = df_final[df_final["fx_rate"] != 0]

# === UPLOAD TO DUNE ===
csv_buffer = io.StringIO()
df_final.to_csv(csv_buffer, index=False)

upload_url = f"https://api.dune.com/api/v1/table/{NAMESPACE}/{TABLE_NAME}/insert"
headers_csv = {
    "X-DUNE-API-KEY": DUNE_API_KEY,
    "Content-Type": "text/csv"
}

resp = requests.post(upload_url, headers=headers_csv, data=csv_buffer.getvalue().encode("utf-8"))

if resp.status_code == 200:
    print("✅ Upload successful!")
else:
    print(f"❌ Upload failed: {resp.status_code} {resp.text}")
