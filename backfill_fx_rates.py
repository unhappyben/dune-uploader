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
    'AED', 'AUD', 'CAD', 'CHF', 'CNY', 'EUR', 'GBP', 'HKD', 'IDR',
    'ILS', 'JPY', 'KES', 'MXN', 'MYR', 'NZD', 'PLN', 'SAR',
    'SGD', 'THB', 'TRY', 'USD', 'VND', 'ZAR'
]
TABLE_NAME = os.getenv("DUNE_TABLE_NAME", "fx_rates")
NAMESPACE = os.getenv("DUNE_NAMESPACE", "unhappyben")

# === Date Setup (Must Run on Monday) ===
today = datetime.today()
if today.weekday() != 0:
    print("❌ This script must run on a Monday.")
    exit(1)

friday = today - timedelta(days=3)
monday = today
date_strs = {
    "friday": friday.strftime("%Y-%m-%d"),
    "monday": monday.strftime("%Y-%m-%d"),
    "saturday": (friday + timedelta(days=1)).strftime("%Y-%m-%d"),
    "sunday": (friday + timedelta(days=2)).strftime("%Y-%m-%d"),
}

# === Fetch Historical FX Data from ExchangeRate-API ===
def fetch_fx(date_obj, label):
    url = f"https://v6.exchangerate-api.com/v6/{EXCHANGE_RATE_API_KEY}/history/{BASE}/{date_obj.year}/{date_obj.month:02}/{date_obj.day:02}"
    try:
        resp = requests.get(url)
        data = resp.json()
        if data.get("result") != "success":
            raise ValueError(data.get("error-type", "Unknown error"))
        rates = data["conversion_rates"]
        print(f"✅ Fetched {label} FX rates")
        return {cur: rates[cur] for cur in CURRENCIES if cur in rates}
    except Exception as e:
        print(f"❌ Error fetching {label} rates:", e)
        return None

fri_rates = fetch_fx(friday, "Friday")
mon_rates = fetch_fx(monday, "Monday")

if fri_rates is None or mon_rates is None:
    print("❌ Cannot proceed without both Friday and Monday data.")
    exit(1)

# === Generate Weighted Interpolated FX Data ===
def build_backfill_rows(day_name, date_str, weights):
    rows = []
    for cur in CURRENCIES:
        if cur not in fri_rates or cur not in mon_rates:
            print(f"⚠️ Skipping {cur}, missing Friday or Monday rate.")
            continue
        inverse_fx = round(weights[0] * fri_rates[cur] + weights[1] * mon_rates[cur], 8)  # e.g. 145.12 JPY/USD
        fx = round(1 / inverse_fx, 8) if inverse_fx != 0 else 0                           # e.g. 0.00691 USD/JPY

        rows.append({
            "date": date_str,
            "currency": cur,
            "fx_rate": fx,                      # USD per foreign currency
            "inverse_fx_rate": inverse_fx       # foreign per USD
        })

    print(f"✅ Built {day_name} rows")
    return rows

saturday_rows = build_backfill_rows("Saturday", date_strs["saturday"], weights=(2/3, 1/3))
sunday_rows = build_backfill_rows("Sunday", date_strs["sunday"], weights=(1/3, 2/3))

# === Combine and Upload ===
df_backfill = pd.DataFrame(saturday_rows + sunday_rows)
df_backfill = df_backfill.dropna(subset=["fx_rate", "inverse_fx_rate"])
df_backfill = df_backfill[(df_backfill["fx_rate"] != 0) & (df_backfill["inverse_fx_rate"] != 0)]

print(f"✅ Final backfill: {len(df_backfill)} rows.")

csv_buffer = io.StringIO()
df_backfill.to_csv(csv_buffer, index=False)

upload_url = f"https://api.dune.com/api/v1/table/{NAMESPACE}/{TABLE_NAME}/insert"
headers = {
    "X-DUNE-API-KEY": DUNE_API_KEY,
    "Content-Type": "text/csv"
}

try:
    resp = requests.post(upload_url, headers=headers, data=csv_buffer.getvalue().encode("utf-8"))
    if resp.status_code == 200:
        print("🚀 Weekend backfill uploaded successfully.")
    else:
        print(f"❌ Upload failed: {resp.status_code} {resp.text}")
except Exception as e:
    print(f"❌ Upload error: {e}")
