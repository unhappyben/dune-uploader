import tradermade as tm
import pandas as pd
import requests
import io
from datetime import datetime, timedelta
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

# === Fetch Friday and Monday FX Data ===
tm.set_rest_api_key(TRADERMADE_API_KEY)
pairlist = ",".join([f"{c}{BASE}" for c in CURRENCIES if c != BASE])

def fetch_fx(date_str):
    try:
        df = tm.historical(currency=pairlist, date=date_str, interval="daily", fields=["close"])
        print(f"✅ Fetched data for {date_str}")
        return df
    except Exception as e:
        print(f"❌ Error fetching {date_str}: {e}")
        return None

df_friday = fetch_fx(date_strs["friday"])
df_monday = fetch_fx(date_strs["monday"])

if df_friday is None or df_monday is None:
    print("❌ Cannot proceed without both Friday and Monday data.")
    exit(1)

# === Build FX Row Helper ===
def extract_fx(df, weight, label):
    rows = {}
    for _, row in df.iterrows():
        pair = row.get("instrument")
        if not isinstance(pair, str) or len(pair) < 6:
            continue
        base_cur = pair[:3]
        quote_cur = pair[3:]
        rate = row.get("close")
        if pd.isna(rate) or rate == 0:
            continue
        fx = rate if quote_cur == BASE else 1 / rate
        rows[base_cur] = weight * fx
    print(f"✅ Processed {label} with {len(rows)} entries")
    return rows

# === Weighted Average ===
fri_rates = extract_fx(df_friday, 1, "Friday")
mon_rates = extract_fx(df_monday, 1, "Monday")

saturday_rows = []
sunday_rows = []

for cur in CURRENCIES:
    if cur not in fri_rates or cur not in mon_rates:
        print(f"⚠️ Skipping {cur}, missing Friday or Monday rate.")
        continue

    sat_fx = round((2/3) * fri_rates[cur] + (1/3) * mon_rates[cur], 8)
    sun_fx = round((1/3) * fri_rates[cur] + (2/3) * mon_rates[cur], 8)

    saturday_rows.append({
        "date": date_strs["saturday"],
        "currency": cur,
        "fx_rate": sat_fx,
        "inverse_fx_rate": round(1 / sat_fx, 8)
    })

    sunday_rows.append({
        "date": date_strs["sunday"],
        "currency": cur,
        "fx_rate": sun_fx,
        "inverse_fx_rate": round(1 / sun_fx, 8)
    })

# Add USD base row
for date_label in ["saturday", "sunday"]:
    row = {
        "date": date_strs[date_label],
        "currency": BASE,
        "fx_rate": 1.0,
        "inverse_fx_rate": 1.0
    }
    if date_label == "saturday":
        saturday_rows.append(row)
    else:
        sunday_rows.append(row)

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
