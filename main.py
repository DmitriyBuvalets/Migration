import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json


# 🔧 Заміни на свої параметри
PROJECT_ID = "report"
DATASET_ID = "analytics"
TABLE_ID = "usd_uah_exchange_rates"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Зчитуємо ключі з secrets
creds_json = os.environ.get("GA_CREDS")

if not creds_json:
    raise ValueError("GA_CREDS is not set")

# Перетворюємо JSON-рядок у словник
creds_dict = json.loads(creds_json)

# Створюємо credentials
credentials = service_account.Credentials.from_service_account_info(creds_dict)

# Створюємо клієнт BigQuery
client = bigquery.Client(credentials=credentials, project=credentials.project_id)



# client = bigquery.Client(project='report', credentials=credentials)
# client = bigquery.Client(credentials=credentials, project=credentials.project_id)


# /***************************************************************************************************************************************/

def fetch_exchange_rates(currencies=("USD", "EUR")):
    url = "https://api.privatbank.ua/p24api/pubinfo?json&exchange&coursid=11"
    response = requests.get(url)
    data = response.json()

    today = datetime.now().date()
    results = []

    for currency in currencies:
        row = next((r for r in data if r["ccy"] == currency), None)
        if row:
            results.append({
                "date": today,
                "currency": row["ccy"],
                "base_currency": row["base_ccy"],
                "rate_type": "card_sell",
                "sale": float(row["sale"]),
                "buy": float(row["buy"])
            })
    return results

def check_if_exists(client, date, currency):
    query = f"""
        SELECT COUNT(*) as count
        FROM `{FULL_TABLE_ID}`
        WHERE DATE(date) = DATE('{date}')
          AND currency = '{currency}'
    """
    result = client.query(query).result()
    return next(result).count > 0

def insert_to_bigquery(client, rows):
    df = pd.DataFrame(rows)
    client.load_table_from_dataframe(df, FULL_TABLE_ID).result()

def main():
    client = bigquery.Client(project='report', credentials=credentials)
    rows = fetch_exchange_rates(["USD", "EUR"])

    to_insert = []
    for row in rows:
        if check_if_exists(client, row["date"], row["currency"]):
            print(f"🔁 Дані за {row['date']} для {row['currency']} вже є.")
        else:
            to_insert.append(row)

    if to_insert:
        insert_to_bigquery(client, to_insert)
        print(f"✅ Додано записи: {to_insert}")
    else:
        print("ℹ️ Нічого не додано. Дані вже є.")

if __name__ == "__main__":
    main()

