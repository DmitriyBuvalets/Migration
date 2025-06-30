"""
apps_fluer_migrate_data_to_bigquery

📦 Призначення:
    Скрипт автоматизує міграцію тижневих даних Appsflyer (сесії / події Android та iOS)
    з CSV-файлів у Google Drive до відповідних таблиць BigQuery.

🔍 Як працює:
    - Проходить по всіх підпапках вказаної папки Google Drive.
    - Для кожного префікса (android_events_all, ios_sessions_cashback тощо):
        - Шукає файл формату `weekly_data_{prefix}_{week_start}.csv`.
        - Завантажує, очищує та нормалізує дані (фінансові поля, дати, числові значення).
        - Вставляє в таблицю BigQuery, якщо дані ще не імпортовані.

📁 Вхідні дані:
    - Google Drive, структура:
        └── Appsflyer/
            ├── Android Session-ALL/
            ├── Android Event-Cashback/
            └── ...
    - Файли: CSV-формату з назвою: weekly_data_{prefix}_{date}.csv

🗃️ Вивід:
    - Дані додаються в таблиці BigQuery: report.apps_fluer.{prefix}

🛠️ Залежності:
    - pandas
    - google-api-python-client
    - google-cloud-bigquery
    - pandas-gbq
    - python-dateutil, pyarrow, re, io

🧠 Додатково:
    - Формування тижнів: неділя — субота
    - Пропускає тиждень, якщо файл не знайдено або дані вже завантажені
    - Підтримка для events та sessions-файлів

Автор: Дмитрій 👨‍💻
"""

import os
import re
import io
import logging
import datetime as dt
from typing import List, Tuple
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import bigquery
from pandas_gbq import to_gbq

def apps_fluer_migrate_data_to_bigquery():
    # === 🔐 Авторизація ===
    SERVICE_ACCOUNT_FILE = "C:\\Users\\user\\Desktop\\Finance\\DRIVE_Scripts_Pandas\\ga_creds.json"
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build("drive", "v3", credentials=credentials)
    credentials_bq = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    bq_client = bigquery.Client(credentials=credentials_bq, project=credentials_bq.project_id)

    # === 📁 Налаштування ===
    FOLDER_ID = '1H45DT2ELHoJKFe-rvbBaM14Y5V2Gdrxc'  # Головна папка Appsflyer
    project_id = "flowers-reporting" 

    prefixes = [
                 "android_sessions_all"   
        # "android_events_agency", "android_events_all", "android_events_cashback", "android_events_fb",
        # "android_sessions_agency", "android_sessions_all", "android_sessions_cashback", "android_sessions_fb",
        # "ios_events_agency", "ios_events_all", "ios_events_cashback", "ios_sessions_agency",
        # "ios_sessions_all", "ios_sessions_cashback", "ios_sessions_fb"
    ]

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\user\Downloads\flowers\flowers\ga_creds.json"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # === 📅 Генерація тижнів місяця ===
    def get_month_weeks(target_date: dt.date = None) -> List[Tuple[dt.date, dt.date]]:
        if target_date is None:
            target_date = dt.date.today()
        year, month = target_date.year, target_date.month
        first_day = dt.date(year, month, 1)
        next_month = dt.date(year + 1, 1, 1) if month == 12 else dt.date(year, month + 1, 1)
        last_day = next_month - dt.timedelta(days=1)
        range_start = first_day - dt.timedelta(days=7)
        range_end = last_day + dt.timedelta(days=7)
        sundays = pd.date_range(start=range_start, end=range_end, freq='W-SUN')
        return [(s.date(), (s + pd.Timedelta(days=6)).date()) for s in sundays if s.month == month or (s + pd.Timedelta(days=6)).month == month]

    # === 📂 Пошук файлу у всіх підпапках ===
    def find_file_in_subfolders(service, parent_folder_id, exact_file_name):
        query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        subfolders = service.files().list(q=query, fields="files(id, name)").execute().get("files", [])
        for folder in subfolders:
            q = f"'{folder['id']}' in parents and name = '{exact_file_name}' and mimeType = 'text/csv' and trashed = false"
            results = service.files().list(q=q, fields="files(id, name, modifiedTime)").execute()
            files = results.get("files", [])
            if files:
                logging.info(f"✅ Знайдено файл '{files[0]['name']}' у '{folder['name']}'")
                return files[0]
        return None
    
    # === 📥 Завантаження CSV з Google Drive у pandas DataFrame ===
    def download_csv_to_dataframe(file_id, prefix):
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        table = bq_client.get_table(bq_client.dataset("apps_fluer").table(prefix))
        bq_columns = [field.name for field in table.schema]
        if any(x in prefix for x in ['events_agency', 'events_all', 'events_cashback']):
            return pd.read_csv(fh, skiprows=2, header=None, names=bq_columns)
        else:
            return pd.read_csv(fh, skiprows=1, header=None, names=bq_columns)
        
    # === 💲 Очищення грошових значень (валюта, форматування, коми) ===
    def clean_revenue(val):
        if pd.isna(val): return None
        cleaned = re.sub(r'[^\d,.]', '', str(val)).replace(",", ".").strip()
        try:
            return float(cleaned)
        except:
            return None
        
    # === 🧼 Очищення даних: фінансові й числові колонки, дати тижня ===
    def clean_data(df, prefix, date_from, date_to):
        numeric_cols = {
            
            "android_events_agency": ["af_purchase_event_counter", "average_daily_unique_users"],
            
            "android_events_all": ["af_purchase_event_counter", "average_daily_unique_users"],
            
            "android_events_cashback": ["campaign_id", "af_purchase_event_counter", "average_daily_unique_users"], 
            "android_events_fb": ["campaign_id", "af_purchase_event_counter", "average_daily_unique_users"],
            
            "android_sessions_agency": ["clicks", "total_conversions", "installs", 
                                        "re_attribution", "re_engagement", "activity_sessions", "average_ecpi", "average_dau","average_mau"],
                    
            "android_sessions_all": ["clicks", "total_conversions", "installs",
                                    "re_attribution", "re_engagement", "activity_sessions", "average_ecpi", "average_dau", "average_mau"],
            
            
            "android_sessions_cashback": ["campaign_id", "clicks", "total_conversions", "installs",
                                    "re_attribution", "re_engagement",  "activity_sessions", "average_ecpi", "average_dau", "average_mau"],
            
            "android_sessions_fb": ["campaign_id", "clicks", "total_conversions", "installs",
                                    "re_attribution", "re_engagement",  "activity_sessions", "average_ecpi", "average_dau", "average_mau"],
            
            
            "ios_events_agency": ["af_purchase_event_counter", "average_daily_unique_users"],
            "ios_events_all": ["af_purchase_event_counter", "average_daily_unique_users"],
            
            "ios_events_cashback": ["campaign_id", "af_purchase_event_counter", "average_daily_unique_users"], 
            
            "ios_sessions_agency": ["clicks", "total_conversions", "installs", 
                                        "re_attribution", "re_engagement", "activity_sessions", "average_ecpi", "average_dau","average_mau"],
            
            "ios_sessions_all": ["clicks", "total_conversions", "installs", 
                                        "re_attribution", "re_engagement", "activity_sessions", "average_ecpi", "average_dau","average_mau"],
            
            "ios_sessions_cashback": ["campaign_id", "clicks", "total_conversions", "installs",
                                    "re_attribution", "re_engagement",  "activity_sessions", "average_ecpi", "average_dau", "average_mau"],
            
            "ios_sessions_fb": ["campaign_id", "clicks", "total_conversions", "installs",
                                    "re_attribution", "re_engagement", "activity_sessions", "average_ecpi", "average_dau", "average_mau"]}
        
        financial_cols = ["revenue", "activity_revenue", "arpdau", "average_dau_mau_rate", "conversion_rate", "cost"]
        for col in financial_cols:
            if col in df.columns:
                df[col] = df[col].apply(clean_revenue)
        for key in numeric_cols:
            if key in prefix:
                for col in numeric_cols[key]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col].astype(str).str.replace(r"\xa0| ", "", regex=True), errors='coerce')
        if 'week_start_date' in df.columns:
            df["week_start_date"] = date_from
            df["week_end_date"] = date_to
        return df
    
    # === ✅ Перевірка, чи дані за тиждень вже є в BigQuery ===
    def is_data_loaded(destination_table, week_end_date):
        query = f"SELECT 1 FROM `{project_id}.{destination_table}` WHERE week_end_date = @date LIMIT 1"
        job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", str(week_end_date))])
        return bq_client.query(query, job_config=job_config).result().total_rows > 0
    
    # === 🚀 Завантаження DataFrame до BigQuery у відповідну таблицю ===
    def upload_to_bigquery(df, destination_table, file_name):
        schema = bq_client.get_table(destination_table).schema
        to_gbq(df, destination_table=f"{project_id}.{destination_table}",
               project_id=project_id, credentials=credentials_bq,
               if_exists="append", table_schema=[{"name": f.name, "type": f.field_type} for f in schema])
        logging.info(f"📤 Завантажено файл '{file_name}' у таблицю {destination_table}")
        
    # === 🔁 Обробка 1 файлу: пошук → завантаження → очищення → BQ ===
    def process_file(prefix, date_from, date_to):
        file_name = f"weekly_data_{prefix}_{date_from}.csv"
        file_drive = find_file_in_subfolders(service, FOLDER_ID, file_name)
        if not file_drive:
            logging.warning(f"⛔ Файл {file_name} не знайдено.")
            return
        destination_table = f"apps_fluer.{prefix}"
        if is_data_loaded(destination_table, date_to):
            logging.info(f"⚠️ Дані за {date_to} вже є у {destination_table}")
            return
        try:
            df = download_csv_to_dataframe(file_drive["id"], prefix)
            df = clean_data(df, prefix, date_from, date_to)
            upload_to_bigquery(df, destination_table, file_name)
        except Exception as e:
            logging.error(f"❌ Помилка обробки {file_name}: {e}")

    for date_from, date_to in get_month_weeks():
        for prefix in prefixes:
            process_file(prefix, date_from, date_to)

# ▶️ Запуск
apps_fluer_migrate_data_to_bigquery()
