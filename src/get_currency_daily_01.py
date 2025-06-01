import requests
import datetime
import pandas as pd
import sqlite3
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

DB_PATH = r'C:\Users\arkik\DataspellProjects\POLI_BANK\src\bank_api\api\bank_data.db'
TABLE_NAME = 'nbg_currency'

def fetch_nbg_currency_df(date):
    date_str = date.strftime('%Y-%m-%d')
    url = f'https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json/?date={date_str}'

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data or 'currencies' not in data[0]:
            _logger.warning(f"No valid currency data for {date_str}")
            return pd.DataFrame()

        currencies = data[0]['currencies']
        records = []
        for c in currencies:
            code = c.get('code')
            rate = float(c.get('rate', 0))
            quantity = float(c.get('quantity', 1))
            rate_per_unit = rate / quantity if quantity else 0
            records.append({
                'date': date_str,
                'currency': code,
                'rate_per_unit': rate_per_unit
            })

        return pd.DataFrame(records)

    except Exception as e:
        _logger.error(f"Failed to fetch NBG data for {date_str}: {e}")
        return pd.DataFrame()

def get_latest_date_from_db(db_path=DB_PATH, table_name=TABLE_NAME):
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT MAX(date) FROM {table_name}")
            result = cursor.fetchone()[0]
            if result:
                return datetime.datetime.strptime(result, '%Y-%m-%d').date()
    except Exception as e:
        _logger.warning(f"Could not determine latest date: {e}")
    return None

def write_df_to_sqlite(df, db_path=DB_PATH, table_name=TABLE_NAME):
    if df.empty:
        _logger.info("No data to write.")
        return
    try:
        with sqlite3.connect(db_path) as conn:
            df.to_sql(table_name, conn, if_exists='append', index=False)
            _logger.info(f"Wrote {len(df)} rows to {table_name}")
    except Exception as e:
        _logger.error(f"Failed to write to DB: {e}")

def fetch_and_store_missing_nbg_rates():
    latest_date = get_latest_date_from_db()
    today = datetime.date.today()

    if latest_date is None:
        latest_date = today - datetime.timedelta(days=30)  # fallback: go 30 days back

    current_date = latest_date + datetime.timedelta(days=1)
    while current_date <= today:
        _logger.info(f"Fetching data for {current_date}")
        df = fetch_nbg_currency_df(current_date)
        write_df_to_sqlite(df)
        current_date += datetime.timedelta(days=1)

# Run the process
if __name__ == '__main__':
    fetch_and_store_missing_nbg_rates()
