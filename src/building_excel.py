import pandas as pd
import datetime
import logging
import sqlite3
import os

from bog_api import initialize_db

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger(__name__)

# Mapping from report columns to DB fields or calculated fields
report_to_db_field_mapping = {
    "თარიღი": "entry_date",
    "დანიშნულება": "document_nomination",
    "გამგზავნის/მიმღების დასახელება": "sender_details_name", # Note: 'beneficiary_details_name' is merged into this in the script
    "დებეტი": "entry_amount_debit",
    "დებეტი ექვ ლარში": "entry_amount_debit_base",
    "კრედიტი": "entry_amount_credit",
    "კრედიტი ექვ ლარში": "entry_amount_credit_base",
    "ოპერაციის შინაარსი": "entry_comment",
    "კურსი": None, # Calculated by script (rate_per_unit_current)
    "ოპერაციის ტიპი": "document_product_group",
    "თანხა": None, # Calculated by script (calculated_amount)
    "თანხა ექვ ლარში": None, # Calculated by script (calculated_amount_gel_equiv)
    "ბრუნვა დებეტი": None, # Calculated by script (total_debit_fc)
    "ბრუნვა კრედიტი": None, # Calculated by script (total_credit_fc)
    "ნაშთი დღის ბოლოს": "closing_balance",
    "საწყისი ნაშთი": "opening_balance",
    "საწყისი ნაშთი ექვ ლარში": None, # Calculated by script (opening_balance_fc * previous_nbg_rate)
    "დამატებითი ინფორმაცია": "document_information",
    "Curr": "currency",
    "Company": "company",
    "Category": None, # Placeholder in script
    "Rule Name": None, # Placeholder in script
    "Status": None # Placeholder in script
}

def get_project_db_path():
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bank_data.db')

def read_transactions_from_sqlite(start_date_str=None, end_date_str=None, db_path=None, table_name='bog_transactions'):
    """
    Reads transactions from the SQLite database within a specified date range.
    """
    if db_path is None:
        db_path = get_project_db_path()
    _logger.info(f"Reading transactions from SQLite table '{table_name}' in '{db_path}'...")
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name}"
        conditions = []
        if start_date_str:
            conditions.append(f"STRFTIME('%Y-%m-%d', entry_date) >= '{start_date_str}'")
        if end_date_str:
            conditions.append(f"STRFTIME('%Y-%m-%d', entry_date) <= '{end_date_str}'")
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        df = pd.read_sql_query(query, conn)
        _logger.info(f"Successfully read {len(df)} transactions from SQLite.")
        if 'entry_date' in df.columns:
            df['entry_date'] = pd.to_datetime(df['entry_date'])
        else:
            _logger.error("'entry_date' column not found in database. Please verify table schema.")
            return pd.DataFrame()
        # Fill missing columns with default values (0 for numeric, None for others)
        required_cols = [
            'entry_amount_debit', 'entry_amount_credit',
            'entry_amount_debit_base', 'entry_amount_credit_base',
            'opening_balance', 'closing_balance'
        ]
        for col in required_cols:
            if col not in df.columns:
                _logger.warning(f"Column '{col}' not found in transactions. Will proceed, but calculations might be affected.")
                df[col] = 0.0
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
    except FileNotFoundError:
        _logger.error(f"SQLite database file not found at: {db_path}.")
        return pd.DataFrame()
    except pd.io.sql.DatabaseError as e:
        _logger.error(f"Database error when reading from SQLite: {e}")
        return pd.DataFrame()
    except Exception as e:
        _logger.error(f"An unexpected error occurred while reading from SQLite: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def build_report_df(df):
    """
    Build the report DataFrame using the report_to_db_field_mapping.
    Handles calculated and placeholder columns as needed.
    """
    report_df = pd.DataFrame()
    for report_col, db_col in report_to_db_field_mapping.items():
        if db_col is not None:
            if db_col in df.columns:
                report_df[report_col] = df[db_col]
            else:
                report_df[report_col] = None
        else:
            # Handle calculated or placeholder columns
            if report_col == "კურსი":
                report_df[report_col] = df.get("rate_per_unit_current", None)
            elif report_col == "თანხა":
                report_df[report_col] = df.get("calculated_amount", None)
            elif report_col == "თანხა ექვ ლარში":
                report_df[report_col] = df.get("calculated_amount_gel_equiv", None)
            elif report_col == "ბრუნვა დებეტი":
                report_df[report_col] = df.get("total_debit_fc", None)
            elif report_col == "ბრუნვა კრედიტი":
                report_df[report_col] = df.get("total_credit_fc", None)
            elif report_col == "საწყისი ნაშთი ექვ ლარში":
                report_df[report_col] = df.get("opening_balance_gel_equiv", None)
            else:
                report_df[report_col] = None
    return report_df

def generate_daily_bank_report(report_date_str):
    """
    Generates daily bank reports for BOG accounts, creating a separate Excel file for each company.
    Reads transactions from the local SQLite database.
    """
    _logger.info(f"Generating daily bank report for {report_date_str} using data from SQLite.")
    df_transactions = read_transactions_from_sqlite(end_date_str=report_date_str)
    if df_transactions.empty:
        _logger.warning(f"No transactions found in SQLite up to the report date {report_date_str}. No reports will be generated.")
        return
    # Build the report DataFrame using the mapping
    report_df = build_report_df(df_transactions)
    # Save to Excel
    output_dir = 'daily_reports'
    os.makedirs(output_dir, exist_ok=True)
    report_filename = os.path.join(output_dir, f"Bank_Report_BOG_{report_date_str}.xlsx")
    report_df.to_excel(report_filename, index=False)
    _logger.info(f"Daily bank report saved to {report_filename}")

def main():
    initialize_db()
    report_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    generate_daily_bank_report(report_date)

if __name__ == '__main__':
    main()