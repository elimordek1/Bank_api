import base64
import logging
import os
import sqlite3
import datetime
import pandas as pd
import requests
import json
import sys

# Setup logging (ensure this is configured once at the start)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger(__name__)

# Your company credentials (keeping this as is)
# read from secrets folder
try:
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    PROJECT_ROOT = os.getcwd()  # Fallback to current working directory

# Correct path to the secrets file
SECRETS_PATH = os.path.join(PROJECT_ROOT, 'secrets', 'bog_company_creds.json')

# Load the JSON file
try:
    COMPANY_CREDENTIALS_BOG = json.load(open(SECRETS_PATH))
except FileNotFoundError:
    _logger.error(f"File not found: {SECRETS_PATH}. Please ensure it exists.")
    raise


def read_accounts_from_excel(excel_file=None):
    if excel_file is None:
        try:
            PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        except NameError:
            PROJECT_ROOT = os.getcwd()  # Fallback to current working directory
        excel_file = os.path.join(PROJECT_ROOT, 'configs', 'banks.xlsx')
    try:
        df = pd.read_excel(excel_file, sheet_name=0)
        _logger.info(f"Successfully read Excel file. Initial DataFrame shape: {df.shape}")
        _logger.debug(f"Initial Excel DataFrame columns: {df.columns.tolist()}")

    except FileNotFoundError:
        _logger.error(f"Excel file not found at: {excel_file}. Please ensure it exists.")
        return pd.DataFrame()
    except Exception as e:
        _logger.error(f"Error reading Excel file: {e}")
        return pd.DataFrame()

    # Ensure 'ID' and 'Account Number' columns exist before processing
    if 'ID' not in df.columns:
        _logger.error("Column 'ID' not found in Excel file. Cannot derive 'company' and 'currency'.")
        return pd.DataFrame()
    if 'Account Number' not in df.columns:
        _logger.error("Column 'Account Number' not found in Excel file. Cannot derive 'account_number'.")
        return pd.DataFrame()

    # Apply operations only if columns exist
    try:
        df['company'] = df['ID'].apply(
            lambda x: x.split(' ')[2] if isinstance(x, str) and len(x.split(' ')) > 2 else None)
        df['currency'] = df['ID'].apply(
            lambda x: x.split(' ')[1] if isinstance(x, str) and len(x.split(' ')) > 1 else None)
        df['bank_name'] = df['ID'].apply(
            lambda x: x.split(' ')[0] if isinstance(x, str) and len(x.split(' ')) > 0 else None)
        df['account_number'] = df['Account Number'].apply(
            lambda x: str(x)[:-3] if isinstance(x, (str, int, float)) and len(
                str(x)) > 3 else x)  # Convert to string first for consistent slicing

    except Exception as e:
        _logger.error(f"Error processing 'ID' or 'Account Number' columns in Excel: {e}")
        return pd.DataFrame()

    df = df[['company', 'currency', 'bank_name', 'account_number']]
    df = df[df['account_number'].notna()]
    df = df[df['company'].notna()]  # Ensure company column is not null after splitting

    _logger.info(f"Processed Excel data. Filtered BOG accounts DataFrame shape: {df[df['bank_name'] == 'BOG'].shape}")
    _logger.debug(
        f"Filtered BOG accounts DataFrame columns after processing: {df[df['bank_name'] == 'BOG'].columns.tolist()}")
    return df[df['bank_name'] == 'BOG']


def initialize_db(db_path='bank_data.db'):
    """
    Initializes the SQLite database and creates the success_log table if it doesn't exist.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS success_log (
                company TEXT NOT NULL,
                account_number TEXT NOT NULL,
                currency TEXT NOT NULL,
                last_run_date TEXT NOT NULL,
                PRIMARY KEY (company, account_number, currency)
            )
        ''')
        conn.commit()
    _logger.info("Database initialized and success_log table ensured.")


def get_last_run_date(company, account_number, currency, db_path='bank_data.db'):
    """
    Retrieves the last successful run date for a given account.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT last_run_date FROM success_log
            WHERE company = ? AND account_number = ? AND currency = ?
        ''', (company, account_number, currency))
        result = cursor.fetchone()
    return result[0] if result else None


def update_last_run_date(company, account_number, currency, new_date, db_path='bank_data.db'):
    """
    Updates the last successful run date for a given account.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO success_log (company, account_number, currency, last_run_date)
            VALUES (?, ?, ?, ?)
        ''', (company, account_number, currency, new_date))
        conn.commit()
    _logger.info(f"Updated last run date for {company}-{account_number}-{currency} to {new_date}")


def fetch_transactions_for_account(client_id, client_secret, account_number, currency, start_date, end_date,
                                   company=None):
    _logger.info(f"Fetching transactions for account: {account_number} ({currency}) from {start_date} to {end_date}")
    auth_string = f"{client_id}:{client_secret}"
    auth_header = base64.b64encode(auth_string.encode()).decode()

    auth_url = 'https://account.bog.ge/auth/realms/bog/protocol/openid-connect/token'
    data = {'grant_type': 'client_credentials'}
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    try:
        response = requests.post(auth_url, headers=headers, data=data, timeout=15)  # Added timeout
        response.raise_for_status()
        access_token = response.json().get('access_token')
        if not access_token:
            _logger.error(f"Auth successful but no access token received for account {account_number}.")
            return []
    except requests.exceptions.Timeout:
        _logger.error(f"Authentication request timed out for account {account_number}.")
        return []
    except requests.exceptions.RequestException as e:
        _logger.error(f"Auth failed for account {account_number}: {e}")
        return []
    except Exception as e:
        _logger.error(f"An unexpected error occurred during authentication for account {account_number}: {e}")
        return []

    headers = {'Authorization': f'Bearer {access_token}'}
    statement_url = f"https://api.businessonline.ge/api/statement/{account_number}/{currency}/{start_date}/{end_date}"

    records = []
    statement_id = None
    try:
        response = requests.get(statement_url, headers=headers, timeout=30)  # Added timeout
        response.raise_for_status()
        response_json = response.json()
        records = response_json.get('Records', [])
        statement_id = response_json.get('Id')
        _logger.info(
            f"Retrieved statement ID: {statement_id} for account {account_number}. Found {len(records)} transactions.")

    except requests.exceptions.Timeout:
        _logger.error(f"Statement fetching request timed out for account {account_number}.")
        return []
    except requests.exceptions.RequestException as e:
        _logger.error(f"Failed to fetch transactions for account {account_number}: {e}")
        return []
    except Exception as e:
        _logger.error(f"An unexpected error occurred during transaction fetch for account {account_number}: {e}")
        return []

    daily_summaries = {}
    if statement_id:
        summary_url = f"https://api.businessonline.ge/api/statement/summary/{account_number}/{currency}/{statement_id}"
        try:
            summary_response = requests.get(summary_url, headers=headers, timeout=15)  # Added timeout
            summary_response.raise_for_status()
            summary_json = summary_response.json()
            for summary in summary_json.get('DailySummaries', []):
                summary_date_full = summary.get('Date')
                if summary_date_full:
                    summary_date = summary_date_full.split('T')[0]
                    daily_summaries[summary_date] = summary
                else:
                    _logger.warning(
                        f"Daily summary for account {account_number} contains an entry with no 'Date'. Skipping this entry.")
            _logger.info(f"Retrieved {len(daily_summaries)} daily summaries for account {account_number}")
        except requests.exceptions.Timeout:
            _logger.error(f"Daily summaries request timed out for account {account_number}.")
        except requests.exceptions.RequestException as e:
            _logger.error(f"Failed to fetch daily summaries for account {account_number}: {e}")
        except Exception as e:
            _logger.error(
                f"An unexpected error occurred during daily summaries fetch for account {account_number}: {e}")

    flattened = []
    for r in records:
        entry_date_str = r.get("EntryDate", "").split('T')[0]
        opening_balance = None
        closing_balance = None

        if entry_date_str in daily_summaries:
            summary = daily_summaries[entry_date_str]
            closing_balance = summary.get('Balance')
            credit_sum = summary.get('CreditSum', 0)
            debit_sum = summary.get('DebitSum', 0)
            if closing_balance is not None:
                opening_balance = closing_balance - credit_sum + debit_sum
        else:
            _logger.warning(
                f"No daily summary found for date {entry_date_str} for account {account_number}. Opening/Closing balances will be null.")

        flattened.append({
            "entry_date": r.get("EntryDate"),
            "entry_document_number": r.get("EntryDocumentNumber"),
            "entry_account_number": r.get("EntryAccountNumber"),
            "entry_amount_debit": r.get("EntryAmountDebit"),
            "entry_amount_debit_base": r.get("EntryAmountDebitBase"),
            "entry_amount_credit": r.get("EntryAmountCredit"),
            "entry_amount_credit_base": r.get("EntryAmountCreditBase"),
            "entry_amount_base": r.get("EntryAmountBase"),
            "entry_amount": r.get("EntryAmount"),
            "entry_comment": r.get("EntryComment"),
            "entry_department": r.get("EntryDepartment"),
            "entry_account_point": r.get("EntryAccountPoint"),
            "document_product_group": r.get("DocumentProductGroup"),
            "document_value_date": r.get("DocumentValueDate"),
            # Sender
            "sender_details_name": r.get("SenderDetails", {}).get("Name"),
            "sender_details_inn": r.get("SenderDetails", {}).get("Inn"),
            "sender_details_account_number": r.get("SenderDetails", {}).get("AccountNumber"),
            "sender_details_bank_code": r.get("SenderDetails", {}).get("BankCode"),
            "sender_details_bank_name": r.get("SenderDetails", {}).get("BankName"),
            # Beneficiary
            "beneficiary_details_name": r.get("BeneficiaryDetails", {}).get("Name"),
            "beneficiary_details_inn": r.get("BeneficiaryDetails", {}).get("Inn"),
            "beneficiary_details_account_number": r.get("BeneficiaryDetails", {}).get("AccountNumber"),
            "beneficiary_details_bank_code": r.get("BeneficiaryDetails", {}).get("BankCode"),
            "beneficiary_details_bank_name": r.get("BeneficiaryDetails", {}).get("BankName"),
            # Document details
            "document_treasury_code": r.get("DocumentTreasuryCode"),
            "document_nomination": r.get("DocumentNomination"),
            "document_information": r.get("DocumentInformation"),
            "document_source_amount": r.get("DocumentSourceAmount"),
            "document_source_currency": r.get("DocumentSourceCurrency"),
            "document_destination_amount": r.get("DocumentDestinationAmount"),
            "document_destination_currency": r.get("DocumentDestinationCurrency"),
            "document_receive_date": r.get("DocumentReceiveDate"),
            "document_branch": r.get("DocumentBranch"),
            "document_department": r.get("DocumentDepartment"),
            "document_actual_date": r.get("DocumentActualDate"),
            "document_expiry_date": r.get("DocumentExpiryDate"),
            "document_rate_limit": r.get("DocumentRateLimit"),
            "document_rate": r.get("DocumentRate"),
            "document_registration_rate": r.get("DocumentRegistrationRate"),
            "document_sender_institution": r.get("DocumentSenderInstitution"),
            "document_intermediary_institution": r.get("DocumentIntermediaryInstitution"),
            "document_beneficiary_institution": r.get("DocumentBeneficiaryInstitution"),
            "document_payee": r.get("DocumentPayee"),
            "document_correspondent_account_number": r.get("DocumentCorrespondentAccountNumber"),
            "document_correspondent_bank_code": r.get("DocumentCorrespondentBankCode"),
            "document_correspondent_bank_name": r.get("DocumentCorrespondentBankName"),
            "document_key": r.get("DocumentKey"),
            "entry_id": r.get("EntryID"),
            "doc_comment": r.get("DocComment"),
            "document_payer_inn": r.get("DocumentPayerInn"),
            "document_payer_name": r.get("DocumentPayerName"),
            "company": company,
            "currency": currency,
            "account_number": account_number,
            "opening_balance": opening_balance,
            "closing_balance": closing_balance,
        })

    return flattened


def get_all_transactions(end_date):
    """
    Fetches transactions for all BOG accounts from their last run date up to the specified end_date.
    """
    initialize_db()  # Ensure the database and success_log table exist
    accounts_df = read_accounts_from_excel()

    # --- Debugging check ---
    if accounts_df.empty:
        _logger.error("read_accounts_from_excel returned an empty DataFrame. Cannot proceed.")
        return pd.DataFrame()
    _logger.info(
        f"Accounts DataFrame from Excel has {len(accounts_df)} rows and columns: {accounts_df.columns.tolist()}")
    # --- End Debugging check ---

    # Check for empty or malformed DataFrame
    required_cols = {'company', 'currency', 'account_number'}
    if not required_cols.issubset(accounts_df.columns):
        _logger.error(
            f"Accounts DataFrame is missing required columns. Expected: {required_cols}, Found: {accounts_df.columns.tolist()}")
        return pd.DataFrame()

    all_records = []
    for _, row in accounts_df.iterrows():
        company = row['company']
        currency = row['currency']
        account_number = row['account_number']

        # Determine start date for fetching
        last_run_date_str = get_last_run_date(company, account_number, currency)
        if last_run_date_str:
            # Fetch from the day after the last successful run
            start_date = (datetime.datetime.strptime(last_run_date_str, '%Y-%m-%d').date() + datetime.timedelta(
                days=1)).strftime('%Y-%m-%d')
        else:
            # If no last run date, fetch for a reasonable historical period, e.g., 30 days back from end_date
            start_date = (
                        datetime.datetime.strptime(end_date, '%Y-%m-%d').date() - datetime.timedelta(days=3)).strftime(
                '%Y-%m-%d')
            _logger.info(f"No previous run date for {company}-{account_number}-{currency}. Fetching from {start_date}.")

        # Ensure start_date does not exceed end_date
        if datetime.datetime.strptime(start_date, '%Y-%m-%d').date() > datetime.datetime.strptime(end_date,
                                                                                                  '%Y-%m-%d').date():
            _logger.info(
                f"Start date {start_date} is after end date {end_date} for {company}-{account_number}-{currency}. Skipping.")
            continue

        creds = COMPANY_CREDENTIALS_BOG.get(company)
        if not creds:
            _logger.warning(f"No credentials for company: {company}. Skipping.")
            continue

        records = fetch_transactions_for_account(
            creds['client_id'],
            creds['client_secret'],
            account_number,
            currency,
            start_date,
            end_date,
            company=company
        )
        if records:
            all_records.extend(records)
            # Update last run date if transactions were successfully fetched
            update_last_run_date(company, account_number, currency, end_date)

    if all_records:
        # Before creating DataFrame, ensure all records have the same keys,
        # otherwise, pd.DataFrame will not create all columns properly
        # Collect all unique keys from all dictionaries in all_records
        all_keys = set().union(*(d.keys() for d in all_records))
        # For each record, add missing keys with None as value
        for record in all_records:
            for key in all_keys:
                if key not in record:
                    record[key] = None

        df_result = pd.DataFrame(all_records)
        _logger.info(f"Successfully created DataFrame with {len(df_result)} transactions.")
        _logger.debug(f"Final DataFrame columns: {df_result.columns.tolist()}")
        return df_result
    _logger.warning("No transactions fetched from any account. Returning empty DataFrame.")
    return pd.DataFrame()


def write_transactions_to_sqlite(df, db_path='bank_data.db', table_name='bog_transactions'):
    """
    Write a DataFrame of BOG transactions to a SQLite3 database.
    """
    if df.empty:
        _logger.warning("No transactions to write to database.")
        return
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists='append', index=False)
        _logger.info(f"Written {len(df)} transactions to {table_name} in {db_path}")


def fetch_transactions_for_specific_day(specific_date):
    """
    Fetches transactions for all BOG accounts for a specific day.

    Args:
        specific_date (str): The date for which transactions are to be fetched (YYYY-MM-DD).
    """
    try:
        # Validate the date format
        specific_date_obj = datetime.datetime.strptime(specific_date, '%Y-%m-%d').date()
    except ValueError:
        _logger.error(f"Invalid date format: {specific_date}. Please use YYYY-MM-DD.")
        return pd.DataFrame()

    initialize_db()  # Ensure the database and success_log table exist
    accounts_df = read_accounts_from_excel()

    if accounts_df.empty:
        _logger.error("No accounts found in the Excel file. Cannot proceed.")
        return pd.DataFrame()

    required_cols = {'company', 'currency', 'account_number'}
    if not required_cols.issubset(accounts_df.columns):
        _logger.error(
            f"Accounts DataFrame is missing required columns. Expected: {required_cols}, Found: {accounts_df.columns.tolist()}")
        return pd.DataFrame()

    all_records = []
    for _, row in accounts_df.iterrows():
        company = row['company']
        currency = row['currency']
        account_number = row['account_number']

        creds = COMPANY_CREDENTIALS_BOG.get(company)
        if not creds:
            _logger.warning(f"No credentials for company: {company}. Skipping.")
            continue

        records = fetch_transactions_for_account(
            creds['client_id'],
            creds['client_secret'],
            account_number,
            currency,
            specific_date,
            specific_date,
            company=company
        )
        if records:
            all_records.extend(records)

    if all_records:
        all_keys = set().union(*(d.keys() for d in all_records))
        for record in all_records:
            for key in all_keys:
                record.setdefault(key, None)

        df_result = pd.DataFrame(all_records)
        _logger.info(f"Successfully fetched transactions for {specific_date}. Total records: {len(df_result)}")
        return df_result

    _logger.warning(f"No transactions fetched for {specific_date}. Returning empty DataFrame.")
    return pd.DataFrame()


def fetch_and_write_transactions_for_specific_day(specific_date, db_path='bank_data.db', table_name='bog_transactions'):
    """
    Fetches transactions for all BOG accounts for a specific day and writes them to the SQLite database.

    Args:
        specific_date (str): The date for which transactions are to be fetched (YYYY-MM-DD).
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table to write transactions to.
    """
    # Fetch transactions for the specific day
    df_transactions = fetch_transactions_for_specific_day(specific_date)

    if df_transactions.empty:
        _logger.warning(f"No transactions fetched for {specific_date}. Nothing to write to the database.")
        return

    # Write transactions to the SQLite database
    write_transactions_to_sqlite(df_transactions, db_path=db_path, table_name=table_name)
    _logger.info(f"Transactions for {specific_date} successfully written to the database.")
    return df_transactions


if __name__ == '__main__':
    # Initialize the database (creates success_log table if not exists)
    initialize_db()

    # Define the end date for fetching transactions
    # Set to today's date in Tbilisi for accurate daily fetches
    #   today = datetime.date.today()
    #   end_date_for_fetch = today.strftime('%Y-%m-%d')

    #  _logger.info(f"Starting transaction fetch for end date: {end_date_for_fetch}")
    #  df = get_all_transactions(end_date_for_fetch)

    # if not df.empty:
    #   write_transactions_to_sqlite(df)
    #      print("\n--- Fetched Transactions Sample (first 5 rows) ---")
    #     print(df.head())
    #      print(f"\nTotal transactions fetched: {len(df)}")

    specific_date = '2025-05-29'  # Replace with the desired date
    df_spec_date = fetch_and_write_transactions_for_specific_day(specific_date)
