import requests
import xml.etree.ElementTree as Et
import xmltodict
import json
import os
import pandas as pd
import logging
from datetime import datetime, timedelta
import sqlite3
import ast

# Set up logging
logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

# Constants
TBC_CERT_BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'secrets', 'tbc_certs')
TBC_CREDENTIALS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'secrets', 'tbc_credentials.json')

# Abbreviation map
CERTIFICATE_COMPANIES = {
    'BRG': 'BEST RETAIL GEORGIA LLC',
    'FRG': 'FASHION RETAIL GEORGIA LLC',
    'GAG': 'GLOBAL APPAREL GEORGIA LLC',
    'MHR': 'MASTER HOME RETAIL LLC',
    'MRG': 'MASTER RETAIL GEORGIA LLC',
    'MSG': 'MEGA STORE GEORGIA LLC',
    'PRG': 'PRO RETAIL GEORGIA LLC',
    'RGG': 'RETAIL GROUP GEORGIA LLC',
    'RGH': 'RETAIL GROUP HOLDING LLC',
    'SRG': 'SPANISH RETAIL GEORGIA LLC',
}

# TBC credentials
with open(TBC_CREDENTIALS_PATH, 'r') as f:
    TBC_CREDENTIALS = json.load(f)

# SOAP Setup
TBC_URL = "https://secdbi.tbconline.ge/dbi/dbiService"
HEADERS = {
    'Content-Type': 'text/xml; charset=utf-8',
}
NAMESPACES = {
    'ns2': 'http://www.mygemini.com/schemas/mygemini'
}

# Movement Request SOAP Payload
MOVEMENTS_PAYLOAD = '''
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:myg="http://www.mygemini.com/schemas/mygemini" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
    <soapenv:Header>
        <wsse:Security>
            <wsse:UsernameToken>
                <wsse:Username>{username}</wsse:Username>
                <wsse:Password>{password}</wsse:Password>
                <wsse:Nonce>{digipass}</wsse:Nonce>
            </wsse:UsernameToken>
        </wsse:Security>
    </soapenv:Header>
    <soapenv:Body>
        <myg:GetAccountMovementsRequestIo>
            <myg:accountMovementFilterIo>
                <myg:accountNumber>{account_number}</myg:accountNumber>
                <myg:accountCurrencyCode>{currency}</myg:accountCurrencyCode>
                <myg:periodFrom>{start_datetime}</myg:periodFrom>
                <myg:periodTo>{end_datetime}</myg:periodTo>
            </myg:accountMovementFilterIo>
        </myg:GetAccountMovementsRequestIo>
    </soapenv:Body>
</soapenv:Envelope>
'''

# Statement Request SOAP Payload
STATEMENT_PAYLOAD = '''
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:myg="http://www.mygemini.com/schemas/mygemini" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
    <soapenv:Header>
        <wsse:Security>
            <wsse:UsernameToken>
                <wsse:Username>{username}</wsse:Username>
                <wsse:Password>{password}</wsse:Password>
                <wsse:Nonce>{digipass}</wsse:Nonce>
            </wsse:UsernameToken>
        </wsse:Security>
    </soapenv:Header>
    <soapenv:Body>
        <myg:GetAccountStatementRequestIo>
            <myg:filter>
                <myg:periodFrom>{start_date}</myg:periodFrom>
                <myg:periodTo>{end_date}</myg:periodTo>
                <myg:accountNumber>{account_number}</myg:accountNumber>
                <myg:currency>{currency}</myg:currency>
            </myg:filter>
        </myg:GetAccountStatementRequestIo>
    </soapenv:Body>
</soapenv:Envelope>
'''

def remove_namespaces(data):
    if isinstance(data, dict):
        return {key.split(':')[-1]: remove_namespaces(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [remove_namespaces(item) for item in data]
    return data

def get_cert_paths(company_abbr):
    company_name = CERTIFICATE_COMPANIES[company_abbr]
    folder_path = os.path.join(TBC_CERT_BASE_PATH, company_name)
    return (
        os.path.join(folder_path, 'server_cert.pem'),
        os.path.join(folder_path, 'key_unencrypted.pem'),
    )

def read_accounts_from_excel(excel_file=None):
    if excel_file is None:
        # Always use the configs/banks.xlsx relative to the project root
        excel_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'configs', 'banks.xlsx')
    try:
        df = pd.read_excel(excel_file, sheet_name=0)
    except Exception as e:
        _logger.error(f"Error reading Excel file: {e}")
        return pd.DataFrame()

    df['company'] = df['ID'].apply(lambda x: x.split(' ')[2])
    df['currency'] = df['ID'].apply(lambda x: x.split(' ')[1])
    df['bank_name'] = df['ID'].apply(lambda x: x.split(' ')[0])
    df['account_number'] = df['Account Number'].apply(lambda x: x[:-3] if isinstance(x, str) and len(x) > 3 else x)
    df = df[['company', 'currency', 'bank_name', 'account_number']]
    df = df[df['account_number'].notna()]
    return df[df['bank_name'] == 'TBC']

def make_soap_request(company_abbr, payload, soap_action=None):
    """Make a SOAP request to TBC API using the company certificates"""
    creds = TBC_CREDENTIALS[company_abbr]
    cert_path = get_cert_paths(company_abbr)

    headers = HEADERS.copy()
    if soap_action:
        headers['SOAPAction'] = soap_action

    try:
        response = requests.post(
            TBC_URL,
            data=payload,
            headers=headers,
            cert=cert_path,
            verify=True
        )
        response.raise_for_status()
        return response.content.decode('utf-8')
    except Exception as e:
        _logger.error(f"TBC API Error for company {company_abbr}: {str(e)}")
        raise

def get_account_statement(company_abbr, account_number, currency, start_date, end_date):
    """Get account statement with opening and closing balances"""
    _logger.info(f"Fetching statement for {company_abbr}, account: {account_number}, currency: {currency}")

    creds = TBC_CREDENTIALS[company_abbr]

    payload = STATEMENT_PAYLOAD.format(
        username=creds['username'],
        password=creds['password'],
        digipass='1111',
        account_number=account_number,
        currency=currency,
        start_date=start_date,
        end_date=end_date
    )

    try:
        xml_response = make_soap_request(
            company_abbr,
            payload,
            soap_action='http://www.mygemini.com/schemas/mygemini/GetAccountStatement'
        )

        root = Et.fromstring(xml_response)
        raw_data = xmltodict.parse(Et.tostring(root, encoding='unicode'))
        cleaned_data = remove_namespaces(raw_data)

        statement = cleaned_data['Envelope']['Body']['GetAccountStatementResponseIo'].get('statement', {})
        return {
            'opening_date': statement.get('openingDate'),
            'opening_balance': statement.get('openingBalance'),
            'closing_date': statement.get('closingDate'),
            'closing_balance': statement.get('closingBalance'),
            'credit_sum': statement.get('creditSum'),
            'debit_sum': statement.get('debitSum'),
            'currency': statement.get('currency')
        }
    except Exception as e:
        _logger.error(f"Error getting statement for account {account_number}: {str(e)}")
        return {
            'opening_date': start_date,
            'opening_balance': None,
            'closing_date': end_date,
            'closing_balance': None,
            'credit_sum': None,
            'debit_sum': None,
            'currency': currency
        }

def get_transactions(company_abbr, account_number, currency, start_date, end_date):
    """Get all transactions with statement data"""

    # Format dates for movements API
    start_datetime = f"{start_date}T00:00:00.000"
    end_datetime = f"{end_date}T23:59:59.999"

    _logger.info(f"Fetching TBC transactions for {company_abbr}, account: {account_number}, currency: {currency}")

    # First get the account statement for balances
    statement = get_account_statement(company_abbr, account_number, currency, start_date, end_date)

    # Now get the movements/transactions
    creds = TBC_CREDENTIALS[company_abbr]

    payload = MOVEMENTS_PAYLOAD.format(
        username=creds['username'],
        password=creds['password'],
        digipass='1111',
        account_number=account_number,
        currency=currency,
        start_datetime=start_datetime,
        end_datetime=end_datetime
    )

    try:
        xml_response = make_soap_request(
            company_abbr,
            payload,
            soap_action='http://www.mygemini.com/schemas/mygemini/GetAccountMovements'
        )

        root = Et.fromstring(xml_response)
        raw_data = xmltodict.parse(Et.tostring(root, encoding='unicode'))
        cleaned_data = remove_namespaces(raw_data)

        # Extract movements
        movements = cleaned_data['Envelope']['Body']['GetAccountMovementsResponseIo'].get('accountMovement', [])

        # If only one transaction exists, wrap it in a list
        if isinstance(movements, dict):
            movements = [movements]

        # Add company, account, and statement info to each movement
        for movement in movements:
            # Add basic account info
            movement['company'] = company_abbr
            movement['currency'] = currency
            movement['account_number'] = account_number

            # Add statement info
            movement['opening_date'] = statement['opening_date']
            movement['opening_balance'] = statement['opening_balance']
            movement['closing_date'] = statement['closing_date']
            movement['closing_balance'] = statement['closing_balance']
            movement['statement_credit_sum'] = statement['credit_sum']
            movement['statement_debit_sum'] = statement['debit_sum']

        return movements
    except Exception as e:
        _logger.error(f"Error processing movements for account {account_number}: {str(e)}")
        return []

def get_all_transactions(start_date, end_date):
    """
    Get all transactions for all TBC accounts from the Excel file

    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        DataFrame: All transactions
    """
    accounts_df = read_accounts_from_excel()
    all_transactions = []

    for _, row in accounts_df.iterrows():
        company = row['company']
        currency = row['currency']
        account_number = row['account_number']

        if company not in TBC_CREDENTIALS:
            _logger.warning(f"No credentials for company: {company}")
            continue

        try:
            transactions = get_transactions(
                company,
                account_number,
                currency,
                start_date,
                end_date
            )
            all_transactions.extend(transactions)
        except Exception as e:
            _logger.error(f"Failed to get transactions for {company} {account_number}: {str(e)}")

    # Convert to DataFrame
    if all_transactions:
        df = pd.DataFrame(all_transactions)
        df['amount'] = df['amount'].apply(lambda x: float(x['amount']))
        return df
    else:
        _logger.warning("No transactions found for the specified period")
        return pd.DataFrame()

def write_transactions_to_sqlite(df, db_path=None, table_name='tbc_transactions'):
    """
    Write a DataFrame of TBC transactions to a SQLite3 database.
    """
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bank_data.db')
    if df.empty:
        _logger.warning("No transactions to write to database.")
        return

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        # Get existing columns
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_cols = set([row[1] for row in cursor.fetchall()])

        # Add missing columns
        for col in df.columns:
            if col not in existing_cols:
                # Default to TEXT type, or infer from df.dtypes if you want
                cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" TEXT')
                _logger.info(f"Added missing column '{col}' to {table_name}")

        # Now write the data
        df.to_sql(table_name, conn, if_exists='append', index=False)
        _logger.info(f"Written {len(df)} transactions to {table_name} in {db_path}")

def get_last_successful_date(db_path=None, table_name='download_log'):
    """
    Get the last successful download date from the download log table.

    Returns:
        str: Last successful date in YYYY-MM-DD format, or None if no records exist
    """
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bank_data.db')
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            # Create table if it doesn't exist
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    date TEXT PRIMARY KEY,
                    status TEXT,
                    transaction_count INTEGER,
                    timestamp TEXT
                )
            ''')
            # Get the last successful date
            cursor.execute(f'''
                SELECT date FROM {table_name} 
                WHERE status = 'success' 
                ORDER BY date DESC 
                LIMIT 1
            ''')
            result = cursor.fetchone()
            return result[0] if result else None
    except Exception as e:
        _logger.error(f"Error getting last successful date: {e}")
        return None

def log_download_status(date, status, transaction_count=0, db_path=None, table_name='download_log'):
    """
    Log the download status for a specific date.

    Args:
        date (str): Date in YYYY-MM-DD format
        status (str): 'success' or 'failed'
        transaction_count (int): Number of transactions downloaded
    """
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bank_data.db')
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            # Insert or replace the log entry
            cursor.execute(f'''
                INSERT OR REPLACE INTO {table_name} 
                (date, status, transaction_count, timestamp) 
                VALUES (?, ?, ?, ?)
            ''', (date, status, transaction_count, datetime.now().isoformat()))
            conn.commit()
            _logger.info(f"Logged download status for {date}: {status} ({transaction_count} transactions)")
    except Exception as e:
        _logger.error(f"Error logging download status: {e}")

def get_next_date(date_str):
    """
    Get the next date from a given date string.

    Args:
        date_str (str): Date in YYYY-MM-DD format

    Returns:
        str: Next date in YYYY-MM-DD format
    """
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    next_date = date_obj + timedelta(days=1)
    return next_date.strftime('%Y-%m-%d')

def get_today_date():
    """
    Get today's date in YYYY-MM-DD format.

    Returns:
        str: Today's date in YYYY-MM-DD format
    """
    return datetime.now().strftime('%Y-%m-%d')

def download_missing_days(start_date=None, max_retries=3):
    """
    Download transactions for missing days between last successful date and today.

    Args:
        start_date (str, optional): Override start date. If None, uses last successful date + 1
        max_retries (int): Maximum number of retries for failed downloads
    """
    today = get_today_date()

    if start_date is None:
        last_successful_date = get_last_successful_date()
        if last_successful_date is None:
            # If no previous downloads, start from 30 days ago
            start_date_obj = datetime.now() - timedelta(days=3)
            start_date = start_date_obj.strftime('%Y-%m-%d')
            _logger.info(f"No previous downloads found. Starting from {start_date}")
        else:
            start_date = get_next_date(last_successful_date)
            _logger.info(f"Last successful download: {last_successful_date}. Starting from {start_date}")

    current_date = start_date
    total_downloaded = 0

    while current_date < today:
        _logger.info(f"Processing date: {current_date}")

        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                # Download transactions for current date
                df = get_all_transactions(current_date, current_date)

                # Write to database (even if empty)
                if not df.empty:
                    write_transactions_to_sqlite(df)
                    transaction_count = len(df)
                    total_downloaded += transaction_count
                    _logger.info(f"Downloaded {transaction_count} transactions for {current_date}")
                else:
                    transaction_count = 0
                    _logger.info(f"No transactions found for {current_date}")

                # Log successful download
                log_download_status(current_date, 'success', transaction_count)
                success = True

            except Exception as e:
                retry_count += 1
                _logger.error(f"Attempt {retry_count} failed for {current_date}: {str(e)}")

                if retry_count >= max_retries:
                    # Log failed download
                    log_download_status(current_date, 'failed', 0)
                    _logger.error(f"Max retries reached for {current_date}. Moving to next date.")
                    success = True  # Move to next date even if failed

        # Move to next date
        current_date = get_next_date(current_date)

    _logger.info(f"Download process completed. Total transactions downloaded: {total_downloaded}")
    return total_downloaded

def get_download_summary(db_path=None, table_name='download_log'):
    """
    Get a summary of download history.

    Returns:
        dict: Summary statistics
    """
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bank_data.db')
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            # Get summary statistics
            cursor.execute(f'''
                SELECT 
                    COUNT(*) as total_days,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_days,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_days,
                    SUM(CASE WHEN status = 'success' THEN transaction_count ELSE 0 END) as total_transactions,
                    MIN(date) as first_date,
                    MAX(date) as last_date
                FROM {table_name}
            ''')
            result = cursor.fetchone()
            if result and result[0] > 0:
                return {
                    'total_days': result[0],
                    'successful_days': result[1],
                    'failed_days': result[2],
                    'total_transactions': result[3],
                    'first_date': result[4],
                    'last_date': result[5]
                }
            else:
                return {
                    'total_days': 0,
                    'successful_days': 0,
                    'failed_days': 0,
                    'total_transactions': 0,
                    'first_date': None,
                    'last_date': None
                }
    except Exception as e:
        _logger.error(f"Error getting download summary: {e}")
        return None

if __name__ == "__main__":
    try:
        # Show current summary
        summary = get_download_summary()
        if summary:
            print("=== Download Summary ===")
            print(f"Total days processed: {summary['total_days']}")
            print(f"Successful downloads: {summary['successful_days']}")
            print(f"Failed downloads: {summary['failed_days']}")
            print(f"Total transactions: {summary['total_transactions']}")
            print(f"Date range: {summary['first_date']} to {summary['last_date']}")
            print()

        # Download missing days
        print("=== Starting Download Process ===")
        total_downloaded = download_missing_days(start_date='2025-05-21')

        print(f"\n=== Process Complete ===")
        print(f"Total new transactions downloaded: {total_downloaded}")

        # Show updated summary
        summary = get_download_summary()
        if summary:
            print("\n=== Updated Summary ===")
            print(f"Total days processed: {summary['total_days']}")
            print(f"Successful downloads: {summary['successful_days']}")
            print(f"Failed downloads: {summary['failed_days']}")
            print(f"Total transactions: {summary['total_transactions']}")
            print(f"Date range: {summary['first_date']} to {summary['last_date']}")

    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        _logger.error(f"Main process error: {e}")
        print(f"Error: {e}")