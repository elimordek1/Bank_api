import requests
import xml.etree.ElementTree as Et
import xmltodict
import json
import os
import pandas as pd
import logging
from datetime import datetime
import sqlite3
import csv

# Remove or comment out the following line to avoid interfering with main logger
# logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger(__name__)

# Constants
TBC_CERT_BASE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'tbc_certificates')
print("DEBUG: TBC_CERT_BASE_PATH =", TBC_CERT_BASE_PATH)

# TLS certificate checklist for each company
TLS_CERTIFICATE_STATUS = {}

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
TBC_CREDENTIALS = {
    'SRG': {'username': 'SRG_1', 'password': 'ASDasd12334!@'},
    'RGG': {'username': 'RGG_1', 'password': 'ASDasd12334!@'},
    'MRG': {'username': 'MRG_1', 'password': 'ASDasd12334!@'},
    'BRG': {'username': 'BRG_1', 'password': 'ASDasd12334!@'},
    'PRG': {'username': 'PRG_1', 'password': 'ASDasd12334!@'},
    'MSG': {'username': 'MSG_1', 'password': 'ASDasd12334!@'},
    'FRG': {'username': 'FRG_1', 'password': 'ASDasd12334!@'},
    'MHR': {'username': 'MHR_1', 'password': 'ASDasd12334!@'},
    'RGH': {'username': 'RGH_1', 'password': 'ASDasd12334!@'},
    'GAG': {'username': 'GAG_1', 'password': 'ASDasd12334!@'},
}

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
    cert_file = os.path.join(folder_path, 'server_cert.pem')
    key_file = os.path.join(folder_path, 'key_unencrypted.pem')
    # Check existence and update checklist
    TLS_CERTIFICATE_STATUS[company_abbr] = {
        'server_cert.pem': os.path.exists(cert_file),
        'key_unencrypted.pem': os.path.exists(key_file)
    }
    return (
        cert_file,
        key_file,
    )

def read_accounts_from_excel(excel_file=None):
    if excel_file is None:
        PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) )
        excel_file = os.path.join(PROJECT_ROOT, 'data', 'Banks.xlsx')
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
    required_cols = {'company', 'currency', 'account_number'}
    if accounts_df.empty or not required_cols.issubset(accounts_df.columns):
        _logger.warning("Accounts DataFrame is empty or missing required columns.")
        return pd.DataFrame()
    all_transactions = []
    for _, row in accounts_df.iterrows():
        company = row['company']
        currency = row['currency']
        account_number = row['account_number']
        if company not in TBC_CREDENTIALS:
            _logger.warning(f"No credentials for company: {company}. Skipping.")
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
    if all_transactions:
        df = pd.DataFrame(all_transactions)
        return df
    else:
        _logger.warning("No transactions found for the specified period")
        return pd.DataFrame()

def write_transactions_to_sqlite(df, db_path='bank_data.db', table_name='tbc_transactions'):
    """
    Write a DataFrame of TBC transactions to a SQLite3 database.
    """
    if df.empty:
        _logger.warning("No transactions to write to database.")
        return
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists='append', index=False)
        _logger.info(f"Written {len(df)} transactions to {table_name} in {db_path}")

def print_tls_certificate_checklist():
    print("\nTLS Certificate Checklist:")
    for company, status in TLS_CERTIFICATE_STATUS.items():
        print(f"{company}:")
        for cert, exists in status.items():
            print(f"  {cert}: {'FOUND' if exists else 'MISSING'}")

def export_tls_certificate_checklist_csv(path='tls_certificate_checklist.csv'):
    with open(path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Company', 'server_cert.pem', 'key_unencrypted.pem'])
        for company, status in TLS_CERTIFICATE_STATUS.items():
            writer.writerow([
                company,
                'FOUND' if status.get('server_cert.pem') else 'MISSING',
                'FOUND' if status.get('key_unencrypted.pem') else 'MISSING'
            ])

def export_tls_certificate_checklist_json(path='tls_certificate_checklist.json'):
    with open(path, 'w') as jsonfile:
        json.dump(TLS_CERTIFICATE_STATUS, jsonfile, indent=2)

def log_tls_certificate_checklist(logger):
    logger.info('TLS Certificate Checklist:')
    for company, status in TLS_CERTIFICATE_STATUS.items():
        logger.info(f"{company}: server_cert.pem={'FOUND' if status.get('server_cert.pem') else 'MISSING'}, key_unencrypted.pem={'FOUND' if status.get('key_unencrypted.pem') else 'MISSING'}")

def create_missing_cert_folders():
    for company_name in CERTIFICATE_COMPANIES.values():
        folder_path = os.path.join(TBC_CERT_BASE_PATH, company_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

# Example usage
if __name__ == "__main__":
    start_date = "2025-05-01"
    end_date = "2025-05-10"

    df = get_all_transactions(start_date, end_date)
    write_transactions_to_sqlite(df)

    if not df.empty:
        df.to_excel('tbc_transactions.xlsx', index=False)
        print(f"Saved {len(df)} transactions to tbc_transactions.xlsx")
        print(f"Sample data: {df.head(2).to_dict('records')}")
    else:
        print("No transactions found")