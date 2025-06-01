import base64
import logging
import os
import sqlite3
import datetime
import pandas as pd
import requests

_logger = logging.getLogger(__name__)

# Your company credentials
COMPANY_CREDENTIALS_BOG = {
    'RGG': {'client_id': '633822f9-a298-49e0-9545-97b78e4d9b04',
            'client_secret': 'fb73a373-d464-4052-b9a4-2fee0a83c9e3'},
    'MRG': {'client_id': 'e86520fd-3861-40fb-841e-3a90cc9ae35f',
            'client_secret': '67c7d183-e133-4741-8496-5745b94f2de4'},
    'SRG': {'client_id': '8f53ad44-9cfc-4f3d-a28c-ca0ba4867b88',
            'client_secret': 'ef024a17-44a0-4103-b3c6-5787a291da0b'},
    'PRG': {'client_id': 'd6749fac-3fe7-4840-99fa-dd7a55eb311f',
            'client_secret': 'ebc950f3-d613-40dd-b93a-6790300bfabd'},
    'MHR': {'client_id': 'fc232347-1f78-40a2-8891-796c9108e8ee',
            'client_secret': 'fd121084-7f54-4af2-941b-a53205f88af5'},
    'RGH': {'client_id': 'e68cb474-6574-4f9c-9910-27284092627d',
            'client_secret': '769009c4-886a-4793-9b9d-a1945ea38e81'},
    'MSG': {'client_id': '36de8978-c8e4-48c2-aa9a-023071c42a40',
            'client_secret': '76979b9e-0f86-48da-9d8e-bfbf4d7ffacd'},
    'FRG': {'client_id': '7910185a-bef6-4fab-80e2-529953814eef',
            'client_secret': '1b3da861-8a75-42fa-810a-769ac93a6463'},
    'BRG': {'client_id': 'c27815b4-0c91-4d13-a9ed-d4f403adf1b7',
            'client_secret': '1d666108-2fcd-4a56-b30d-542fede107b5'},
}


def read_accounts_from_excel(excel_file=None):
    if excel_file is None:
        PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__name__)))))
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
        response = requests.post(auth_url, headers=headers, data=data)
        response.raise_for_status()
        access_token = response.json().get('access_token')
    except Exception as e:
        _logger.error(f"Auth failed for account {account_number}: {e}")
        return []

    headers = {'Authorization': f'Bearer {access_token}'}
    statement_url = f"https://api.businessonline.ge/api/statement/{account_number}/{currency}/{start_date}/{end_date}"

    try:
        response = requests.get(statement_url, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        records = response_json.get('Records', [])
        statement_id = response_json.get('Id')
        _logger.info(f"Retrieved statement ID: {statement_id} for account {account_number}")

    except Exception as e:
        _logger.error(f"Failed to fetch transactions for account {account_number}: {e}")
        return []

    daily_summaries = {}
    if statement_id:
        summary_url = f"https://api.businessonline.ge/api/statement/summary/{account_number}/{currency}/{statement_id}"
        try:
            summary_response = requests.get(summary_url, headers=headers)
            summary_response.raise_for_status()
            summary_json = summary_response.json()
            for summary in summary_json.get('DailySummaries', []):
                # The date in the summary is like '2025-05-27T00:00:00', we need 'YYYY-MM-DD'
                summary_date = summary.get('Date', '').split('T')[0]
                daily_summaries[summary_date] = summary
            _logger.info(f"Retrieved {len(daily_summaries)} daily summaries for account {account_number}")
        except Exception as e:
            _logger.error(f"Failed to fetch daily summaries for account {account_number}: {e}")

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
            _logger.warning(f"No daily summary found for date {entry_date_str} for account {account_number}. Opening/Closing balances will be null.")


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
    initialize_db() # Ensure the database and success_log table exist
    accounts_df = read_accounts_from_excel()
    # Check for empty or malformed DataFrame
    required_cols = {'company', 'currency', 'account_number'}
    if accounts_df.empty or not required_cols.issubset(accounts_df.columns):
        _logger.warning("Accounts DataFrame is empty or missing required columns.")
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
            start_date = (datetime.datetime.strptime(last_run_date_str, '%Y-%m-%d').date() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            # If no last run date, fetch for a reasonable historical period, e.g., 30 days back from end_date
            start_date = (datetime.datetime.strptime(end_date, '%Y-%m-%d').date() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
            _logger.info(f"No previous run date for {company}-{account_number}-{currency}. Fetching from {start_date}.")

        # Ensure start_date does not exceed end_date
        if datetime.datetime.strptime(start_date, '%Y-%m-%d').date() > datetime.datetime.strptime(end_date, '%Y-%m-%d').date():
            _logger.info(f"Start date {start_date} is after end date {end_date} for {company}-{account_number}-{currency}. Skipping.")
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
        columns = list(all_records[0].keys())
        for record in all_records:
            for column in columns:
                if column not in record:
                    record[column] = None
        return pd.DataFrame(all_records)
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


def fetch_nbg_currency_df(date=None):
    if not date:
        date = datetime.date.today()
    date_str = date.strftime('%Y-%m-%d')
    url = f'https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json/?date={date_str}'

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data or not isinstance(data, list) or 'currencies' not in data[0]:
            print(f"No valid data for {date_str}")
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

        df = pd.DataFrame(records)
        return df

    except Exception as e:
        print(f"Failed to fetch rates: {e}")
        return pd.DataFrame()


# Example usage
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO) # Keep this for running the example

    # Initialize the database (creates success_log table if not exists)
    initialize_db()

    # Define the end date for fetching transactions
    # This will fetch up to and including '2025-05-29'
    today = datetime.date.today()
    end_date_for_fetch = today.strftime('%Y-%m-%d')
    # Or, for a specific date: end_date_for_fetch = '2025-05-29'


    df = get_all_transactions(end_date_for_fetch)
    write_transactions_to_sqlite(df)
    print("\n--- Fetched Transactions Sample (first 5 rows) ---")
    print(df.head())
    print(f"\nTotal transactions fetched: {len(df)}")

    # Example of filtering and adding NBG rates (as in your original example)
    srg_df = df[df['company'] == 'FRG']
    srg_df = srg_df[srg_df['currency'] == 'EUR']
    if not srg_df.empty:
        # Get the date from the first entry to fetch NBG rate for that specific day
        nbg_date_str = srg_df['entry_date'].iloc[0].split('T')[0]
        df_nbg = fetch_nbg_currency_df(datetime.datetime.strptime(nbg_date_str, '%Y-%m-%d').date())

        if not df_nbg.empty:
            srg_df = srg_df.merge(df_nbg[['currency', 'rate_per_unit']], on='currency', how='left')
            # Calculate rate_per_unit_live, handling division by zero and NaNs
            srg_df['rate_per_unit_live'] = (srg_df['entry_amount_debit_base'].fillna(0) + srg_df['entry_amount_credit_base'].fillna(0)) / \
                                           (srg_df['entry_amount_debit'].fillna(0) + srg_df['entry_amount_credit'].fillna(0))
            # Replace inf values with NaN which pandas can handle better
            srg_df['rate_per_unit_live'] = srg_df['rate_per_unit_live'].replace([float('inf'), -float('inf')], pd.NA)

            print("\n--- FRG EUR Transactions with NBG Rates Sample (first 5 rows) ---")
            print(srg_df.head())
        else:
            print("\n--- NBG currency data not available for merging ---")
    else:
        print("\n--- No FRG EUR transactions found to process ---")