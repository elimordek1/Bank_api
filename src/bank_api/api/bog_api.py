import pandas as pd
import base64
import requests
import logging
import sqlite3
import os

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
    return df[df['bank_name'] == 'BOG']


def fetch_transactions_for_account(client_id, client_secret, account_number, currency, start_date, end_date,
                                   company=None):
    _logger.info(f"Fetching transactions for account: {account_number}")
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
        _logger.error(f"Auth failed: {e}")
        return []

    headers = {'Authorization': f'Bearer {access_token}'}
    statement_url = f"https://api.businessonline.ge/api/statement/{account_number}/{currency}/{start_date}/{end_date}"

    try:
        response = requests.get(statement_url, headers=headers)
        response.raise_for_status()
        records = response.json().get('Records', [])
    except Exception as e:
        _logger.error(f"Failed to fetch transactions: {e}")
        return []

    # Flatten records
    flattened = []
    for r in records:
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
            "account_number": account_number
        })

    return flattened


def get_all_transactions(start_date, end_date):
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
        all_records.extend(records)
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


# Example usage
if __name__ == '__main__':
    # Remove or comment out the following line to avoid interfering with main logger
    # logging.basicConfig(level=logging.INFO)
    df = get_all_transactions('2025-04-04', '2025-05-05')
    write_transactions_to_sqlite(df)
    print(df.head())
