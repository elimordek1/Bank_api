import pandas as pd
import base64
import requests
import logging
import os
import json # Import json for pretty printing the response
import datetime # Added for the new read_accounts_from_excel function
import sys # Added for the new read_accounts_from_excel function
import sqlite3 # Added for the new read_accounts_from_excel function

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
    _logger.info(f"Successfully loaded company credentials from {SECRETS_PATH}")
except FileNotFoundError:
    _logger.error(f"File not found: {SECRETS_PATH}. Please ensure it exists.")
    # Set to an empty dict to avoid NameError later, but the program will likely fail
    # if credentials are truly needed.
    COMPANY_CREDENTIALS_BOG = {}
    # Re-raising the error to stop execution if credentials are critical
    raise
except json.JSONDecodeError as e:
    _logger.error(f"Error decoding JSON from {SECRETS_PATH}: {e}")
    COMPANY_CREDENTIALS_BOG = {}
    raise


def read_accounts_from_excel(excel_file=None):
    """
    Reads bank account information from an Excel file, with updated path handling
    and robust error checking.
    """
    if excel_file is None:
        try:
            PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        except NameError:
            PROJECT_ROOT = os.getcwd()  # Fallback to current working directory
        excel_file = os.path.join(PROJECT_ROOT, 'configs', 'banks.xlsx')
        _logger.info(f"Using default Excel file path: {excel_file}")

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


def fetch_transactions_for_account(client_id, client_secret, account_number, currency, start_date, end_date,
                                   company=None):
    """
    Fetches transactions for a given account.
    This function is provided by the user and is included for context.
    """
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
    except requests.exceptions.RequestException as e:
        _logger.error(f"Authentication failed for transactions: {e}")
        return []

    headers = {'Authorization': f'Bearer {access_token}'}
    statement_url = f"https://api.businessonline.ge/api/statement/{account_number}/{currency}/{start_date}/{end_date}"
    # In a real scenario, you would make the actual request here
    # try:
    #     response = requests.get(statement_url, headers=headers)
    #     response.raise_for_status()
    #     return response.json()
    # except requests.exceptions.RequestException as e:
    #     _logger.error(f"Failed to fetch transactions: {e}")
    #     return []
    _logger.info(f"Simulating transaction fetch for {account_number}. URL: {statement_url}")
    return [{"transactionId": "123", "amount": 100.0, "description": "Dummy Transaction"}]


def fetch_account_balance(client_id: str, client_secret: str, account_number: str, currency: str) -> dict:
    """
    Retrieves the current and available balance of a bank account from the BOG API.

    Args:
        client_id (str): The client ID for authentication.
        client_secret (str): The client secret for authentication.
        account_number (str): The bank account number.
        currency (str): The currency code in ISO 4217 format (e.g., 'USD', 'GEL').

    Returns:
        dict: A dictionary containing 'AvailableBalance' and 'CurrentBalance' if successful,
              otherwise an empty dictionary.
    """
    _logger.info(f"Attempting to fetch balance for account: {account_number} in {currency}")

    # 1. Authenticate to get the access token
    auth_string = f"{client_id}:{client_secret}"
    auth_header = base64.b64encode(auth_string.encode()).decode()

    auth_url = 'https://account.bog.ge/auth/realms/bog/protocol/openid-connect/token'
    data = {'grant_type': 'client_credentials'}
    headers = {
        'Authorization': f'Basic {auth_header}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    access_token = None
    try:
        _logger.debug("Requesting access token...")
        response = requests.post(auth_url, headers=headers, data=data)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        access_token = response.json().get('access_token')
        if not access_token:
            _logger.error("Access token not found in authentication response.")
            return {}
        _logger.debug("Access token obtained successfully.")
    except requests.exceptions.RequestException as e:
        _logger.error(f"Authentication failed for balance inquiry: {e}")
        return {}

    # 2. Construct the balance inquiry URL
    base_api_url = 'https://api.businessonline.ge'
    balance_url = f"{base_api_url}/api/accounts/{account_number}/{currency}"
    _logger.info(f"Constructed balance URL: {balance_url}")

    # 3. Make the GET request for the balance
    headers = {'Authorization': f'Bearer {access_token}'}
    try:
        _logger.debug(f"Making GET request to {balance_url} with bearer token.")
        response = requests.get(balance_url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        balance_data = response.json()
        _logger.info("Balance data received successfully.")
        return balance_data
    except requests.exceptions.RequestException as e:
        _logger.error(f"Failed to fetch account balance: {e}")
        return {}
    except json.JSONDecodeError as e:
        _logger.error(f"Failed to decode JSON response for balance: {e}")
        return {}

def update_balances_to_excel(output_excel_file: str = None):
    """
    Fetches account balances for all BOG accounts found in the Excel config
    and writes them to a new Excel file.

    Args:
        output_excel_file (str, optional): The path to the output Excel file.
                                           If None, a default path will be used.
    """
    _logger.info("Starting process to update account balances to Excel.")

    # Determine default output file path if not provided
    if output_excel_file is None:
        try:
            PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        except NameError:
            PROJECT_ROOT = os.getcwd()
        output_excel_file = os.path.join(PROJECT_ROOT, 'reports', 'bog_account_balances1.xlsx')
        _logger.info(f"No output Excel file specified, using default: {output_excel_file}")

    # Ensure the directory for the output file exists
    output_dir = os.path.dirname(output_excel_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        _logger.info(f"Created output directory: {output_dir}")

    bog_accounts = read_accounts_from_excel()

    if bog_accounts.empty:
        _logger.warning("No BOG accounts found or Excel file could not be processed. No balances to fetch.")
        print("No BOG accounts found to fetch balances for.")
        return

    _logger.info(f"Found {len(bog_accounts)} BOG accounts to process.")
    results = []

    for index, row in bog_accounts.iterrows():
        company_id = row['company']
        account_num = row['account_number']
        curr = row['currency']

        credentials = COMPANY_CREDENTIALS_BOG.get(company_id)
        if credentials:
            client_id = credentials['client_id']
            client_secret = credentials['client_secret']

            _logger.info(f"Fetching balance for Company: {company_id}, Account: {account_num}, Currency: {curr}")
            balance_info = fetch_account_balance(client_id, client_secret, account_num, curr)

            if balance_info:
                results.append({
                    'Company': company_id,
                    'Account Number': account_num,
                    'Currency': curr,
                    'AvailableBalance': balance_info.get('AvailableBalance'),
                    'CurrentBalance': balance_info.get('CurrentBalance'),
                    'Timestamp': datetime.datetime.now().isoformat()
                })
                _logger.info(f"Successfully fetched balance for {account_num} ({curr}).")
            else:
                results.append({
                    'Company': company_id,
                    'Account Number': account_num,
                    'Currency': curr,
                    'AvailableBalance': 'Error',
                    'CurrentBalance': 'Error',
                    'Timestamp': datetime.datetime.now().isoformat()
                })
                _logger.error(f"Failed to fetch balance for {account_num} ({curr}).")
        else:
            _logger.warning(f"Credentials not found for company ID: {company_id}. Skipping balance fetch.")
            results.append({
                'Company': company_id,
                'Account Number': account_num,
                'Currency': curr,
                'AvailableBalance': 'No Credentials',
                'CurrentBalance': 'No Credentials',
                'Timestamp': datetime.datetime.now().isoformat()
            })

    if results:
        df_balances = pd.DataFrame(results)
        try:
            df_balances.to_excel(output_excel_file, index=False, sheet_name='AccountBalances')
            _logger.info(f"Successfully wrote account balances to {output_excel_file}")
            print(f"Account balances successfully written to: {output_excel_file}")
        except Exception as e:
            _logger.error(f"Error writing balances to Excel file {output_excel_file}: {e}")
            print(f"Error writing balances to Excel: {e}")
    else:
        _logger.info("No balance data was collected to write to Excel.")
        print("No balance data was collected.")

# --- Example Usage ---
if __name__ == "__main__":
    # Configure logging for better visibility
    # This basicConfig might be overridden if run in an environment that already
    # configured logging. For standalone script, it's fine.
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # When run directly, call the new update_balances_to_excel function
    update_balances_to_excel()

    # The previous dummy example is now handled by update_balances_to_excel
    # if bog_accounts.empty and COMPANY_CREDENTIALS_BOG:
    #     _logger.info("\n--- Demonstrating with dummy credentials and account if no accounts are found ---")
    #     dummy_client_id = list(COMPANY_CREDENTIALS_BOG.values())[0]['client_id'] if COMPANY_CREDENTIALS_BOG else "dummy_client_id"
    #     dummy_client_secret = list(COMPANY_CREDENTIALS_BOG.values())[0]['client_secret'] if COMPANY_CREDENTIALS_BOG else "dummy_client_secret"
    #     dummy_account_number = "GE12345678901234567890"
    #     dummy_currency = "USD"
    #     _logger.info(f"Attempting to fetch balance for dummy account: {dummy_account_number} in {dummy_currency}")
    #     balance_info_dummy = fetch_account_balance(dummy_client_id, dummy_client_secret, dummy_account_number, dummy_currency)
    #     if balance_info_dummy:
    #         print(f"Balance for {dummy_account_number} ({dummy_currency}):")
    #         print(json.dumps(balance_info_dummy, indent=2))
    #     else:
    #         print(f"Could not retrieve balance for dummy account. (Expected to fail if credentials are not valid).")
