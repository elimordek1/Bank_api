import logging
import pandas as pd
import requests
import base64
import os

_logger = logging.getLogger(__name__)


def read_accounts_from_excel(bank, excel_file=None):
    if excel_file is None:
        PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) )
        excel_file = os.path.join(PROJECT_ROOT, 'data', 'Banks.xlsx')
    try:
        df = pd.read_excel(excel_file, sheet_name=0)
    except Exception as e:
        _logger.error(f"Error reading Excel file: {e}")
        return pd.DataFrame()
    required_cols = {'ID', 'Account Number'}
    if not required_cols.issubset(df.columns):
        _logger.error(f"Excel file missing required columns: {required_cols - set(df.columns)}")
        return pd.DataFrame()
    df['company'] = df['ID'].apply(lambda x: x.split(' ')[2])
    df['currency'] = df['ID'].apply(lambda x: x.split(' ')[1])
    df['bank_name'] = df['ID'].apply(lambda x: x.split(' ')[0])
    df['account_number'] = df['Account Number'].apply(lambda x: x[:-3] if isinstance(x, str) and len(x) > 3 else x)
    df = df[['company', 'currency', 'bank_name', 'account_number']]
    df = df[df['account_number'].notna()]
    return df[df['bank_name'] == bank]

####TBC CURRENCY RATES

# Your API credentials
API_KEY = "BjvWVkrQnzWeogGaruys8J3J8KdG7Wt6"
SECRET = "EVDIkS00NPF0KPHoBL5SH5tsdFqQ2crzVzAqJX7JG0UbmiXs00GEu0ocaeE3sYYl"

def get_exchange_rates(currencies=None):
    """
    Get TBC Bank commercial exchange rates

    Args:
        currencies: List of currencies like ['USD', 'EUR'] or None for all

    Returns:
        Dictionary with exchange rates
    """
    url = "https://test-api.tbcbank.ge/v1/exchange-rates/commercial"

    headers = {
        'apikey': API_KEY
    }

    params = {}
    if currencies:
        params['currency'] = ','.join(currencies)

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        return None

def print_rates(currencies=None):
    """Print exchange rates in a nice format"""
    data = get_exchange_rates(currencies)

    if data:
        print(f"Base Currency: {data['base']}")
        print("-" * 30)
        for rate in data['commercialRatesList']:
            currency = rate['currency']
            buy = rate['buy']
            sell = rate['sell']
            print(f"{currency}: Buy {buy} | Sell {sell}")
    else:
        print("Failed to get rates")

##BOG CURRENCY RATES

# Single company credentials for exchange rate API
CLIENT_ID = '633822f9-a298-49e0-9545-97b78e4d9b04'
CLIENT_SECRET = 'fb73a373-d464-4052-b9a4-2fee0a83c9e3'

def get_access_token():
    """Get access token from BOG API"""
    auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
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
        return response.json().get('access_token')
    except Exception as e:
        _logger.error(f"Authentication failed: {e}")
        return None

def get_commercial_exchange_rate(currency):
    """
    Get commercial exchange rate for a given currency

    Args:
        currency (str): Currency code in ISO 4217 format (e.g., 'USD', 'EUR')

    Returns:
        dict: Exchange rate data with 'buy' and 'sell' rates, or None if failed
    """
    access_token = get_access_token()
    if not access_token:
        return None

    headers = {'Authorization': f'Bearer {access_token}'}
    exchange_rate_url = f"https://api.businessonline.ge/api/rates/commercial/{currency.upper()}"

    try:
        response = requests.get(exchange_rate_url, headers=headers)
        response.raise_for_status()
        data = response.json()

        return {
            'currency': currency.upper(),
            'buy': data.get('Buy'),
            'sell': data.get('Sell'),
            'timestamp': data.get('timestamp')  # if available
        }
    except Exception as e:
        _logger.error(f"Failed to fetch exchange rate for {currency}: {e}")
        return None

def get_multiple_exchange_rates(currencies):
    """
    Get exchange rates for multiple currencies

    Args:
        currencies (list): List of currency codes

    Returns:
        list: List of exchange rate data dictionaries
    """
    rates = []
    for currency in currencies:
        rate = get_commercial_exchange_rate(currency)
        if rate:
            rates.append(rate)
    return rates

# Remove or comment out the following line to avoid interfering with main logger
# logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    # Get USD and EUR rates
    print("USD and EUR rates:")
    print_rates(['USD', 'EUR'])

    print("\n" + "="*30 + "\n")

    # Get all available rates
    print("All rates:")
    print_rates()

    print("\n" + "="*30 + "\n")

    # Get just the data as dictionary
    rates_data = get_exchange_rates(['USD'])
    print("USD data as dictionary:")
    print(rates_data)

    # Get single currency rate
    usd_rate = get_commercial_exchange_rate('USD')
    if usd_rate:
        print(f"USD Exchange Rate:")
        print(f"Buy: {usd_rate['buy']}")
        print(f"Sell: {usd_rate['sell']}")

    # Get multiple currencies
    currencies = ['USD', 'EUR', 'GBP']
    rates = get_multiple_exchange_rates(currencies)

    print("\nAll Exchange Rates:")
    for rate in rates:
        print(f"{rate['currency']}: Buy={rate['buy']}, Sell={rate['sell']}")

