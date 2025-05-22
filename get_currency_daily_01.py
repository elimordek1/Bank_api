import pandas as pd
import base64
import requests
import logging
from datetime import datetime
import os

_logger = logging.getLogger(__name__)

# BOG credentials - using one company
BOG_CLIENT_ID = '633822f9-a298-49e0-9545-97b78e4d9b04'
BOG_CLIENT_SECRET = 'fb73a373-d464-4052-b9a4-2fee0a83c9e3'

# TBC API credentials
TBC_API_KEY = "BjvWVkrQnzWeogGaruys8J3J8KdG7Wt6"
TBC_SECRET = "EVDIkS00NPF0KPHoBL5SH5tsdFqQ2crzVzAqJX7JG0UbmiXs00GEu0ocaeE3sYYl"

def get_bog_access_token():
    """Get access token from BOG API"""
    auth_string = f"{BOG_CLIENT_ID}:{BOG_CLIENT_SECRET}"
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
        _logger.error(f"BOG Authentication failed: {e}")
        return None

def get_bog_exchange_rate(currency):
    """Get BOG commercial exchange rate for a currency"""
    access_token = get_bog_access_token()
    if not access_token:
        return None

    headers = {'Authorization': f'Bearer {access_token}'}
    exchange_rate_url = f"https://api.businessonline.ge/api/rates/commercial/{currency.upper()}"

    try:
        response = requests.get(exchange_rate_url, headers=headers)
        response.raise_for_status()
        data = response.json()

        return {
            'bank': 'BOG',
            'currency': currency.upper(),
            'buy': data.get('Buy'),
            'sell': data.get('Sell'),
            'date': datetime.now().strftime('%Y-%m-%d'),
            'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    except Exception as e:
        _logger.error(f"Failed to fetch BOG rate for {currency}: {e}")
        return None

def get_tbc_exchange_rates(currencies=None):
    """
    Get TBC commercial exchange rates using their API

    Args:
        currencies (list): List of currency codes or None for all

    Returns:
        list: List of rate dictionaries
    """
    try:
        url = "https://test-api.tbcbank.ge/v1/exchange-rates/commercial"
        headers = {'apikey': TBC_API_KEY}

        params = {}
        if currencies:
            params['currency'] = ','.join(currencies)

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        rates = []
        current_datetime = datetime.now()

        for rate in data.get('commercialRatesList', []):
            rates.append({
                'bank': 'TBC',
                'currency': rate.get('currency'),
                'buy': rate.get('buy'),
                'sell': rate.get('sell'),
                'date': current_datetime.strftime('%Y-%m-%d'),
                'datetime': current_datetime.strftime('%Y-%m-%d %H:%M:%S')
            })

        return rates
    except Exception as e:
        _logger.error(f"Failed to fetch TBC rates: {e}")
        return []

def get_daily_currency_rates(currencies=['USD', 'EUR', 'GBP', 'TRY']):
    """
    Get daily currency rates from both BOG and TBC

    Args:
        currencies (list): List of currency codes to fetch

    Returns:
        pandas.DataFrame: Combined rates from both banks
    """
    all_rates = []

    # Get BOG rates
    _logger.info("Fetching BOG exchange rates...")
    for currency in currencies:
        bog_rate = get_bog_exchange_rate(currency)
        if bog_rate:
            all_rates.append(bog_rate)

    # Get TBC rates
    _logger.info("Fetching TBC exchange rates...")
    tbc_rates = get_tbc_exchange_rates(currencies)  # Pass currencies parameter
    all_rates.extend(tbc_rates)

    if all_rates:
        df = pd.DataFrame(all_rates)
        # Reorder columns for better readability
        df = df[['date', 'datetime', 'bank', 'currency', 'buy', 'sell']]
        return df
    else:
        return pd.DataFrame()

def save_daily_rates_to_excel(currencies=['USD', 'EUR', 'GBP', 'TRY'], filename=None):
    """
    Get daily rates and save to Excel

    Args:
        currencies (list): List of currency codes
        filename (str): Excel filename (optional)

    Returns:
        pandas.DataFrame: The saved DataFrame
    """
    df = get_daily_currency_rates(currencies)

    if not df.empty:
        if filename is None:
            date_str = datetime.now().strftime('%Y%m%d')
            filename = f'daily_exchange_rates_{date_str}.xlsx'

        df.to_excel(filename, index=False)
        _logger.info(f"Daily exchange rates saved to {filename}")
        print(f"Daily exchange rates saved to {filename}")

        # Print summary
        print(f"\nRates fetched for {len(df)} bank-currency combinations:")
        summary = df.groupby(['bank', 'currency']).size().reset_index(name='count')
        print(summary)

    return df

def append_to_historical_rates(new_rates_df, historical_file='historical_exchange_rates.xlsx'):
    """
    Append new rates to historical rates file

    Args:
        new_rates_df (DataFrame): New rates to append
        historical_file (str): Historical rates file path
    """
    if new_rates_df.empty:
        return

    try:
        # Try to read existing historical data
        if os.path.exists(historical_file):
            historical_df = pd.read_excel(historical_file)

            # Check for duplicates (same date, bank, currency)
            merge_cols = ['date', 'bank', 'currency']
            existing_combinations = set(
                historical_df[merge_cols].apply(tuple, axis=1)
            )
            new_combinations = set(
                new_rates_df[merge_cols].apply(tuple, axis=1)
            )

            # Only add truly new rates
            mask = ~new_rates_df[merge_cols].apply(tuple, axis=1).isin(existing_combinations)
            unique_new_rates = new_rates_df[mask]

            if not unique_new_rates.empty:
                combined_df = pd.concat([historical_df, unique_new_rates], ignore_index=True)
                combined_df = combined_df.sort_values(['date', 'bank', 'currency'])
                combined_df.to_excel(historical_file, index=False)
                _logger.info(f"Added {len(unique_new_rates)} new rates to {historical_file}")
            else:
                _logger.info("No new rates to add - all rates already exist")
        else:
            # Create new historical file
            new_rates_df.to_excel(historical_file, index=False)
            _logger.info(f"Created new historical file: {historical_file}")

    except Exception as e:
        _logger.error(f"Error updating historical rates: {e}")

def daily_rates_job(currencies=['USD', 'EUR', 'GBP', 'TRY']):
    """
    Main function to run daily - gets rates and updates historical data

    Args:
        currencies (list): Currencies to fetch
    """
    _logger.info("Starting daily currency rates job...")

    # Get today's rates
    today_rates = get_daily_currency_rates(currencies)

    if not today_rates.empty:
        # Save today's rates
        date_str = datetime.now().strftime('%Y%m%d')
        daily_filename = f'daily_rates_{date_str}.xlsx'
        today_rates.to_excel(daily_filename, index=False)

        # Update historical rates
        append_to_historical_rates(today_rates)

        _logger.info(f"Daily rates job completed. Fetched {len(today_rates)} rates.")
        return today_rates
    else:
        _logger.warning("No rates fetched today")
        return pd.DataFrame()

# Example usage and testing
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    # Test individual functions
    print("Testing BOG USD rate:")
    usd_bog = get_bog_exchange_rate('USD')
    if usd_bog:
        print(f"BOG USD: Buy={usd_bog['buy']}, Sell={usd_bog['sell']}")

    print("\nTesting TBC rates:")
    tbc_rates = get_tbc_exchange_rates(['USD', 'EUR'])
    if tbc_rates:
        print(f"TBC fetched {len(tbc_rates)} currency rates")
        for rate in tbc_rates[:2]:  # Show first 2
            print(f"TBC {rate['currency']}: Buy={rate['buy']}, Sell={rate['sell']}")

    # Run daily job
    print("\n" + "="*50)
    print("Running daily rates job...")
    currencies = ['USD', 'EUR', 'GBP', 'TRY']
    df = daily_rates_job(currencies)

    if not df.empty:
        print("\nToday's rates summary:")
        print(df[['bank', 'currency', 'buy', 'sell']])