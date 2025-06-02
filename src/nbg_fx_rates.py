import os
import sqlite3
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger(__name__)

# Database configuration
try:
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
except NameError:
    PROJECT_ROOT = os.getcwd()

DB_PATH = os.path.join(PROJECT_ROOT, 'bank_data.db')
TABLE_NAME = 'nbg_fx_rates'


def initialize_nbg_rates_table(db_path: str = DB_PATH) -> None:
    """
    Create the NBG FX rates table if it doesn't exist.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                date TEXT NOT NULL,
                currency TEXT NOT NULL,
                rate REAL NOT NULL,
                quantity INTEGER NOT NULL,
                rate_per_unit REAL NOT NULL,
                PRIMARY KEY (date, currency)
            )
        ''')
        conn.commit()
    _logger.info(f"Initialized {TABLE_NAME} table")


def fetch_nbg_rates_from_api(target_date: date) -> pd.DataFrame:
    """
    Fetch exchange rates from NBG API for a specific date.
    
    Args:
        target_date: The date for which to fetch rates
        
    Returns:
        DataFrame with columns: date, currency, rate, quantity, rate_per_unit
    """
    date_str = target_date.strftime('%Y-%m-%d')
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
        
        for curr in currencies:
            code = curr.get('code')
            rate = float(curr.get('rate', 0))
            quantity = float(curr.get('quantity', 1))
            rate_per_unit = rate / quantity if quantity else 0
            
            records.append({
                'date': date_str,
                'currency': code,
                'rate': rate,
                'quantity': quantity,
                'rate_per_unit': rate_per_unit
            })
        
        # Always add GEL with rate 1.0
        records.append({
            'date': date_str,
            'currency': 'GEL',
            'rate': 1.0,
            'quantity': 1,
            'rate_per_unit': 1.0
        })
        
        df = pd.DataFrame(records)
        _logger.info(f"Successfully fetched {len(df)} currency rates for {date_str}")
        return df
        
    except requests.exceptions.RequestException as e:
        _logger.error(f"Failed to fetch NBG rates for {date_str}: {e}")
        return pd.DataFrame()
    except Exception as e:
        _logger.error(f"Unexpected error fetching NBG rates for {date_str}: {e}")
        return pd.DataFrame()


def store_rates_to_db(df: pd.DataFrame, db_path: str = DB_PATH) -> None:
    """
    Store exchange rates to the database.
    """
    if df.empty:
        _logger.warning("No rates to store")
        return
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Use replace to update existing records
            df.to_sql(TABLE_NAME, conn, if_exists='append', index=False, method='multi')
            _logger.info(f"Stored {len(df)} rates to database")
    except sqlite3.IntegrityError:
        # If we get integrity errors, update existing records
        with sqlite3.connect(db_path) as conn:
            for _, row in df.iterrows():
                conn.execute(f'''
                    INSERT OR REPLACE INTO {TABLE_NAME} 
                    (date, currency, rate, quantity, rate_per_unit) 
                    VALUES (?, ?, ?, ?, ?)
                ''', (row['date'], row['currency'], row['rate'], row['quantity'], row['rate_per_unit']))
            conn.commit()
            _logger.info(f"Updated {len(df)} rates in database")
    except Exception as e:
        _logger.error(f"Failed to store rates to database: {e}")


def get_nbg_rate(currency: str, target_date: date, db_path: str = DB_PATH) -> Optional[float]:
    """
    Get the NBG exchange rate for a specific currency and date.
    
    Args:
        currency: Currency code (e.g., 'USD', 'EUR')
        target_date: The date for which to get the rate
        db_path: Path to the database
        
    Returns:
        Exchange rate per unit, or None if not found
    """
    if currency == 'GEL':
        return 1.0
    
    date_str = target_date.strftime('%Y-%m-%d') if isinstance(target_date, date) else str(target_date)[:10]
    
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f'''
                SELECT rate_per_unit FROM {TABLE_NAME}
                WHERE date = ? AND currency = ?
            ''', (date_str, currency))
            result = cursor.fetchone()
            
            if result:
                return float(result[0])
            else:
                _logger.warning(f"No rate found for {currency} on {date_str}")
                return None
                
    except Exception as e:
        _logger.error(f"Error fetching rate for {currency} on {date_str}: {e}")
        return None


def get_latest_date_in_db(db_path: str = DB_PATH) -> Optional[date]:
    """
    Get the most recent date for which we have rates in the database.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT MAX(date) FROM {TABLE_NAME}")
            result = cursor.fetchone()
            
            if result and result[0]:
                return datetime.strptime(result[0], '%Y-%m-%d').date()
            return None
            
    except Exception as e:
        _logger.warning(f"Could not determine latest date: {e}")
        return None


def fetch_and_store_missing_rates(start_date: Optional[date] = None, 
                                 end_date: Optional[date] = None,
                                 db_path: str = DB_PATH) -> None:
    """
    Fetch and store all missing rates between start_date and end_date.
    
    Args:
        start_date: Start date (defaults to last date in DB + 1 day)
        end_date: End date (defaults to today)
        db_path: Path to the database
    """
    initialize_nbg_rates_table(db_path)
    
    # Determine date range
    if end_date is None:
        end_date = date.today()
    
    if start_date is None:
        latest_db_date = get_latest_date_in_db(db_path)
        if latest_db_date:
            start_date = latest_db_date + timedelta(days=1)
        else:
            # Default to 30 days ago if no data exists
            start_date = end_date - timedelta(days=30)
    
    # Fetch rates for each missing day
    current_date = start_date
    while current_date <= end_date:
        _logger.info(f"Fetching rates for {current_date}")
        df = fetch_nbg_rates_from_api(current_date)
        if not df.empty:
            store_rates_to_db(df, db_path)
        current_date += timedelta(days=1)


def get_all_rates_for_date(target_date: date, db_path: str = DB_PATH) -> Dict[str, float]:
    """
    Get all currency rates for a specific date as a dictionary.
    
    Args:
        target_date: The date for which to get rates
        db_path: Path to the database
        
    Returns:
        Dictionary mapping currency codes to rates
    """
    date_str = target_date.strftime('%Y-%m-%d') if isinstance(target_date, date) else str(target_date)[:10]
    
    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(
                f"SELECT currency, rate_per_unit FROM {TABLE_NAME} WHERE date = ?",
                conn,
                params=(date_str,)
            )
            
            if df.empty:
                _logger.warning(f"No rates found for {date_str}")
                return {}
            
            # Always include GEL
            rates_dict = df.set_index('currency')['rate_per_unit'].to_dict()
            rates_dict['GEL'] = 1.0
            return rates_dict
            
    except Exception as e:
        _logger.error(f"Error fetching rates for {date_str}: {e}")
        return {}


def ensure_rates_for_date(target_date: date, db_path: str = DB_PATH) -> bool:
    """
    Ensure rates exist for a specific date, fetching them if necessary.
    
    Args:
        target_date: The date for which to ensure rates exist
        db_path: Path to the database
        
    Returns:
        True if rates are available (either existed or were fetched), False otherwise
    """
    # Check if rates already exist
    existing_rates = get_all_rates_for_date(target_date, db_path)
    if existing_rates:
        return True
    
    # Try to fetch rates
    _logger.info(f"Rates not found for {target_date}, fetching from NBG API")
    df = fetch_nbg_rates_from_api(target_date)
    
    if not df.empty:
        store_rates_to_db(df, db_path)
        return True
    
    return False


# Example usage and testing
if __name__ == '__main__':
    # Initialize the table
    initialize_nbg_rates_table()
    
    # Fetch missing rates for the last 7 days
    end_date = date.today()
    start_date = end_date - timedelta(days=7)
    
    print(f"Fetching rates from {start_date} to {end_date}")
    fetch_and_store_missing_rates(start_date, end_date)
    
    # Test getting a specific rate
    test_date = date.today()
    test_currency = 'USD'
    rate = get_nbg_rate(test_currency, test_date)
    print(f"\n{test_currency} rate on {test_date}: {rate}")
    
    # Get all rates for today
    all_rates = get_all_rates_for_date(test_date)
    print(f"\nAll rates for {test_date}:")
    for curr, rate in sorted(all_rates.items()):
        print(f"  {curr}: {rate}") 