import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from bank_api.data import get_currency_daily

@patch('bank_api.data.get_currency_daily.get_bog_exchange_rate')
@patch('bank_api.data.get_currency_daily.get_tbc_exchange_rates')
def test_get_daily_currency_rates(mock_tbc, mock_bog):
    # Mock BOG returns one rate per currency
    mock_bog.side_effect = lambda currency: {'bank': 'BOG', 'currency': currency, 'buy': 1, 'sell': 2, 'date': '2025-01-01', 'datetime': '2025-01-01 00:00:00'}
    # Mock TBC returns a list of rates
    mock_tbc.return_value = [
        {'bank': 'TBC', 'currency': 'USD', 'buy': 3, 'sell': 4, 'date': '2025-01-01', 'datetime': '2025-01-01 00:00:00'}
    ]
    df = get_currency_daily.get_daily_currency_rates(['USD'])
    assert isinstance(df, pd.DataFrame)
    assert set(['bank', 'currency', 'buy', 'sell', 'date', 'datetime']).issubset(df.columns)
    assert (df['bank'] == 'BOG').any() and (df['bank'] == 'TBC').any()

@patch('sqlite3.connect')
def test_write_rates_to_sqlite(mock_connect):
    df = pd.DataFrame([
        {'bank': 'TBC', 'currency': 'USD', 'buy': 3, 'sell': 4, 'date': '2025-01-01', 'datetime': '2025-01-01 00:00:00'}
    ])
    mock_conn = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn
    get_currency_daily.write_rates_to_sqlite(df)
    mock_conn.to_sql.assert_not_called()  # DataFrame's to_sql is called, not connection
    # Instead, check that the connection was used
    assert mock_conn.method_calls 

def test_get_daily_currency_rates_empty(monkeypatch):
    from bank_api.data import get_currency_daily
    monkeypatch.setattr(get_currency_daily, 'get_bog_exchange_rate', lambda currency: None)
    monkeypatch.setattr(get_currency_daily, 'get_tbc_exchange_rates', lambda currencies: [])
    df = get_currency_daily.get_daily_currency_rates(['USD'])
    assert df.empty or len(df) == 0

def test_get_daily_currency_rates_malformed(monkeypatch):
    from bank_api.data import get_currency_daily
    monkeypatch.setattr(get_currency_daily, 'get_bog_exchange_rate', lambda currency: {'foo': 'bar'})
    monkeypatch.setattr(get_currency_daily, 'get_tbc_exchange_rates', lambda currencies: [{'foo': 'bar'}])
    df = get_currency_daily.get_daily_currency_rates(['USD'])
    assert isinstance(df, pd.DataFrame)
    assert df.empty 