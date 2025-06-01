from unittest.mock import patch, MagicMock
import pandas as pd
from bank_api.api import bog_api
from unittest.mock import patch, MagicMock

import pandas as pd
from bank_api.api import bog_api


@patch('bank_api.api.bog_api.read_accounts_from_excel')
@patch('bank_api.api.bog_api.fetch_transactions_for_account')
def test_get_all_transactions(mock_fetch, mock_read_accounts):
    # Mock accounts DataFrame
    mock_read_accounts.return_value = pd.DataFrame([
        {'company': 'RGG', 'currency': 'USD', 'bank_name': 'BOG', 'account_number': '123'}
    ])
    # Mock fetch_transactions_for_account returns a list of dicts
    mock_fetch.return_value = [{
        'entry_date': '2025-01-01', 'company': 'RGG', 'currency': 'USD', 'account_number': '123'
    }]
    df = bog_api.get_all_transactions('2025-01-01', '2025-01-31')
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert set(['company', 'currency', 'account_number']).issubset(df.columns)

@patch('sqlite3.connect')
def test_write_transactions_to_sqlite(mock_connect):
    df = pd.DataFrame([
        {'entry_date': '2025-01-01', 'company': 'RGG', 'currency': 'USD', 'account_number': '123'}
    ])
    mock_conn = MagicMock()
    mock_connect.return_value.__enter__.return_value = mock_conn
    bog_api.write_transactions_to_sqlite(df)
    mock_conn.to_sql.assert_not_called()  # DataFrame's to_sql is called, not connection
    assert mock_conn.method_calls

def test_get_all_transactions_empty(monkeypatch):
    from bank_api.api import bog_api
    monkeypatch.setattr(bog_api, 'read_accounts_from_excel', lambda excel_file='data/Banks.xlsx': pd.DataFrame([]))
    df = bog_api.get_all_transactions('2025-01-01', '2025-01-31')
    assert df.empty

def test_get_all_transactions_invalid_data(monkeypatch):
    from bank_api.api import bog_api
    # Missing required columns
    monkeypatch.setattr(bog_api, 'read_accounts_from_excel', lambda excel_file='data/Banks.xlsx': pd.DataFrame([{'foo': 'bar'}]))
    # fetch_transactions_for_account returns invalid data
    monkeypatch.setattr(bog_api, 'fetch_transactions_for_account', lambda *a, **kw: [{'foo': 'bar'}])
    df = bog_api.get_all_transactions('2025-01-01', '2025-01-31')
    assert isinstance(df, pd.DataFrame)
    assert df.empty 