import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

import bank_api.main as main_module

@patch('bank_api.main.get_bog_transactions')
@patch('bank_api.main.write_bog_to_sqlite')
@patch('bank_api.main.get_tbc_transactions')
@patch('bank_api.main.write_tbc_to_sqlite')
@patch('bank_api.main.get_daily_currency_rates')
@patch('bank_api.main.write_rates_to_sqlite')
def test_main_etl(
    mock_write_rates_to_sqlite,
    mock_get_daily_currency_rates,
    mock_write_tbc_to_sqlite,
    mock_get_tbc_transactions,
    mock_write_bog_to_sqlite,
    mock_get_bog_transactions
):
    # Setup mocks
    fake_bog_df = pd.DataFrame({'a': [1]})
    fake_tbc_df = pd.DataFrame({'b': [2]})
    fake_rates_df = pd.DataFrame({'c': [3]})
    mock_get_bog_transactions.return_value = fake_bog_df
    mock_get_tbc_transactions.return_value = fake_tbc_df
    mock_get_daily_currency_rates.return_value = fake_rates_df

    # Run main
    main_module.main()

    # Check calls
    mock_get_bog_transactions.assert_called_once()
    mock_write_bog_to_sqlite.assert_called_once_with(fake_bog_df)
    mock_get_tbc_transactions.assert_called_once()
    mock_write_tbc_to_sqlite.assert_called_once_with(fake_tbc_df)
    mock_get_daily_currency_rates.assert_called_once()
    mock_write_rates_to_sqlite.assert_called_once_with(fake_rates_df) 