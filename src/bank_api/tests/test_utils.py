import pytest
import pandas as pd
from bank_api.utils import utils
import os

# Test read_accounts_from_excel with a known bank
# This test assumes Banks.xlsx exists in the data directory and has at least one BOG or TBC account

def test_read_accounts_from_excel_bog():
    df = utils.read_accounts_from_excel('BOG', excel_file='data/Banks.xlsx')
    assert isinstance(df, pd.DataFrame)
    # If the file is not empty, check columns
    if not df.empty:
        assert set(['company', 'currency', 'bank_name', 'account_number']).issubset(df.columns)


def test_read_accounts_from_excel_tbc():
    df = utils.read_accounts_from_excel('TBC', excel_file='data/Banks.xlsx')
    assert isinstance(df, pd.DataFrame)
    if not df.empty:
        assert set(['company', 'currency', 'bank_name', 'account_number']).issubset(df.columns)

def test_read_accounts_from_excel_missing_file(tmp_path, caplog):
    missing_file = tmp_path / "nonexistent.xlsx"
    with caplog.at_level('ERROR'):
        df = utils.read_accounts_from_excel('BOG', excel_file=str(missing_file))
    assert df.empty
    assert any("Error reading Excel file" in message for message in caplog.text.splitlines())

def test_read_accounts_from_excel_invalid_columns(monkeypatch):
    from bank_api.utils import utils
    # Patch pd.read_excel to return DataFrame with wrong columns
    monkeypatch.setattr(utils.pd, 'read_excel', lambda *a, **kw: pd.DataFrame([{'foo': 'bar'}]))
    df = utils.read_accounts_from_excel('BOG', excel_file='data/Banks.xlsx')
    # Should return DataFrame, but missing required columns
    assert isinstance(df, pd.DataFrame)
    assert 'company' not in df.columns or df.empty 