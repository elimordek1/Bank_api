import os
import pandas as pd
import sqlite3
import logging
from datetime import datetime
import re # Import regex module

# Import our NBG rates module
# This assumes nbg_fx_rates.py is in a sibling directory or accessible via PYTHONPATH.
from nbg_fx_rates import get_nbg_rate, ensure_rates_for_date

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger(__name__)

# --- BOG MAPPING: {db_field_name: output_column_name} ---
BOG_DB_TO_REPORT = {
    "company": "კომპანია",
    "currency": "ვალუტა",
    "entry_date": "თარიღი",
    "document_nomination": "დანიშნულება",
    "sender_details_name": "გამგზავნის/მიმღების დასახელება",
    "entry_amount_debit": "დებეტი",
    "entry_amount_debit_base": "დებეტი ექვ ლარში",
    "entry_amount_credit": "კრედიტი",
    "entry_amount_credit_base": "კრედიტი ექვ ლარში",
    "entry_comment": "ოპერაციის შინაარსი",
    "document_product_group": "ოპერაციის ტიპი",
    "document_information": "დამატებითი ინფორმაცია",
    "closing_balance": "ნაშთი დღის ბოლოს",
    "opening_balance": "საწყისი ნაშთი",
    "entry_amount": "თანხა",
    "account_number": "ანგარიშის ნომერი",
    "document_rate": "ტრანზაქციის კურსი",
    # Add more direct mappings as needed
}

# Define paths relative to the script's location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR) # Assuming bank_data.db and output are in the parent directory

DB_PATH = os.path.join(PARENT_DIR, 'bank_data.db')
OUTPUT_DIR = os.path.join(PARENT_DIR, 'output')

# --- TBC MAPPING: {db_field_name: output_column_name} ---
TBC_DB_TO_REPORT = {
    "company": "კომპანია",
    "currency": "ვალუტა",
    "valueDate": "თარიღი",
    "description": "დანიშნულება",
    "additionalInformation": "დამატებითი ინფორმაცია",
    "amount": "თანხა",  # Used for calculation
    "debitCredit": "debitCredit",  # Used for calculation
    "closing_balance": "ნაშთი",
    "closing_balance_currency": "ნაშთი ექვ.",
    "transactionType": "ტრანზაქციის ტიპი",
    "documentDate": "საბუთის თარიღი",
    "documentNumber": "საბუთის №",
    "partnerAccountNumber": "პარტნიორის ანგარიში",
    "partnerName": "პარტნიორი",
    "partnerTaxCode": "პარტნიორის საგადასახადო კოდი",
    "taxpayerCode": "გადასახადის გადამხდელის კოდი",
    "taxpayerName": "გადასახადის გადამხდელის დასახელება",
    "operationCode": "ოპ. კოდი",
    "account_number": "ანგარიშის ნომერი",
    # Add more direct mappings as needed
}


# --- BOG ---
def read_bog_transactions():
    """
    Reads BOG transactions from the SQLite database table 'bog_transactions'.
    Converts 'entry_date' column to date objects.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            _logger.info(f"Reading data from table: bog_transactions in database: {DB_PATH}")
            df = pd.read_sql_query(f"SELECT * FROM bog_transactions", conn)
        if 'entry_date' in df.columns:
            # Convert to datetime first, then extract date part
            df['entry_date'] = pd.to_datetime(df['entry_date'], errors='coerce').dt.date
        return df
    except Exception as e:
        _logger.error(f"Error reading BOG transactions from DB: {e}")
        return pd.DataFrame() # Return empty DataFrame on error


def map_and_transform_bog(df):
    """
    Maps and transforms BOG transaction data.
    Transactional exchange differences calculation has been removed.
    """
    mapping = BOG_DB_TO_REPORT
    result = pd.DataFrame()
    # Preserve raw data for later calculations or debugging
    result['_raw_account_number'] = df['account_number'] if 'account_number' in df.columns else ''
    result['_raw_currency'] = df['currency'] if 'currency' in df.columns else ''
    result['_raw_date'] = df['entry_date'] if 'entry_date' in df.columns else ''
    result['_raw_company'] = df['company'] if 'company' in df.columns else ''

    # Handle 'ტრანზაქციის კურსი' (document_rate) specifically
    target_document_rate_series = pd.Series(pd.NA, index=df.index)
    if 'document_rate' in df.columns:
        # Ensure it's a Series and convert to float, coercing errors
        target_document_rate_series = pd.to_numeric(df['document_rate'], errors='coerce')
    else:
        _logger.warning("No 'document_rate' column found in raw BOG data, assigning pd.NA to 'ტრანზაქციის კურსი'")

    result['ტრანზაქციის კურსი'] = target_document_rate_series

    # Apply direct mappings and specific calculations
    for src, tgt in mapping.items():
        if src == 'document_rate':
            continue # Already handled above

        if tgt == 'კომპანია':
            result[tgt] = df['company'] if 'company' in df.columns else ''
        elif tgt in ["კატეგორია", "წესი", "სტატუსი"]: # These are empty in the original mapping
            result[tgt] = ''
        elif tgt == "ბრუნვა დებეტი":
            # Group by company, date, currency, account and sum debit amounts
            if not df.empty and 'entry_amount_debit' in df.columns:
                result[tgt] = df.groupby(['company', 'entry_date', 'currency', 'account_number'])['entry_amount_debit'].transform('sum')
            else:
                result[tgt] = pd.NA
        elif tgt == "ბრუნვა კრედიტი":
            # Group by company, date, currency, account and sum credit amounts
            if not df.empty and 'entry_amount_credit' in df.columns:
                result[tgt] = df.groupby(['company', 'entry_date', 'currency', 'account_number'])['entry_amount_credit'].transform('sum')
            else:
                result[tgt] = pd.NA
        elif tgt == "დებეტი ექვ ლარში" and src == "entry_amount_debit_base":
            result[tgt] = pd.to_numeric(df[src], errors='coerce') if src in df.columns else pd.NA
        elif tgt == "კრედიტი ექვ ლარში" and src == "entry_amount_credit_base":
            result[tgt] = pd.to_numeric(df[src], errors='coerce') if src in df.columns else pd.NA
        elif tgt == "თანხა":
            if src in df.columns:
                result[tgt] = pd.to_numeric(df[src], errors='coerce')
            elif not df.empty and 'entry_amount_debit' in df.columns and 'entry_amount_credit' in df.columns:
                # Sum of debit and credit if 'amount' is not directly available
                result[tgt] = pd.to_numeric(df['entry_amount_debit'].fillna(0), errors='coerce') + \
                              pd.to_numeric(df['entry_amount_credit'].fillna(0), errors='coerce')
            else:
                result[tgt] = pd.NA
        elif tgt == "თანხა ექვ ლარში":
            if 'თანხა' in result.columns and not result.empty and 'currency' in df.columns and 'entry_date' in df.columns:
                # Calculate GEL equivalent using NBG rate
                result[tgt] = result["თანხა"] * df.apply(
                    lambda row: get_nbg_rate(row['currency'], row['entry_date']) or 0,
                    axis=1
                )
            else:
                result[tgt] = pd.NA
        elif tgt == "კურსი":
            if not df.empty and 'currency' in df.columns and 'entry_date' in df.columns:
                result[tgt] = df.apply(lambda row: get_nbg_rate(row['currency'], row['entry_date']), axis=1)
            else:
                result[tgt] = pd.NA
        elif tgt == "საწყისი ნაშთი ექვ ლარში":
            if not df.empty and 'opening_balance' in df.columns and 'currency' in df.columns and 'entry_date' in df.columns:
                result[tgt] = df.apply(
                    lambda row: (pd.to_numeric(row['opening_balance'], errors='coerce') or 0) * \
                                (get_nbg_rate(row['currency'], row['entry_date']) or 0),
                    axis=1
                )
            else:
                result[tgt] = pd.NA
        else:
            result[tgt] = df[src] if src in df.columns else pd.NA

    # Ensure all expected columns are present, even if with NA
    for tgt in ["კატეგორია", "წესი", "სტატუსი", "ბრუნვა დებეტი", "ბრუნვა კრედიტი", "თანხა ექვ ლარში", "კურსი",
                "საწყისი ნაშთი ექვ ლარში"]:
        if tgt not in result.columns:
            result[tgt] = pd.NA

    # Calculate TRANS_CURRENCY_RATE if possible
    if 'entry_amount' in df.columns and 'entry_amount_base' in df.columns:
        entry_amount = pd.to_numeric(df['entry_amount'], errors='coerce')
        entry_amount_base = pd.to_numeric(df['entry_amount_base'], errors='coerce')
        # Avoid division by zero and handle cases where amount is zero
        result['TRANS_CURRENCY_RATE'] = (
            entry_amount_base.abs() / entry_amount.abs()
        ).replace([float('inf'), -float('inf')], pd.NA)
    else:
        result['TRANS_CURRENCY_RATE'] = pd.NA

    return result


def read_tbc_transactions():
    """
    Reads TBC transactions from the SQLite database table 'tbc_transactions'.
    Converts 'valueDate' column to date objects.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            _logger.info(f"Reading data from table: tbc_transactions in database: {DB_PATH}")
            df = pd.read_sql_query(f"SELECT * FROM tbc_transactions", conn)
        if 'valueDate' in df.columns:
            df['valueDate'] = pd.to_datetime(df['valueDate'], errors='coerce').dt.date
        return df
    except Exception as e:
        _logger.error(f"Error reading TBC transactions from DB: {e}")
        return pd.DataFrame() # Return empty DataFrame on error


def map_and_transform_tbc(df):
    """
    Maps and transforms TBC transaction data.
    Transactional exchange differences calculation has been removed.
    """
    mapping = TBC_DB_TO_REPORT
    result = pd.DataFrame()
    result['_raw_account_number'] = df['account_number'] if 'account_number' in df.columns else ''
    result['_raw_currency'] = df['currency'] if 'currency' in df.columns else ''
    result['_raw_date'] = df['valueDate'] if 'valueDate' in df.columns else ''
    result['_raw_company'] = df['company'] if 'company' in df.columns else ''

    # Handle 'ტრანზაქციის კურსი' (document_rate equivalent for TBC) specifically
    # For TBC, this is 'exchangeRate'. It needs parsing from "1 CUR = X.XXX GEL" format.
    target_document_rate_series = pd.Series(pd.NA, index=df.index)
    if 'exchangeRate' in df.columns:
        # Define a function to extract the numeric rate from the string
        def parse_exchange_rate_string(rate_str):
            if pd.isna(rate_str) or not isinstance(rate_str, str):
                return pd.NA
            match = re.search(r'=\s*([\d.]+)\s*GEL', rate_str)
            if match:
                try:
                    return float(match.group(1))
                except ValueError:
                    _logger.warning(f"Could not convert extracted rate '{match.group(1)}' to float.")
                    return pd.NA
            else:
                _logger.warning(f"Could not parse exchange rate string: '{rate_str}'")
                return pd.NA

        target_document_rate_series = df['exchangeRate'].apply(parse_exchange_rate_string)
    else:
        _logger.warning("No 'exchangeRate' column found in raw TBC data, assigning pd.NA to 'ტრანზაქციის კურსი'")
    result['ტრანზაქციის კურსი'] = target_document_rate_series # Map TBC's exchangeRate to this common column

    for src, tgt in mapping.items():
        if src == 'exchangeRate': # Already handled above as 'ტრანზაქციის კურსი'
            continue
        if tgt == 'კომპანია':
            result[tgt] = df['company'] if 'company' in df.columns else ''
        elif tgt == 'კურსი':
            # This is the NBG rate for TBC transactions
            if not df.empty and 'currency' in df.columns and 'valueDate' in df.columns:
                result[tgt] = df.apply(lambda row: get_nbg_rate(row.get('currency', ''), row.get('valueDate', '')),
                                       axis=1)
            else:
                result[tgt] = pd.NA
        elif tgt == 'გასული თანხა':
            if not df.empty and 'amount' in df.columns and 'debitCredit' in df.columns:
                result[tgt] = df.apply(lambda row: pd.to_numeric(row['amount'], errors='coerce') if str(row.get('debitCredit', '')) == '1' else pd.NA,
                                       axis=1)
            else:
                result[tgt] = pd.NA
        elif tgt == 'გასული თანხა ექვ.':
            if not df.empty and 'amount' in df.columns and 'debitCredit' in df.columns and 'currency' in df.columns and 'valueDate' in df.columns:
                result[tgt] = df.apply(lambda row: (
                    pd.to_numeric(row['amount'], errors='coerce') * (get_nbg_rate(row.get('currency', ''), row.get('valueDate', '')) or 0)) if str(
                    row.get('debitCredit', '')) == '1' else pd.NA,
                                       axis=1)
            else:
                result[tgt] = pd.NA
        elif tgt == 'შემოსული თანხა':
            if not df.empty and 'amount' in df.columns and 'debitCredit' in df.columns:
                result[tgt] = df.apply(lambda row: pd.to_numeric(row['amount'], errors='coerce') if str(row.get('debitCredit', '')) == '0' else pd.NA,
                                       axis=1)
            else:
                result[tgt] = pd.NA
        elif tgt == 'შემოსული თანხა ექვ.':
            if not df.empty and 'amount' in df.columns and 'debitCredit' in df.columns and 'currency' in df.columns and 'valueDate' in df.columns:
                result[tgt] = df.apply(lambda row: (
                    pd.to_numeric(row['amount'], errors='coerce') * (get_nbg_rate(row.get('currency', ''), row.get('valueDate', '')) or 0)) if str(
                    row.get('debitCredit', '')) == '0' else pd.NA,
                                       axis=1)
            else:
                result[tgt] = pd.NA
        elif src == 'debitCredit':
            continue # Already used in calculations
        else:
            result[tgt] = df[src] if src in df.columns else pd.NA

    # Ensure all expected columns are present, even if with NA
    for tgt in ['გასული თანხა', 'გასული თანხა ექვ.', 'შემოსული თანხა', 'შემოსული თანხა ექვ.', 'კურსი']:
        if tgt not in result.columns:
            result[tgt] = pd.NA

    return result


def get_superset_columns():
    """
    Determines the superset of all columns expected in the final harmonized report.
    FX difference related columns are not included.
    """
    bog_cols = list(BOG_DB_TO_REPORT.values())
    tbc_cols = list(TBC_DB_TO_REPORT.values())

    bog_calculated_cols = [
        "კატეგორია", "წესი", "სტატუსი", "ბრუნვა დებეტი", "ბრუნვა კრედიტი",
        "თანხა ექვ ლარში", "კურსი", "საწყისი ნაშთი ექვ ლარში", "TRANS_CURRENCY_RATE",
        "თანხა", "ვალუტა", "დებეტი", "კრედიტი", "დებეტი ექვ ლარში", "კრედიტი ექვ ლარში"
    ]
    tbc_calculated_cols = ['გასული თანხა', 'გასული თანხა ექვ.', 'შემოსული თანხა', 'შემოსული თანხა ექვ.', 'კურსი']

    superset = []
    # Add BOG columns
    for col in bog_cols:
        if col not in superset:
            superset.append(col)
    for col in bog_calculated_cols:
        if col not in superset:
            superset.append(col)
    # Add TBC columns
    for col in tbc_cols:
        if col not in superset:
            superset.append(col)
    for col in tbc_calculated_cols:
        if col not in superset:
            superset.append(col)
    return superset


SUPERSET_COLUMNS = get_superset_columns()


def harmonize_df(df, superset, bank_name=None):
    """
    Harmonizes a DataFrame to a common set of columns, filling missing
    columns with pd.NA. Optionally adds a 'bank' column.
    """
    df_h = df.copy()
    # Drop raw columns before harmonizing to the superset
    raw_cols = [col for col in df_h.columns if col.startswith('_raw_')]
    df_h = df_h.drop(columns=raw_cols, errors='ignore')

    for col in superset:
        if col not in df_h.columns:
            df_h[col] = pd.NA
    # Reorder columns to match the superset
    df_h = df_h[superset]
    if bank_name is not None:
        df_h['bank'] = bank_name
    return df_h


# --- WRITER ---
def write_excel(date, bog_gel, bog_other, tbc_gel, tbc_other):
    """
    Writes the processed bank transaction data into an Excel file,
    segregated by bank and currency.
    Transactional FX difference formatting has been removed.
    """
    # Output file path no longer includes company name
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    file_path = os.path.join(OUTPUT_DIR, f"Report_Harmonized_{date}.xlsx")

    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        # Write BOG GEL sheet
        bog_gel_h = harmonize_df(bog_gel, SUPERSET_COLUMNS)
        bog_gel_h.to_excel(writer, sheet_name='bog_gel', index=False)

        # Write BOG Other
        bog_other_h = harmonize_df(bog_other, SUPERSET_COLUMNS)
        bog_other_h.to_excel(writer, sheet_name='bog_other', index=False)

        # Write TBC GEL sheet
        tbc_gel_h = harmonize_df(tbc_gel, SUPERSET_COLUMNS)
        tbc_gel_h.to_excel(writer, sheet_name='tbc_gel', index=False)

        # Write TBC Other
        tbc_other_h = harmonize_df(tbc_other, SUPERSET_COLUMNS)
        tbc_other_h.to_excel(writer, sheet_name='tbc_other', index=False)

        # Combined sheet
        combined = []
        if not bog_gel.empty:
            combined.append(harmonize_df(bog_gel, SUPERSET_COLUMNS, bank_name='BOG'))
        if not bog_other.empty:
            combined.append(harmonize_df(bog_other, SUPERSET_COLUMNS, bank_name='BOG'))
        if not tbc_gel.empty:
            combined.append(harmonize_df(tbc_gel, SUPERSET_COLUMNS, bank_name='TBC'))
        if not tbc_other.empty:
            combined.append(harmonize_df(tbc_other, SUPERSET_COLUMNS, bank_name='TBC'))

        if combined:
            combined_df = pd.concat(combined, ignore_index=True)
            cols = [c for c in SUPERSET_COLUMNS if c != 'bank'] + ['bank']
            combined_df = combined_df.reindex(columns=cols, fill_value=pd.NA)
            combined_df.to_excel(writer, sheet_name='all_banks_combined', index=False)

    _logger.info(f"Wrote harmonized report: {file_path}")


# --- MAIN EXECUTION ---
def main():
    """
    Main function to orchestrate reading, transforming, and writing bank data.
    Company separation has been removed.
    """
    _logger.info("Starting harmonized bank data export.")

    # Read raw data
    bog_raw_df = read_bog_transactions()
    tbc_raw_df = read_tbc_transactions()

    # Map and transform data
    bog_df = map_and_transform_bog(bog_raw_df) if not bog_raw_df.empty else pd.DataFrame(columns=SUPERSET_COLUMNS)
    tbc_df = map_and_transform_tbc(tbc_raw_df) if not tbc_raw_df.empty else pd.DataFrame(columns=SUPERSET_COLUMNS)

    # Ensure 'თარიღი' column exists for filtering
    if 'თარიღი' not in bog_df.columns:
        bog_df['თარიღი'] = pd.NA
    if 'თარიღი' not in tbc_df.columns:
        tbc_df['თარიღი'] = pd.NA

    # Get all unique dates from both DataFrames (across all companies)
    all_dates = set(bog_df['თარიღი'].dropna().unique()) | set(tbc_df['თარიღი'].dropna().unique())

    for date in all_dates:
        _logger.info(f"Processing data for date: {date}")
        # Filter data for the current date (all companies combined for this date)
        bog_date = bog_df[bog_df['თარიღი'] == date] if not bog_df.empty else pd.DataFrame(columns=bog_df.columns)
        tbc_date = tbc_df[tbc_df['თარიღი'] == date] if not tbc_df.empty else pd.DataFrame(columns=tbc_df.columns)

        # Ensure 'ვალუტა' column exists for filtering
        if 'ვალუტა' not in bog_date.columns:
            bog_date['ვალუტა'] = ''
        if 'ვალუტა' not in tbc_date.columns:
            tbc_date['ვალუტა'] = ''

        # Segregate by currency (GEL vs. Other)
        bog_gel = bog_date[bog_date['ვალუტა'] == 'GEL'] if not bog_date.empty else pd.DataFrame(columns=bog_date.columns)
        bog_other = bog_date[bog_date['ვალუტა'] != 'GEL'] if not bog_date.empty else pd.DataFrame(columns=bog_date.columns)
        tbc_gel = tbc_date[tbc_date['ვალუტა'] == 'GEL'] if not tbc_date.empty else pd.DataFrame(columns=tbc_date.columns)
        tbc_other = tbc_date[tbc_date['ვალუტა'] != 'GEL'] if not tbc_date.empty else pd.DataFrame(columns=tbc_date.columns)

        # Write data to Excel for the current date (all companies combined)
        write_excel(date, bog_gel, bog_other, tbc_gel, tbc_other)

    _logger.info("Harmonized bank data export completed.")


if __name__ == '__main__':
    main()
