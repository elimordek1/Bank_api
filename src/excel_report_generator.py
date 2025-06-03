import os
import pandas as pd
import sqlite3
import logging
from datetime import datetime, timedelta  # Import timedelta for date calculations

# Import our NBG rates module
# Renamed to avoid conflict with the wrapper function in this file
from nbg_fx_rates import get_nbg_rate as get_nbg_rate_from_module, ensure_rates_for_date

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger(__name__)

# --- BOG MAPPING: {output_column_name: db_field_name or None for calculated/placeholder} ---
BOG_DB_TO_REPORT = {
    "Company": "company",
    "Curr": "currency",
    "თარიღი": "entry_date",
    "დანიშნულება": "document_nomination",
    "გამგზავნის/მიმღების დასახელება": "sender_details_name",
    "დებეტი": "entry_amount_debit",  # Raw debit amount
    "დებეტი ექვ ლარში": "entry_amount_debit_base",  # Raw debit in GEL equivalent
    "კრედიტი": "entry_amount_credit",  # Raw credit amount
    "კრედიტი ექვ ლარში": "entry_amount_credit_base",  # Raw credit in GEL equivalent
    "ოპერაციის შინაარსი": "entry_comment",
    "ოპერაციის ტიპი": "document_product_group",
    "დამატებითი ინფორმაცია": "document_information",
    "ნაშთი დღის ბოლოს": "closing_balance",
    "საწყისი ნაშთი": "opening_balance",
    "თანხა": "entry_amount",  # This is often derived, set to None if not direct DB field
    "ანგარიშის ნომერი": "account_number",
    "Category": None,  # Placeholder
    "Rule Name": None,  # Placeholder
    "Status": None,  # Placeholder
    "ბრუნვა დებეტი": None,  # Calculated
    "ბრუნვა კრედიტი": None,  # Calculated
    "თანხა ექვ ლარში": None,  # Calculated
    "კურსი": None,  # Calculated
    "საწყისი ნაშთი ექვ ლარში": None,  # Calculated
}

BOG_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bank_data.db')
BOG_TABLE_NAME = 'bog_transactions'

# --- TBC MAPPING: {output_column_name: db_field_name or None for calculated/placeholder} ---
TBC_DB_TO_REPORT = {
    "Company": "company",
    "Curr": "currency",
    "თარიღი": "valueDate",
    "დანიშნულება": "description",
    "დამატებითი ინფორმაცია": "additionalInformation",
    "თანხა": "amount",  # Raw amount for debit/credit split
    "ნაშთი": "closing_balance",
    "ნაშთი ექვ.": "closing_balance_currency",
    "ტრანზაქციის ტიპი": "transactionType",
    "საბუთის თარიღი": "documentDate",
    "საბუთის №": "documentNumber",
    "პარტნიორის ანგარიში": "partnerAccountNumber",
    "პარტნიორი": "partnerName",
    "პარტნიორის საგადასახადო კოდი": "partnerTaxCode",
    "გადასახადის გადამხდელის კოდი": "taxpayerCode",
    "გადასახადის გადამხდელის დასახელება": "taxpayerName",
    "ოპ. კოდი": "operationCode",
    "ანგარიშის ნომერი": "account_number",
    "Category": None,  # Placeholder
    "Rule Name": None,  # Placeholder
    "Status": None,  # Placeholder
    "საწყისი ნაშთი": "opening_balance",  # Changed: Map directly from 'opening_balance' column in raw TBC data
    "პარტნიორის ბანკის კოდი": "partnerBankCode",
    "პარტნიორის ბანკი": "partnerBank",
    "შუამავალი ბანკის კოდი": "intermediaryBankCode",
    "შუამავალი ბანკი": "intermediaryBank",
    "ხარჯის ტიპი": "expenseType",
    "სახაზინო კოდი": "treasuryCode",
    "დამატებითი დანიშნულება": "additionalPurpose",
    "debitCredit": "debitCredit",  # Used internally for logic, not direct output
    "გასული თანხა": None,  # Calculated
    "გასული თანხა ექვ.": None,  # Calculated
    "შემოსული თანხა": None,  # Calculated
    "შემოსული თანხა ექვ.": None,  # Calculated
    "კურსი": None,  # Calculated
}

TBC_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bank_data.db')
TBC_TABLE_NAME = 'tbc_transactions'

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')


# Wrapper function to get NBG rate, handles date conversion and ensures rates exist
def get_nbg_rate(currency, date):
    """
    Get NBG exchange rate for a currency on a specific date.

    Args:
        currency: Currency code (e.g., 'USD', 'EUR')
        date: Date as string, datetime, or date object

    Returns:
        Exchange rate or None if not available
    """
    if currency == 'GEL':
        return 1.0

    if isinstance(date, str):
        try:
            date_obj = datetime.strptime(date[:10], '%Y-%m-%d').date()
        except ValueError:
            _logger.error(f"Invalid date format: {date}")
            return None
    elif isinstance(date, datetime):
        date_obj = date.date()
    else:
        date_obj = date

    ensure_rates_for_date(date_obj)

    return get_nbg_rate_from_module(currency, date_obj) or 0


# --- BOG Bank of Georgia related functions ---
def read_bog_transactions():
    """Reads BOG transactions from the SQLite database."""
    with sqlite3.connect(BOG_DB_PATH) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {BOG_TABLE_NAME}", conn)
    if 'entry_date' in df.columns:
        df['entry_date'] = pd.to_datetime(df['entry_date']).dt.date
    return df


def map_and_transform_bog(df):
    """
    Maps BOG database fields to report column names and performs transformations.
    This function now populates columns with their final desired names.
    """
    result = pd.DataFrame()

    # Store raw data for internal grouping in add_foreign_currency_summary_rows
    result['_raw_account_number'] = df['account_number'] if 'account_number' in df.columns else ''
    result['_raw_currency'] = df['currency'] if 'currency' in df.columns else ''
    result['_raw_date'] = df['entry_date'] if 'entry_date' in df.columns else ''
    result['_raw_company'] = df['company'] if 'company' in df.columns else ''

    # Populate columns based on BOG_DB_TO_REPORT mapping and calculations
    for output_col, db_col in BOG_DB_TO_REPORT.items():
        if db_col is not None and db_col in df.columns:
            result[output_col] = df[db_col]
        elif output_col == "თანხა":
            # Calculate 'თანხა' if not directly available
            if 'entry_amount_debit' in df.columns and 'entry_amount_credit' in df.columns:
                result[output_col] = df['entry_amount_debit'].fillna(0) + df['entry_amount_credit'].fillna(0)
            else:
                result[output_col] = ''
        elif output_col == "ბრუნვა დებეტი":
            result[output_col] = df.groupby(['company', 'entry_date', 'currency', 'account_number'])[
                'entry_amount_debit'].transform('sum') if not df.empty and 'entry_amount_debit' in df.columns else ''
        elif output_col == "ბრუნვა კრედიტი":
            result[output_col] = df.groupby(['company', 'entry_date', 'currency', 'account_number'])[
                'entry_amount_credit'].transform('sum') if not df.empty and 'entry_amount_credit' in df.columns else ''
        elif output_col == "თანხა ექვ ლარში":
            if 'თანხა' in result.columns and not result.empty:
                result[output_col] = result["თანხა"] * df.apply(
                    lambda row: get_nbg_rate(row['currency'], row['entry_date']),
                    axis=1) if 'currency' in df.columns and 'entry_date' in df.columns else ''
            else:
                result[output_col] = ''
        elif output_col == "კურსი":
            result[output_col] = df.apply(lambda row: get_nbg_rate(row['currency'], row['entry_date']),
                                          axis=1) if not df.empty and 'currency' in df.columns and 'entry_date' in df.columns else ''
        elif output_col == "საწყისი ნაშთი ექვ ლარში":
            result[output_col] = df.apply(
                lambda row: (row['opening_balance'] or 0) * (get_nbg_rate(row['currency'], row['entry_date'])),
                axis=1) if not df.empty and 'opening_balance' in df.columns and 'currency' in df.columns and 'entry_date' in df.columns else ''
        else:
            # For placeholders or columns not directly from DB/calculated here
            result[output_col] = ''

    return result


# --- TBC TBC Bank related functions ---
def read_tbc_transactions():
    """Reads TBC transactions from the SQLite database."""
    with sqlite3.connect(TBC_DB_PATH) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {TBC_TABLE_NAME}", conn)
    if 'valueDate' in df.columns:
        df['valueDate'] = pd.to_datetime(df['valueDate']).dt.date
    return df


def map_and_transform_tbc(df):
    """
    Maps TBC database fields to report column names and performs transformations.
    This function now populates columns with their final desired names.
    """
    result = pd.DataFrame()

    # Store raw data for internal grouping in add_foreign_currency_summary_rows
    result['_raw_account_number'] = df['account_number'] if 'account_number' in df.columns else ''
    result['_raw_currency'] = df['currency'] if 'currency' in df.columns else ''
    result['_raw_date'] = df['valueDate'] if 'valueDate' in df.columns else ''
    result['_raw_company'] = df['company'] if 'company' in df.columns else ''

    # Populate columns based on TBC_DB_TO_REPORT mapping and calculations
    for output_col, db_col in TBC_DB_TO_REPORT.items():
        if db_col is not None and db_col in df.columns:
            # Special handling for debitCredit as it's used for logic, not direct output
            if output_col == "debitCredit":
                result[output_col] = df[db_col]
            else:
                result[output_col] = df[db_col]
        elif output_col in ['გასული თანხა', 'შემოსული თანხა', 'გასული თანხა ექვ.', 'შემოსული თანხა ექვ.']:
            # Apply the logic for outgoing/incoming amounts and their GEL equivalents
            # Ensure 'amount' is numeric and handle NaN values before calculations
            if not df.empty and 'amount' in df.columns and 'debitCredit' in df.columns:
                # Convert 'amount' to numeric, coercing errors to NaN
                # Then check for NaN and use 0 if it's NaN

                if output_col == 'გასული თანხა':  # Outgoing Amount (Credit)
                    result[output_col] = df.apply(
                        lambda row: (pd.to_numeric(row['amount'], errors='coerce') if pd.notna(
                            pd.to_numeric(row['amount'], errors='coerce')) else 0)
                        if str(row.get('debitCredit', '')) == '0' else '',
                        axis=1
                    )
                elif output_col == 'გასული თანხა ექვ.':  # Outgoing Amount GEL Equivalent (Credit)
                    result[output_col] = df.apply(
                        lambda row: ((pd.to_numeric(row['amount'], errors='coerce') if pd.notna(
                            pd.to_numeric(row['amount'], errors='coerce')) else 0) * (
                                         get_nbg_rate(row.get('currency', ''), row.get('valueDate', ''))))
                        if str(row.get('debitCredit', '')) == '0' else '',
                        axis=1
                    )
                elif output_col == 'შემოსული თანხა':  # Incoming Amount (Debit)
                    result[output_col] = df.apply(
                        lambda row: (pd.to_numeric(row['amount'], errors='coerce') if pd.notna(
                            pd.to_numeric(row['amount'], errors='coerce')) else 0)
                        if str(row.get('debitCredit', '')) == '1' else '',
                        axis=1
                    )
                elif output_col == 'შემოსული თანხა ექვ.':  # Incoming Amount GEL Equivalent (Debit)
                    result[output_col] = df.apply(
                        lambda row: ((pd.to_numeric(row['amount'], errors='coerce') if pd.notna(
                            pd.to_numeric(row['amount'], errors='coerce')) else 0) * (
                                         get_nbg_rate(row.get('currency', ''), row.get('valueDate', ''))))
                        if str(row.get('debitCredit', '')) == '1' else '',
                        axis=1
                    )
            else:
                result[output_col] = ''
        elif output_col == 'კურსი':
            if 'exchangeRate' in df.columns:
                result[output_col] = df['exchangeRate'] if pd.api.types.is_numeric_dtype(df['exchangeRate']) else ''
            else:
                result[output_col] = df.apply(
                    lambda row: get_nbg_rate(row.get('currency', ''), row.get('valueDate', '')),
                    axis=1) if not df.empty else ''
        else:
            # For placeholders or columns not directly from DB/calculated here
            result[output_col] = ''

    return result


def get_report_column_sets():
    """
    Defines the specific column sets for each type of report sheet.
    """
    # BOG GEL desired columns
    bog_gel_cols = [
        "თარიღი",
        "დანიშნულება",
        "გამგზავნის/მიმღების დასახელება",
        "დებეტი",
        "კრედიტი",
        "ოპერაციის შინაარსი",
        "ოპერაციის ტიპი",
        "თანხა",
        "ბრუნვა დებეტი",
        "ბრუნვა კრედიტი",
        "ნაშთი დღის ბოლოს",
        "დამატებითი ინფორმაცია",
        "Curr",
        "Company",
        "Category",
        "Rule Name",
        "Status"
    ]

    # BOG Other (Currency) desired columns
    bog_other_cols = [
        "თარიღი",
        "დანიშნულება",
        "გამგზავნის/მიმღების დასახელება",
        "დებეტი",
        "დებეტი ექვ ლარში",
        "კრედიტი",
        "კრედიტი ექვ ლარში",
        "ოპერაციის შინაარსი",
        "კურსი",
        "ოპერაციის ტიპი",
        "თანხა",
        "თანხა ექვ ლარში",
        "ბრუნვა დებეტი",
        "ბრუნვა კრედიტი",
        "ნაშთი დღის ბოლოს",
        "დამატებითი ინფორმაცია",
        "Curr",
        "Company",
        "Category",
        "Rule Name",
        "Status",
        "სავალუტო სხვაობა - Term1",
        "სავალუტო სხვაობა - Term2",
        "სავალუტო სხვაობა - Term3"
    ]

    # TBC specific columns as provided by the user, in the specified order
    tbc_common_ordered_cols = [
        "თარიღი",
        "დანიშნულება",
        "დამატებითი ინფორმაცია",
        "გასული თანხა",
        "გასული თანხა ექვ.",
        "შემოსული თანხა",
        "შემოსული თანხა ექვ.",
        "ნაშთი",
        "ნაშთი ექვ.",
        "ტრანზაქციის ტიპი",
        "საბუთის თარიღი",
        "საბუთის №",
        "პარტნიორის ანგარიში",
        "პარტნიორი",
        "პარტნიორის საგადასახადო კოდი",
        "პარტნიორის ბანკის კოდი",
        "პარტნიორის ბანკი",
        "შუამავალი ბანკის კოდი",
        "შუამავალი ბანკი",
        "ხარჯის ტიპი",
        "გადასახადის გადამხდელის კოდი",
        "გადასახადის გადამხდელის დასახელება",
        "სახაზინო კოდი",
        "ოპ. კოდი",
        "დამატებითი დანიშნულება",
        "Curr",
        "Company",
        "Category",
        "Rule Name",
        "Status",
        "ანგარიშის ნომერი",  # This is often common, adding it here for TBC
        "საწყისი ნაშთი",  # Added for TBC sheets
    ]

    # TBC GEL columns will use the common TBC order, but some fields will be empty
    tbc_gel_cols = list(tbc_common_ordered_cols)

    # TBC Other (Currency) columns will use the common TBC order and include FX terms
    tbc_other_cols = list(tbc_common_ordered_cols)
    tbc_other_cols.extend([
        "კურსი",  # Currency rate is only relevant for non-GEL
        "სავალუტო სხვაობა - Term1",
        "სავალუტო სხვაობა - Term2",
        "სავალუტო სხვაობა - Term3"
    ])

    # Combined columns will be the superset of all, maintaining a logical order
    # Fix for FutureWarning: unique with argument that is not not a Series, Index, ExtensionArray, or np.ndarray is deprecated
    combined_cols = pd.Series(
        bog_gel_cols + bog_other_cols + tbc_gel_cols + tbc_other_cols + ["bank"]).unique().tolist()

    return {
        'bog_gel': bog_gel_cols,
        'bog_other': bog_other_cols,
        'tbc_gel': tbc_gel_cols,
        'tbc_other': tbc_other_cols,
        'combined': combined_cols
    }


def add_foreign_currency_summary_rows(df, bank_type='BOG'):
    """
    Adds summary rows for foreign currency accounts to calculate exchange rate differences
    based on the provided new logic.

    New Formula (adjusted for expected output):
    (Opening Balance * Previous Day's GEL Exchange Rate)
    - ((Opening Balance + Credit - Debit) * Today's GEL Exchange Rate)
    + ((Credit - Debit) * Today's GEL Exchange Rate)

    Args:
        df (pd.DataFrame): The DataFrame containing bank transactions, including raw columns.
        bank_type (str): 'BOG' or 'TBC' to determine how to extract data.

    Returns:
        pd.DataFrame: The DataFrame with added summary rows for exchange differences.
    """
    if df.empty:
        return df

    if '_raw_currency' not in df.columns or '_raw_account_number' not in df.columns:
        _logger.warning("Raw columns not found in dataframe, cannot add summary rows for exchange difference.")
        return df

    foreign_df = df[df['_raw_currency'] != 'GEL'].copy()
    if foreign_df.empty:
        return df

    groups = foreign_df.groupby(['_raw_company', '_raw_account_number', '_raw_currency', '_raw_date'])

    summary_rows = []

    for (company, account, currency, date), group in groups:
        try:
            opening_balance = 0  # Initialize for safety
            closing_balance = 0  # Initialize for safety

            if bank_type == 'BOG':
                # Safely get opening_balance for BOG
                if 'საწყისი ნაშთი' in group.columns and not group['საწყისი ნაშთი'].empty:
                    val = pd.to_numeric(group['საწყისი ნაშთი'].iloc[0], errors='coerce')
                    opening_balance = val if pd.notna(val) else 0
                else:
                    opening_balance = 0

                # Safely get closing_balance for BOG
                if 'ნაშთი დღის ბოლოს' in group.columns and not group['ნაშთი დღის ბოლოს'].empty:
                    val = pd.to_numeric(group['ნაშთი დღის ბოლოს'].iloc[0], errors='coerce')
                    closing_balance = val if pd.notna(val) else 0
                else:
                    closing_balance = 0
            else:  # TBC
                # Safely get opening_balance for TBC (read directly from 'საწყისი ნაშთი')
                if 'საწყისი ნაშთი' in group.columns and not group['საწყისი ნაშთი'].empty:
                    val = pd.to_numeric(group['საწყისი ნაშთი'].iloc[0], errors='coerce')
                    opening_balance = val if pd.notna(val) else 0
                else:
                    opening_balance = 0

                # Safely get closing_balance for TBC
                if 'ნაშთი' in group.columns and not group['ნაშთი'].empty:
                    val = pd.to_numeric(group['ნაშთი'].iloc[0], errors='coerce')
                    closing_balance = val if pd.notna(val) else 0
                else:
                    closing_balance = 0

            # For turnover calculation, use the already mapped/transformed columns
            if bank_type == 'BOG':
                total_debit_for_turnover = pd.to_numeric(group['დებეტი'], errors='coerce').fillna(0).sum()
                total_credit_for_turnover = pd.to_numeric(group['კრედიტი'], errors='coerce').fillna(0).sum()
            else:  # TBC
                # Use the 'შემოსული თანხა' (incoming) and 'გასული თანხა' (outgoing) for turnover
                # as these are now populated based on the debitCredit logic
                total_debit_for_turnover = pd.to_numeric(group['შემოსული თანხა'], errors='coerce').fillna(0).sum()
                total_credit_for_turnover = pd.to_numeric(group['გასული თანხა'], errors='coerce').fillna(0).sum()

            turnover = total_credit_for_turnover - total_debit_for_turnover

            today_rate = get_nbg_rate(currency, date)
            previous_date = date - timedelta(days=1)
            previous_day_rate = get_nbg_rate(currency, previous_date)

            term1 = opening_balance * previous_day_rate
            term2 = (opening_balance + turnover) * today_rate
            term3 = turnover * today_rate

            exchange_diff = term1 - term2 + term3

            summary_row = pd.Series(dtype='object')

            # Populate relevant columns for the summary row based on the bank type
            summary_row['Company'] = company
            summary_row['Curr'] = currency
            summary_row['თარიღი'] = date
            summary_row['დანიშნულება'] = 'გადაფასება'  # Changed to 'გადაფასება'
            summary_row['ოპერაციის შინაარსი'] = 'კურსთაშორისი სხვაობა'
            summary_row['ანგარიშის ნომერი'] = account

            # Populate Debit GEL Equivalent or Credit GEL Equivalent based on exchange_diff sign
            ccy_diff_dr_column_name = 'დებეტი ექვ ლარში' if bank_type == 'BOG' else 'შემოსული თანხა ექვ.'
            ccy_diff_cr_column_name = 'კრედიტი ექვ ლარში' if bank_type == 'BOG' else 'გასული თანხა ექვ.'

            if exchange_diff < 0:
                summary_row[ccy_diff_dr_column_name] = exchange_diff
                summary_row[ccy_diff_cr_column_name] = ''  # Ensure the other is empty
            elif exchange_diff > 0:
                summary_row[ccy_diff_cr_column_name] = abs(exchange_diff)
                summary_row[ccy_diff_dr_column_name] = ''  # Ensure the other is empty
            else:
                summary_row[ccy_diff_dr_column_name] = 0
                summary_row[ccy_diff_cr_column_name] = 0

            # The 'თანხა ექვ ლარში' is the total exchange difference, which is already handled
            if bank_type == 'BOG':
                summary_row['თანხა ექვ ლარში'] = exchange_diff  # This holds the raw calculated difference
            

            summary_row['კურსი'] = today_rate

            # Add opening balance for TBC summary rows (now it's the directly read value)
            if bank_type == 'TBC':
                summary_row['საწყისი ნაშთი'] = opening_balance

            # Add intermediate terms for debugging/information
            summary_row['სავალუტო სხვაობა - Term1'] = term1
            summary_row['სავალუტო სხვაობა - Term2'] = term2
            summary_row['სავალუტო სხვაობა - Term3'] = term3

            # Set other columns to empty string for consistency with harmonize_df
            for col in get_report_column_sets()['combined']:
                if col not in summary_row.index and not col.startswith('_raw_') and col != 'bank':
                    summary_row[col] = ''

            summary_rows.append(summary_row)

        except Exception as e:
            _logger.warning(f"Error calculating summary for {company}-{account}-{currency}-{date}: {e}")
            continue

    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        result = pd.concat([df, summary_df], ignore_index=True)
        return result

    return df


# Get the predefined column sets
REPORT_COLUMN_SETS = get_report_column_sets()


def harmonize_df(df, target_columns, bank_name=None):
    """
    Harmonizes a DataFrame to a predefined set of target columns, filling missing
    columns with empty strings and reordering, then dropping columns not in target_columns.
    """
    df_h = df.copy()
    raw_cols = [col for col in df_h.columns if col.startswith('_raw_')]
    df_h = df_h.drop(columns=raw_cols, errors='ignore')

    # Add missing columns with empty string values
    for col in target_columns:
        if col not in df_h.columns:
            df_h[col] = ''

    # Select and reorder only the target columns
    df_h = df_h[target_columns]

    if bank_name is not None and 'bank' in target_columns:
        df_h['bank'] = bank_name

    return df_h


# --- WRITER functions for Excel output ---
def write_excel(company, date, bog_gel, bog_other, tbc_gel, tbc_other):
    """
    Writes the processed bank data for a specific company and date into an Excel file,
    creating separate sheets for GEL and other currencies, and a combined sheet.
    """
    company_dir = os.path.join(OUTPUT_DIR, company)
    os.makedirs(company_dir, exist_ok=True)
    file_path = os.path.join(company_dir, f"Report_{company}_{date}.xlsx")

    bog_other_with_summary = add_foreign_currency_summary_rows(bog_other, bank_type='BOG')
    tbc_other_with_summary = add_foreign_currency_summary_rows(tbc_other, bank_type='TBC')

    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        workbook = writer.book
        summary_format = workbook.add_format({
            'bold': True,
            'bg_color': '#FFFF99',
            'border': 1
        })

        # Write BOG GEL sheet
        bog_gel_h = harmonize_df(bog_gel, REPORT_COLUMN_SETS['bog_gel'])
        bog_gel_h.to_excel(writer, sheet_name='bog_gel', index=False)

        # Write BOG Other with summary rows
        bog_other_h = harmonize_df(bog_other_with_summary, REPORT_COLUMN_SETS['bog_other'])
        bog_other_h.to_excel(writer, sheet_name='bog_other', index=False)
        worksheet = writer.sheets['bog_other']
        for idx, row in bog_other_with_summary.iterrows():
            if 'სავალუტო სხვაობა' in str(row.get('დანიშნულება', '')):
                worksheet.set_row(idx + 1, None, summary_format)

        # Write TBC GEL sheet
        tbc_gel_h = harmonize_df(tbc_gel, REPORT_COLUMN_SETS['tbc_gel'])
        tbc_gel_h.to_excel(writer, sheet_name='tbc_gel', index=False)

        # Write TBC Other with summary rows
        tbc_other_h = harmonize_df(tbc_other_with_summary, REPORT_COLUMN_SETS['tbc_other'])
        tbc_other_h.to_excel(writer, sheet_name='tbc_other', index=False)
        worksheet = writer.sheets['tbc_other']
        for idx, row in tbc_other_with_summary.iterrows():
            if 'სავალუტო სხვაობა' in str(row.get('დანიშნულება', '')):
                worksheet.set_row(idx + 1, None, summary_format)

        # Combined sheet
        combined = []
        if not bog_gel.empty:
            combined.append(harmonize_df(bog_gel, REPORT_COLUMN_SETS['combined'], bank_name='BOG'))
        if not bog_other_with_summary.empty:
            combined.append(harmonize_df(bog_other_with_summary, REPORT_COLUMN_SETS['combined'], bank_name='BOG'))
        if not tbc_gel.empty:
            combined.append(harmonize_df(tbc_gel, REPORT_COLUMN_SETS['combined'], bank_name='TBC'))
        if not tbc_other_with_summary.empty:
            combined.append(harmonize_df(tbc_other_with_summary, REPORT_COLUMN_SETS['combined'], bank_name='TBC'))
        if combined:
            combined_df = pd.concat(combined, ignore_index=True)
            # Ensure 'bank' is the last column if it's in the combined set
            if 'bank' in REPORT_COLUMN_SETS['combined']:
                cols = [c for c in REPORT_COLUMN_SETS['combined'] if c != 'bank'] + ['bank']
                combined_df = combined_df[cols]
            combined_df.to_excel(writer, sheet_name='all_banks_combined', index=False)

            worksheet = writer.sheets['all_banks_combined']
            for idx, row in combined_df.iterrows():
                if 'სავალუტო სხვაობა' in str(row.get('დანიშნულება', '')):
                    worksheet.set_row(idx + 1, None, summary_format)

    _logger.info(f"Wrote report: {file_path}")


# --- MAIN execution flow ---
def main():
    """
    Main function to read, process, and write bank transaction reports.
    """
    bog_df = read_bog_transactions()
    bog_df = map_and_transform_bog(bog_df) if not bog_df.empty else pd.DataFrame()
    tbc_df = read_tbc_transactions()
    tbc_df = map_and_transform_tbc(tbc_df) if not tbc_df.empty else pd.DataFrame()

    if 'Company' not in bog_df.columns:
        bog_df['Company'] = ''
    if 'Company' not in tbc_df.columns:
        tbc_df['Company'] = ''

    companies = set(bog_df['Company'].dropna().unique()) | set(tbc_df['Company'].dropna().unique())

    for company in companies:
        bog_c = bog_df[bog_df['Company'] == company] if not bog_df.empty else pd.DataFrame(columns=bog_df.columns)
        
        tbc_c = tbc_df[tbc_df['Company'] == company] if not tbc_df.empty else pd.DataFrame(columns=tbc_df.columns)


        if 'თარიღი' not in bog_c.columns:
            bog_c['თარიღი'] = ''
        if 'თარიღი' not in tbc_c.columns:
            tbc_c['თარიღი'] = ''

        dates = set(bog_c['თარიღი'].dropna().unique()) | set(tbc_c['თარიღი'].dropna().unique())

        for date in dates:
            bog_date = bog_c[bog_c['თარიღი'] == date] if not bog_c.empty else pd.DataFrame(columns=bog_c.columns)
            tbc_date = tbc_c[tbc_c['თარიღი'] == date] if not tbc_c.empty else pd.DataFrame(columns=tbc_c.columns)

            if 'Curr' not in bog_date.columns:
                bog_date['Curr'] = ''
            if 'Curr' not in tbc_date.columns:
                tbc_date['Curr'] = ''

            bog_gel = bog_date[bog_date['Curr'] == 'GEL'] if not bog_date.empty else pd.DataFrame(
                columns=bog_date.columns)
            bog_other = bog_date[bog_date['Curr'] != 'GEL'] if not bog_date.empty else pd.DataFrame(
                columns=bog_date.columns)
            
            
            tbc_gel = tbc_date[tbc_date['Curr'] == 'GEL'] if not tbc_date.empty else pd.DataFrame(
                columns=tbc_date.columns)
            
            tbc_other = tbc_date[tbc_date['Curr'] != 'GEL'] if not tbc_date.empty else pd.DataFrame(
                columns=tbc_date.columns)
            
                

            write_excel(company, date, bog_gel, bog_other, tbc_gel, tbc_other)


if __name__ == '__main__':
    main()
