import pandas as pd
import datetime
import os
import logging
import sqlite3

# Assuming the provided API interaction code is in a file named 'bog_api_connector.py'
# We still need fetch_nbg_currency_df from here, and initialize_db for consistency,
# but we will *not* use get_all_transactions for fetching now.
from bog_api import fetch_nbg_currency_df, initialize_db

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger(__name__)

def read_transactions_from_sqlite(start_date_str=None, end_date_str=None, db_path='bank_data.db', table_name='bog_transactions'):
    """
    Reads transactions from the SQLite database within a specified date range.

    Args:
        start_date_str (str, optional): The start date (YYYY-MM-DD) for filtering transactions.
                                        If None, fetches from the earliest date available.
        end_date_str (str): The end date (YYYY-MM-DD) for filtering transactions.
                                      If None, fetches up to the latest date available.
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table containing transactions.

    Returns:
        pd.DataFrame: A DataFrame containing the fetched transactions.
    """
    _logger.info(f"Reading transactions from SQLite table '{table_name}' in '{db_path}'...")
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        query = f"SELECT * FROM {table_name}"
        conditions = []

        # Adjusting conditions to properly filter by date only, ignoring time for exact day match
        if start_date_str:
            conditions.append(f"STRFTIME('%Y-%m-%d', entry_date) >= '{start_date_str}'")
        if end_date_str:
            conditions.append(f"STRFTIME('%Y-%m-%d', entry_date) <= '{end_date_str}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        df = pd.read_sql_query(query, conn)
        _logger.info(f"Successfully read {len(df)} transactions from SQLite.")

        # Ensure 'entry_date' is datetime for consistency, then convert to date string
        if 'entry_date' in df.columns:
            df['entry_date'] = pd.to_datetime(df['entry_date'])
        else:
            _logger.error("'entry_date' column not found in database. Please verify table schema.")
            return pd.DataFrame()

        # Ensure base currency fields are numeric and handle potential NaNs
        # It's crucial that these columns exist and are numeric for correct GEL equivalent reporting.
        required_numeric_cols = [
            'entry_amount_debit', 'entry_amount_credit',
            'entry_amount_debit_base', 'entry_amount_credit_base',
            'opening_balance', 'closing_balance'
            # 'opening_balance_base', 'closing_balance_base' - these may not be directly used for transaction lines
        ]
        for col in required_numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                _logger.warning(f"Column '{col}' not found in transactions. Will proceed, but calculations might be affected.")
                df[col] = 0.0 # Add column as 0.0 if not found


        return df
    except FileNotFoundError:
        _logger.error(f"SQLite database file not found at: {db_path}.")
        return pd.DataFrame()
    except pd.io.sql.DatabaseError as e:
        _logger.error(f"Database error when reading from SQLite: {e}")
        return pd.DataFrame()
    except Exception as e:
        _logger.error(f"An unexpected error occurred while reading from SQLite: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


def generate_daily_bank_report(report_date_str):
    """
    Generates daily bank reports for BOG accounts, creating a separate Excel file for each company.
    Reads transactions from the local SQLite database.

    Args:
        report_date_str (str): The date for which to generate the report, in 'YYYY-MM-DD' format.
    """
    _logger.info(f"Generating daily bank report for {report_date_str} using data from SQLite.")

    # READ TRANSACTIONS FROM SQLITE DATABASE
    # Fetch all transactions up to the report date for opening balance calculation
    df_transactions = read_transactions_from_sqlite(end_date_str=report_date_str)

    if df_transactions.empty:
        _logger.warning(f"No transactions found in SQLite up to the report date {report_date_str}. No reports will be generated.")
        print(f"No transactions found for {report_date_str}. No reports generated.")
        return

    df_transactions['entry_date_only'] = df_transactions['entry_date'].dt.strftime('%Y-%m-%d')
    # Filter for transactions specifically on the report day
    df_report_day = df_transactions[df_transactions['entry_date_only'] == report_date_str].copy()

    if df_report_day.empty:
        _logger.info(f"No transactions specifically on {report_date_str} found in the filtered SQLite data. No reports will be generated.")
        print(f"No transactions found for {report_date_str}. No reports generated.")
        return

    report_date_dt = datetime.datetime.strptime(report_date_str, '%Y-%m-%d').date()
    previous_report_date_dt = report_date_dt - datetime.timedelta(days=1)

    # Fetch NBG currency rates for the report date (current rates)
    nbg_rates_df_current = fetch_nbg_currency_df(report_date_dt)
    if nbg_rates_df_current.empty:
        _logger.warning(f"Could not fetch CURRENT NBG currency rates for {report_date_str}. Foreign currency equivalents might be inaccurate.")
        nbg_rates_df_current = pd.DataFrame([{'date': report_date_str, 'currency': 'GEL', 'rate_per_unit': 1.0}])

    # Fetch NBG currency rates for the PREVIOUS day (needed for revaluation calculation)
    nbg_rates_df_previous = fetch_nbg_currency_df(previous_report_date_dt)
    if nbg_rates_df_previous.empty:
        _logger.warning(f"Could not fetch PREVIOUS DAY'S NBG currency rates for {previous_report_date_dt}. Revaluation might be inaccurate. Defaulting to current day's rate for opening balance GEL equivalent.")
        # If previous day's rates aren't available, default to current day's for opening balance value
        nbg_rates_df_previous = nbg_rates_df_current.copy()

        # Merge CURRENT NBG rates with transactions (still needed for 'კურსი' and revaluation)
    df_report_day = pd.merge(df_report_day, nbg_rates_df_current[['currency', 'rate_per_unit']],
                             on='currency', how='left', suffixes=('', '_current'))

    # Fill NaN rates for GEL currency (rate is 1)
    df_report_day.loc[df_report_day['currency'] == 'GEL', 'rate_per_unit_current'] = 1.0
    df_report_day['rate_per_unit_current'] = df_report_day['rate_per_unit_current'].fillna(0)


    # --- CRITICAL FIXES FOR GEL EQUIVALENTS OF TRANSACTIONS ---
    df_report_day['entry_amount_debit'] = pd.to_numeric(df_report_day['entry_amount_debit'], errors='coerce').fillna(0)
    df_report_day['entry_amount_credit'] = pd.to_numeric(df_report_day['entry_amount_credit'], errors='coerce').fillna(0)

    # Use the base currency amounts directly from the database for GEL equivalents of transactions
    df_report_day['დებეტი ექვ ლარში'] = df_report_day['entry_amount_debit_base']
    df_report_day['კრედიტი ექვ ლარში'] = df_report_day['entry_amount_credit_base']

    df_report_day['calculated_amount'] = df_report_day['entry_amount_credit'] - df_report_day['entry_amount_debit']
    df_report_day['calculated_amount_gel_equiv'] = df_report_day['კრედიტი ექვ ლარში'] - df_report_day['დებეტი ექვ ლარში']

    # Ensure 'opening_balance' and 'closing_balance' are numeric (these are FC balances from DB)
    df_report_day['opening_balance'] = pd.to_numeric(df_report_day['opening_balance'], errors='coerce').fillna(0)
    df_report_day['closing_balance'] = pd.to_numeric(df_report_day['closing_balance'], errors='coerce').fillna(0)

    # Group by company, account_number, currency to get daily sums and opening/closing balances
    # 'total_debit_gel_eq' and 'total_credit_gel_eq' will now accurately sum the historical GEL values
    daily_summary = df_report_day.groupby(['company', 'account_number', 'currency']).agg(
        total_debit_fc=('entry_amount_debit', 'sum'),
        total_credit_fc=('entry_amount_credit', 'sum'),
        total_debit_gel_eq=('დებეტი ექვ ლარში', 'sum'),
        total_credit_gel_eq=('კრედიტი ექვ ლარში', 'sum'),
        closing_balance_fc=('closing_balance', 'last'),  # Last transaction's closing balance for the day
        opening_balance_fc=('opening_balance', 'first') # First transaction's opening balance for the day
    ).reset_index()

    # Merge daily summary back to individual transactions to get turnover and balances per row
    df_report_day = df_report_day.merge(
        daily_summary[['company', 'account_number', 'currency',
                       'total_debit_fc', 'total_credit_fc',
                       'opening_balance_fc', 'closing_balance_fc']],
        on=['company', 'account_number', 'currency'],
        how='left'
    )

    # Assign summary values to new columns for the report
    df_report_day['ბრუნვა დებეტი'] = df_report_day['total_debit_fc']
    df_report_day['ბრუნვა კრედიტი'] = df_report_day['total_credit_fc']
    df_report_day['ნაშთი დღის ბოლოს'] = df_report_day['closing_balance_fc']
    df_report_day['საწყისი ნაშთი'] = df_report_day['opening_balance_fc'] # Adding Opening Balance FC


    # Prepare base columns for the report
    common_report_columns = {
        'entry_date_only': 'თარიღი',
        'document_nomination': 'დანიშნულება',
        'sender_details_name': 'გამგზავნის/მიმღების დასახელება',
        'beneficiary_details_name': 'მიმღების დასახელება', # Temporary for combining
        'entry_amount_debit': 'დებეტი',       # FC Debit
        'entry_amount_credit': 'კრედიტი',     # FC Credit
        'entry_comment': 'ოპერაციის შინაარსი',
        'document_product_group': 'ოპერაციის ტიპი',
        'calculated_amount': 'თანხა',        # Net FC Amount for transactions
        'ბრუნვა დებეტი': 'ბრუნვა დებეტი',
        'ბრუნვა კრედიტი': 'ბრუნვა კრედიტი',
        'ნაშთი დღის ბოლოს': 'ნაშთი დღის ბოლოს',
        'საწყისი ნაშთი': 'საწყისი ნაშთი',
        'document_information': 'დამატებითი ინფორმაცია',
        'currency': 'Curr',
        'company': 'Company'
    }

    final_df_all_companies = df_report_day.rename(columns=common_report_columns)
    # Combine sender and beneficiary names for the "From/To" column
    final_df_all_companies['გამგზავნის/მიმღების დასახელება'] = final_df_all_companies['გამგზავნის/მიმღების დასახელება'].fillna(final_df_all_companies['მიმღების დასახელება'])
    final_df_all_companies.drop(columns=['მიმღების დასახელება'], inplace=True, errors='ignore')

    # Add specific columns for foreign currency (GEL equivalent columns)
    # These are now correctly based on the 'base' amounts from the DB for individual transactions.
    # We rename here for clarity in final_df_all_companies.
    # These already come from 'entry_amount_debit_base' and 'entry_amount_credit_base' in df_report_day
    # So we just ensure column names are consistent.
    final_df_all_companies['დებეტი ექვ ლარში'] = final_df_all_companies['დებეტი ექვ ლარში'] # This now holds base values
    final_df_all_companies['კრედიტი ექვ ლარში'] = final_df_all_companies['კრედიტი ექვ ლარში'] # This now holds base values
    final_df_all_companies['კურსი'] = final_df_all_companies['rate_per_unit_current'] # Using the current day's rate
    final_df_all_companies['თანხა ექვ ლარში'] = final_df_all_companies['calculated_amount_gel_equiv'] # This now holds base values

    # Calculate and add Opening Balance GEL Equivalent
    # Use the `opening_balance` (FC) from the daily summary and the `rate_per_unit_previous`

    # Prepare previous rates to be merged onto the report day's data
    nbg_rates_df_previous_for_merge = nbg_rates_df_previous[['currency', 'rate_per_unit']].rename(columns={'rate_per_unit': 'rate_per_unit_previous'})

    # It's safer to merge onto the original df_report_day then propagate
    df_report_day_with_prev_rate = pd.merge(df_report_day.copy(), nbg_rates_df_previous_for_merge,
                                            left_on='currency', right_on='currency', how='left')

    # Ensure GEL rate for previous day is 1.0 if not found or is GEL
    df_report_day_with_prev_rate.loc[df_report_day_with_prev_rate['currency'] == 'GEL', 'rate_per_unit_previous'] = 1.0
    df_report_day_with_prev_rate['rate_per_unit_previous'] = df_report_day_with_prev_rate['rate_per_unit_previous'].fillna(0) # Fill any remaining NaNs

    # This calculation needs to happen BEFORE `final_df_all_companies` is sliced per company
    # because 'საწყისი ნაშთი' comes from `df_report_day`
    # and 'rate_per_unit_previous' is from `df_report_day_with_prev_rate`
    final_df_all_companies['საწყისი ნაშთი ექვ ლარში'] = \
        df_report_day_with_prev_rate['opening_balance_fc'] * df_report_day_with_prev_rate['rate_per_unit_previous']


    # Add new columns for native currency, with placeholders
    final_df_all_companies['Category'] = 'Uncategorized'
    final_df_all_companies['Rule Name'] = None
    final_df_all_companies['Status'] = 'Uncategorized'

    # Get unique companies
    companies = final_df_all_companies['Company'].unique()
    if len(companies) == 0:
        _logger.warning("No companies found in the transactions for the report date. No reports will be generated.")
        return

    # Define the output directory
    output_dir = 'daily_reports'
    os.makedirs(output_dir, exist_ok=True)

    # Define the exact order of columns for each sheet
    foreign_currency_output_order = [
        'თარიღი', 'დანიშნულება', 'გამგზავნის/მიმღების დასახელება',
        'საწყისი ნაშთი', 'საწყისი ნაშთი ექვ ლარში', # Added opening balances
        'დებეტი', 'დებეტი ექვ ლარში', 'კრედიტი', 'კრედიტი ექვ ლარში',
        'ოპერაციის შინაარსი', 'კურსი', 'ოპერაციის ტიპი', 'თანხა', 'თანხა ექვ ლარში',
        'ბრუნვა დებეტი', 'ბრუნვა კრედიტი', 'ნაშთი დღის ბოლოს',
        'დამატებითი ინფორმაცია', 'Curr', 'Company'
    ]

    native_currency_output_order = [
        'თარიღი', 'დანიშნულება', 'გამგზავნის/მიმღების დასახელება',
        'საწყისი ნაშთი', 'საწყისი ნაშთი ექვ ლარში', # GEL accounts also have base currency
        'დებეტი', 'კრედიტი', 'ოპერაციის შინაარსი', 'ოპერაციის ტიპი', 'თანხა',
        'ბრუნვა დებეტი', 'ბრუნვა კრედიტი', 'ნაშთი დღის ბოლოს',
        'დამატებითი ინფორმაცია', 'Curr', 'Company', 'Category', 'Rule Name', 'Status'
    ]


    for company_name in companies:
        _logger.info(f"Generating report for company: {company_name}")
        company_df = final_df_all_companies[final_df_all_companies['Company'] == company_name].copy()

        foreign_currency_df = company_df[company_df['Curr'] != 'GEL'].copy()
        native_currency_df = company_df[company_df['Curr'] == 'GEL'].copy()

        # Add the special "Opening Balance Adjustment" transaction for Foreign Currency for *this company*
        _logger.info(f"Adding special opening balance adjustment transaction for foreign currency accounts for {company_name}.")

        revaluation_rows = []
        company_daily_summary = daily_summary[daily_summary['company'] == company_name]

        for index, row_summary in company_daily_summary[company_daily_summary['currency'] != 'GEL'].iterrows():
            company = row_summary['company']
            account_number = row_summary['account_number']
            currency = row_summary['currency']
            opening_balance_fc = row_summary['opening_balance_fc']
            total_debit_fc = row_summary['total_debit_fc']
            total_credit_fc = row_summary['total_credit_fc']
            closing_balance_fc = row_summary['closing_balance_fc']

            current_nbg_rate = nbg_rates_df_current[nbg_rates_df_current['currency'] == currency]['rate_per_unit'].iloc[0] if \
                not nbg_rates_df_current[nbg_rates_df_current['currency'] == currency].empty else 0.0

            # Get previous day's NBG rate for this currency
            previous_nbg_rate = nbg_rates_df_previous[nbg_rates_df_previous['currency'] == currency]['rate_per_unit'].iloc[0] if \
                not nbg_rates_df_previous[nbg_rates_df_previous['currency'] == currency].empty else current_nbg_rate # Fallback to current if prev not found

            # Calculate GEL equivalent of opening balance using PREVIOUS day's rate
            opening_balance_gel_prev_day = opening_balance_fc * previous_nbg_rate

            # --- net_turnover_gel_from_transactions already uses the _base amounts due to earlier change ---
            net_turnover_gel_from_transactions = row_summary['total_credit_gel_eq'] - row_summary['total_debit_gel_eq']

            # --- REVALUATION CALCULATION using your precise formula ---
            revaluation_amount_gel = (closing_balance_fc * current_nbg_rate) - \
                                     (opening_balance_gel_prev_day + net_turnover_gel_from_transactions)

            if abs(revaluation_amount_gel) < 0.005: # Treat very small amounts as zero for reporting
                revaluation_amount_gel = 0.0

            # Determine if it's a debit or credit revaluation in GEL equivalent
            reval_debit_gel = 0.0
            reval_credit_gel = 0.0

            # For revaluation, FC Debit/Credit are 0, and 'თანხა' (Net FC Amount) is 0.
            # This is because the FC nominal value doesn't change due to revaluation, only its GEL equivalent.
            reval_debit_fc = 0.0
            reval_credit_fc = 0.0
            net_reval_fc_amount = 0.0 # Will be 0 for revaluation

            if revaluation_amount_gel < 0: # Loss (increase in GEL debit)
                reval_debit_gel = abs(revaluation_amount_gel)
            elif revaluation_amount_gel > 0: # Gain (increase in GEL credit)
                reval_credit_gel = revaluation_amount_gel

            revaluation_row = {
                'თარიღი': report_date_str,
                'დანიშნულება': 'სავალუტო ნაშთის გადაფასება', # Foreign currency balance revaluation
                'გამგზავნის/მიმღების დასახელება': 'სისტემური', # System
                'საწყისი ნაშთი': 0.0, # This is an adjustment line, not an opening balance itself
                'საწყისი ნაშთი ექვ ლარში': 0.0, # This is an adjustment line, not an opening balance itself
                'დებეტი': reval_debit_fc,       # FC Debit for revaluation is 0
                'დებეტი ექვ ლარში': reval_debit_gel, # This will be the GEL equivalent debit
                'კრედიტი': reval_credit_fc,     # FC Credit for revaluation is 0
                'კრედიტი ექვ ლარში': reval_credit_gel, # This will be the GEL equivalent credit
                'ოპერაციის შინაარსი': f"სავალუტო ნაშთის გადაფასება {currency} ანგარიშზე", # Balance revaluation on {currency} account
                'კურსი': current_nbg_rate, # The rate used for the *current* valuation
                'ოპერაციის ტიპი': 'გადაფასება', # Revaluation
                'თანხა': net_reval_fc_amount, # Net FC change for revaluation is 0
                'თანხა ექვ ლარში': revaluation_amount_gel, # The actual net GEL impact of the revaluation
                'ბრუნვა დებეტი': 0.0, # This is not part of normal turnover; it's an adjustment
                'ბრუნვა კრედიტი': 0.0, # This is not part of normal turnover; it's an adjustment
                'ნაშთი დღის ბოლოს': closing_balance_fc, # This should be the FC balance of the account (for reference)
                'დამატებითი ინფორმაცია': f"Account: {account_number}, Prev NBG Rate: {previous_nbg_rate:.4f}, Current NBG Rate: {current_nbg_rate:.4f}",
                'Curr': currency,
                'Company': company,
            }
            revaluation_rows.append(revaluation_row)

        if revaluation_rows:
            df_reval_transactions = pd.DataFrame(revaluation_rows)
            # Ensure df_reval_transactions has all foreign_currency_df columns before concat
            for col in foreign_currency_df.columns:
                if col not in df_reval_transactions.columns:
                    df_reval_transactions[col] = None

            foreign_currency_df = pd.concat([foreign_currency_df, df_reval_transactions], ignore_index=True)
            _logger.info(f"Added {len(revaluation_rows)} revaluation transactions to foreign currency report for {company_name}.")


        # Ensure all columns exist in the respective dataframes before reordering, fill missing with None
        for col in foreign_currency_output_order:
            if col not in foreign_currency_df.columns:
                foreign_currency_df[col] = None
        for col in native_currency_output_order:
            if col not in native_currency_df.columns:
                native_currency_df[col] = None

        # Apply the final column order
        foreign_currency_df = foreign_currency_df[foreign_currency_output_order]
        native_currency_df = native_currency_df[native_currency_output_order]

        # Define the output filename for the current company
        # Replacing spaces with underscores for cleaner filenames
        clean_company_name = company_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        report_filename = os.path.join(output_dir, f"Bank_Report_BOG_{clean_company_name}_{report_date_str}.xlsx")

        # Write to Excel
        with pd.ExcelWriter(report_filename, engine='xlsxwriter') as writer:
            foreign_currency_df.to_excel(writer, sheet_name='Foreign Currency BOG', index=False)
            native_currency_df.to_excel(writer, sheet_name='Native Currency BOG', index=False)
        _logger.info(f"Daily bank report for {company_name} saved to {report_filename}")
        print(f"Daily bank report for {company_name} on {report_date_str} successfully generated at {report_filename}")


if __name__ == '__main__':
    # Initialize the database (ensures success_log table exists and creates bank_data.db if not)
    initialize_db()

    # Get today's date for the report
    today = datetime.date.today()
    # Assuming you want the report for yesterday based on current date and time
    # This will be May 29, 2025
    yesterday = today - datetime.timedelta(days=1)
    report_date = yesterday.strftime('%Y-%m-%d')

    # Example usage: Generate report for yesterday
    print(f"Attempting to generate daily bank report for {report_date}...")
    generate_daily_bank_report(report_date)

    # You can also generate for a specific past date, e.g.:
    # Make sure you have transactions for this date in your bank_data.db
    # print("\nAttempting to generate daily bank report for 2024-05-29...")
    # generate_daily_bank_report('2024-05-29')