import os
import pandas as pd
import sqlite3
import logging
from datetime import datetime

# Import our NBG rates module
from src.nbg_fx_rates import get_nbg_rate, ensure_rates_for_date

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
    # Add more direct mappings as needed
}

BOG_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bank_data.db')
BOG_TABLE_NAME = 'bog_transactions'

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

TBC_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'bank_data.db')
TBC_TABLE_NAME = 'tbc_transactions'

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output')

# Replace the stub function with a wrapper that handles date conversion
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
    
    # Convert date to date object if needed
    if isinstance(date, str):
        try:
            date_obj = datetime.strptime(date[:10], '%Y-%m-%d').date()
        except:
            _logger.error(f"Invalid date format: {date}")
            return None
    elif isinstance(date, datetime):
        date_obj = date.date()
    else:
        date_obj = date
    
    # Ensure rates exist for this date
    ensure_rates_for_date(date_obj)
    
    # Import the function from our module to avoid naming conflict
    from src.nbg_fx_rates import get_nbg_rate as get_rate_from_db
    return get_rate_from_db(currency, date_obj)

# --- BOG ---
def read_bog_transactions():
    with sqlite3.connect(BOG_DB_PATH) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {BOG_TABLE_NAME}", conn)
    if 'entry_date' in df.columns:
        df['entry_date'] = pd.to_datetime(df['entry_date']).dt.date
    return df

def map_and_transform_bog(df):
    mapping = BOG_DB_TO_REPORT
    result = pd.DataFrame()
    # Preserve raw data for later calculations
    result['_raw_account_number'] = df['account_number'] if 'account_number' in df.columns else ''
    result['_raw_currency'] = df['currency'] if 'currency' in df.columns else ''
    result['_raw_date'] = df['entry_date'] if 'entry_date' in df.columns else ''
    result['_raw_company'] = df['company'] if 'company' in df.columns else ''
    
    for src, tgt in mapping.items():  # src is DB field, tgt is output column
        if tgt == 'კომპანია':
            result[tgt] = df['company'] if 'company' in df.columns else ''
        elif tgt in ["კატეგორია", "წესი", "სტატუსი"]:
            result[tgt] = ''
        elif tgt == "ბრუნვა დებეტი":
            result[tgt] = df.groupby(['company', 'entry_date', 'currency', 'account_number'])['entry_amount_debit'].transform('sum') if not df.empty and 'entry_amount_debit' in df.columns else ''
        elif tgt == "ბრუნვა კრედიტი":
            result[tgt] = df.groupby(['company', 'entry_date', 'currency', 'account_number'])['entry_amount_credit'].transform('sum') if not df.empty and 'entry_amount_credit' in df.columns else ''
        elif tgt == "დებეტი ექვ ლარში" and src == "entry_amount_debit_base":
            result[tgt] = df[src] if src in df.columns else ''
        elif tgt == "კრედიტი ექვ ლარში" and src == "entry_amount_credit_base":
            result[tgt] = df[src] if src in df.columns else ''
        elif tgt == "თანხა":
            if src in df.columns:
                result[tgt] = df[src]
            else:
                result[tgt] = df['entry_amount_debit'].fillna(0) + df['entry_amount_credit'].fillna(0) if not df.empty and 'entry_amount_debit' in df.columns and 'entry_amount_credit' in df.columns else ''
        elif tgt == "თანხა ექვ ლარში":
            if 'თანხა' in result.columns and not result.empty:
                result[tgt] = result["თანხა"] * df.apply(lambda row: get_nbg_rate(row['currency'], row['entry_date']) or 0, axis=1) if 'currency' in df.columns and 'entry_date' in df.columns else ''
            else:
                result[tgt] = ''
        elif tgt == "კურსი":
            result[tgt] = df.apply(lambda row: get_nbg_rate(row['currency'], row['entry_date']), axis=1) if not df.empty and 'currency' in df.columns and 'entry_date' in df.columns else ''
        elif tgt == "საწყისი ნაშთი ექვ ლარში":
            result[tgt] = df.apply(lambda row: (row['opening_balance'] or 0) * (get_nbg_rate(row['currency'], row['entry_date']) or 0), axis=1) if not df.empty and 'opening_balance' in df.columns and 'currency' in df.columns and 'entry_date' in df.columns else ''
        else:
            # For direct mappings
            result[tgt] = df[src] if src in df.columns else ''
    
    # Add calculated columns that aren't in the mapping
    for tgt in ["კატეგორია", "წესი", "სტატუსი", "ბრუნვა დებეტი", "ბრუნვა კრედიტი", "თანხა ექვ ლარში", "კურსი", "საწყისი ნაშთი ექვ ლარში"]:
        if tgt not in result.columns:
            result[tgt] = ''
    
    return result

def read_tbc_transactions():
    with sqlite3.connect(TBC_DB_PATH) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {TBC_TABLE_NAME}", conn)
    # Normalize date field for mapping
    if 'valueDate' in df.columns:
        df['valueDate'] = pd.to_datetime(df['valueDate']).dt.date
    return df

def map_and_transform_tbc(df):
    mapping = TBC_DB_TO_REPORT
    result = pd.DataFrame()
    # Preserve raw data for later calculations
    result['_raw_account_number'] = df['account_number'] if 'account_number' in df.columns else ''
    result['_raw_currency'] = df['currency'] if 'currency' in df.columns else ''
    result['_raw_date'] = df['valueDate'] if 'valueDate' in df.columns else ''
    result['_raw_company'] = df['company'] if 'company' in df.columns else ''
    
    for src, tgt in mapping.items():  # src is DB field, tgt is output column
        if tgt == 'კომპანია':
            result[tgt] = df['company'] if 'company' in df.columns else ''
        elif tgt == 'კურსი':
            if 'exchangeRate' in df.columns:
                result[tgt] = df['exchangeRate']
            else:
                result[tgt] = df.apply(lambda row: get_nbg_rate(row.get('currency', ''), row.get('valueDate', '')), axis=1) if not df.empty else ''
        elif tgt == 'გასული თანხა':
            result[tgt] = df.apply(lambda row: row['amount'] if str(row.get('debitCredit', '')) == '1' else '', axis=1) if not df.empty and 'amount' in df.columns and 'debitCredit' in df.columns else ''
        elif tgt == 'გასული თანხა ექვ.':
            result[tgt] = df.apply(lambda row: (row['amount'] * (get_nbg_rate(row.get('currency', ''), row.get('valueDate', '')) or 0)) if str(row.get('debitCredit', '')) == '1' else '', axis=1) if not df.empty and 'amount' in df.columns and 'debitCredit' in df.columns else ''
        elif tgt == 'შემოსული თანხა':
            result[tgt] = df.apply(lambda row: row['amount'] if str(row.get('debitCredit', '')) == '0' else '', axis=1) if not df.empty and 'amount' in df.columns and 'debitCredit' in df.columns else ''
        elif tgt == 'შემოსული თანხა ექვ.':
            result[tgt] = df.apply(lambda row: (row['amount'] * (get_nbg_rate(row.get('currency', ''), row.get('valueDate', '')) or 0)) if str(row.get('debitCredit', '')) == '0' else '', axis=1) if not df.empty and 'amount' in df.columns and 'debitCredit' in df.columns else ''
        elif src == 'debitCredit':
            # Skip this field as it's used for calculations only
            continue
        else:
            # For direct mappings
            result[tgt] = df[src] if src in df.columns else ''
    
    # Add calculated columns that might not be in the result yet
    for tgt in ['გასული თანხა', 'გასული თანხა ექვ.', 'შემოსული თანხა', 'შემოსული თანხა ექვ.', 'კურსი']:
        if tgt not in result.columns:
            result[tgt] = ''
    
    return result

def get_superset_columns():
    # Union of all columns from both mappings, preserving order: BOG first, then TBC extras
    bog_cols = list(BOG_DB_TO_REPORT.values())  # Get output column names
    tbc_cols = list(TBC_DB_TO_REPORT.values())  # Get output column names
    
    # Also add calculated columns that aren't in the direct mappings
    bog_calculated_cols = ["კატეგორია", "წესი", "სტატუსი", "ბრუნვა დებეტი", "ბრუნვა კრედიტი", "თანხა ექვ ლარში", "კურსი", "საწყისი ნაშთი ექვ ლარში"]
    tbc_calculated_cols = ['გასული თანხა', 'გასული თანხა ექვ.', 'შემოსული თანხა', 'შემოსული თანხა ექვ.', 'კურსი']
    
    superset = []
    # Add BOG columns first
    for col in bog_cols:
        if col not in superset:
            superset.append(col)
    # Add BOG calculated columns
    for col in bog_calculated_cols:
        if col not in superset:
            superset.append(col)
    # Add TBC columns
    for col in tbc_cols:
        if col not in superset:
            superset.append(col)
    # Add TBC calculated columns
    for col in tbc_calculated_cols:
        if col not in superset:
            superset.append(col)
    return superset

def add_foreign_currency_summary_rows(df, bank_type='BOG'):
    """
    Add summary rows for foreign currency accounts to calculate exchange rate differences.
    Formula: (Opening balance + Turnover(credit-debit)) * NBG Rate - Opening balance in GEL equivalent + Turnover
    """
    if df.empty:
        return df
    
    # Check if we have the raw columns
    if '_raw_currency' not in df.columns or '_raw_account_number' not in df.columns:
        _logger.warning("Raw columns not found in dataframe, cannot add summary rows")
        return df
    
    # Filter for foreign currency only
    foreign_df = df[df['_raw_currency'] != 'GEL'].copy()
    if foreign_df.empty:
        return df
    
    # Group by company, account_number, currency, date
    groups = foreign_df.groupby(['_raw_company', '_raw_account_number', '_raw_currency', '_raw_date'])
    
    summary_rows = []
    
    for (company, account, currency, date), group in groups:
        try:
            # Calculate values for the group
            # Get opening and closing balance (should be same for all rows in group)
            if bank_type == 'BOG':
                opening_balance = pd.to_numeric(group['საწყისი ნაშთი'].iloc[0], errors='coerce') if 'საწყისი ნაშთი' in group.columns else 0
                closing_balance = pd.to_numeric(group['ნაშთი დღის ბოლოს'].iloc[0], errors='coerce') if 'ნაშთი დღის ბოლოს' in group.columns else 0
                opening_balance_gel = pd.to_numeric(group['საწყისი ნაშთი ექვ ლარში'].iloc[0], errors='coerce') if 'საწყისი ნაშთი ექვ ლარში' in group.columns else 0
            else:  # TBC
                # TBC doesn't have separate opening balance, calculate from closing balance and turnover
                closing_balance = pd.to_numeric(group['ნაშთი'].iloc[0], errors='coerce') if 'ნაშთი' in group.columns else 0
                # For TBC, we'll need to calculate opening balance from transactions
                opening_balance = closing_balance  # Placeholder, will adjust with turnover
                opening_balance_gel = 0  # TBC doesn't provide this directly
            
            # Calculate turnover
            if bank_type == 'BOG':
                total_debit = pd.to_numeric(group['დებეტი'], errors='coerce').sum()
                total_credit = pd.to_numeric(group['კრედიტი'], errors='coerce').sum()
                turnover = total_credit - total_debit
            else:  # TBC
                # For TBC, use გასული თანხა (debit) and შემოსული თანხა (credit)
                total_debit = pd.to_numeric(group['გასული თანხა'], errors='coerce').sum()
                total_credit = pd.to_numeric(group['შემოსული თანხა'], errors='coerce').sum()
                turnover = total_credit - total_debit
                # Adjust opening balance for TBC
                opening_balance = closing_balance - turnover
            
            # Get NBG rate
            nbg_rate = get_nbg_rate(currency, date) or 0
            
            # Calculate exchange difference using the formula
            # (Opening balance + Turnover) * NBG Rate - Opening balance in GEL equivalent + Turnover
            exchange_diff = (opening_balance + turnover) * nbg_rate - opening_balance_gel + turnover
            
            # Create summary row
            summary_row = pd.Series()
            
            # Copy the structure from the first row of the group
            for col in df.columns:
                if col.startswith('_raw_'):
                    summary_row[col] = group[col].iloc[0]
                else:
                    summary_row[col] = ''
            
            # Fill in specific values
            summary_row['კომპანია'] = company
            summary_row['ვალუტა'] = currency
            summary_row['თარიღი'] = date
            summary_row['დანიშნულება'] = f'სავალუტო სხვაობა - {account}'
            summary_row['ოპერაციის შინაარსი'] = 'კურსთაშორისი სხვაობა'
            summary_row['ანგარიშის ნომერი'] = account
            summary_row['თანხა ექვ ლარში'] = exchange_diff
            summary_row['კურსი'] = nbg_rate
            
            summary_rows.append(summary_row)
            
        except Exception as e:
            _logger.warning(f"Error calculating summary for {company}-{account}-{currency}-{date}: {e}")
            continue
    
    if summary_rows:
        # Combine original dataframe with summary rows
        summary_df = pd.DataFrame(summary_rows)
        result = pd.concat([df, summary_df], ignore_index=True)
        return result
    
    return df

SUPERSET_COLUMNS = get_superset_columns()

# Harmonize DataFrame to superset columns and order
def harmonize_df(df, superset, bank_name=None):
    df_h = df.copy()
    # Remove temporary raw columns before harmonizing
    raw_cols = [col for col in df_h.columns if col.startswith('_raw_')]
    df_h = df_h.drop(columns=raw_cols, errors='ignore')
    
    for col in superset:
        if col not in df_h.columns:
            df_h[col] = ''
    df_h = df_h[superset]
    if bank_name is not None:
        df_h['bank'] = bank_name
    return df_h

# --- WRITER ---
def write_excel(company, date, bog_gel, bog_other, tbc_gel, tbc_other):
    company_dir = os.path.join(OUTPUT_DIR, company)
    os.makedirs(company_dir, exist_ok=True)
    file_path = os.path.join(company_dir, f"Report_{company}_{date}.xlsx")
    
    # Apply foreign currency summary rows to non-GEL dataframes
    bog_other_with_summary = add_foreign_currency_summary_rows(bog_other, bank_type='BOG')
    tbc_other_with_summary = add_foreign_currency_summary_rows(tbc_other, bank_type='TBC')
    
    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        # Get the workbook and add formats
        workbook = writer.book
        summary_format = workbook.add_format({
            'bold': True,
            'bg_color': '#FFFF99',  # Light yellow background
            'border': 1
        })
        
        # Write BOG GEL sheet
        bog_gel_h = harmonize_df(bog_gel, SUPERSET_COLUMNS)
        bog_gel_h.to_excel(writer, sheet_name='bog_gel', index=False)
        
        # Write BOG Other with summary rows
        bog_other_h = harmonize_df(bog_other_with_summary, SUPERSET_COLUMNS)
        bog_other_h.to_excel(writer, sheet_name='bog_other', index=False)
        worksheet = writer.sheets['bog_other']
        # Format summary rows
        for idx, row in bog_other_with_summary.iterrows():
            if 'სავალუტო სხვაობა' in str(row.get('დანიშნულება', '')):
                worksheet.set_row(idx + 1, None, summary_format)  # +1 for header row
        
        # Write TBC GEL sheet
        tbc_gel_h = harmonize_df(tbc_gel, SUPERSET_COLUMNS)
        tbc_gel_h.to_excel(writer, sheet_name='tbc_gel', index=False)
        
        # Write TBC Other with summary rows
        tbc_other_h = harmonize_df(tbc_other_with_summary, SUPERSET_COLUMNS)
        tbc_other_h.to_excel(writer, sheet_name='tbc_other', index=False)
        worksheet = writer.sheets['tbc_other']
        # Format summary rows
        for idx, row in tbc_other_with_summary.iterrows():
            if 'სავალუტო სხვაობა' in str(row.get('დანიშნულება', '')):
                worksheet.set_row(idx + 1, None, summary_format)  # +1 for header row
        
        # Combined sheet
        combined = []
        if not bog_gel.empty:
            combined.append(harmonize_df(bog_gel, SUPERSET_COLUMNS, bank_name='BOG'))
        if not bog_other_with_summary.empty:
            combined.append(harmonize_df(bog_other_with_summary, SUPERSET_COLUMNS, bank_name='BOG'))
        if not tbc_gel.empty:
            combined.append(harmonize_df(tbc_gel, SUPERSET_COLUMNS, bank_name='TBC'))
        if not tbc_other_with_summary.empty:
            combined.append(harmonize_df(tbc_other_with_summary, SUPERSET_COLUMNS, bank_name='TBC'))
        if combined:
            combined_df = pd.concat(combined, ignore_index=True)
            # Ensure 'bank' is last column for clarity
            cols = [c for c in SUPERSET_COLUMNS if c != 'bank'] + ['bank']
            combined_df = combined_df[cols]
            combined_df.to_excel(writer, sheet_name='all_banks_combined', index=False)
            
            # Format summary rows in combined sheet
            worksheet = writer.sheets['all_banks_combined']
            for idx, row in combined_df.iterrows():
                if 'სავალუტო სხვაობა' in str(row.get('დანიშნულება', '')):
                    worksheet.set_row(idx + 1, None, summary_format)  # +1 for header row
    
    _logger.info(f"Wrote report: {file_path}")

# --- MAIN ---
def main():
    bog_df = read_bog_transactions()
    bog_df = map_and_transform_bog(bog_df) if not bog_df.empty else pd.DataFrame()
    tbc_df = read_tbc_transactions()
    tbc_df = map_and_transform_tbc(tbc_df) if not tbc_df.empty else pd.DataFrame()

    # Ensure 'კომპანია' column exists
    if 'კომპანია' not in bog_df.columns:
        bog_df['კომპანია'] = ''
    if 'კომპანია' not in tbc_df.columns:
        tbc_df['კომპანია'] = ''

    companies = set(bog_df['კომპანია'].dropna().unique()) | set(tbc_df['კომპანია'].dropna().unique())
    for company in companies:
        bog_c = bog_df[bog_df['კომპანია'] == company] if not bog_df.empty else pd.DataFrame(columns=bog_df.columns)
        tbc_c = tbc_df[tbc_df['კომპანია'] == company] if not tbc_df.empty else pd.DataFrame(columns=tbc_df.columns)
        
        # Ensure 'თარიღი' column exists before accessing it
        if 'თარიღი' not in bog_c.columns:
            bog_c['თარიღი'] = ''
        if 'თარიღი' not in tbc_c.columns:
            tbc_c['თარიღი'] = ''
            
        dates = set(bog_c['თარიღი'].dropna().unique()) | set(tbc_c['თარიღი'].dropna().unique())
        for date in dates:
            bog_date = bog_c[bog_c['თარიღი'] == date] if not bog_c.empty else pd.DataFrame(columns=bog_c.columns)
            tbc_date = tbc_c[tbc_c['თარიღი'] == date] if not tbc_c.empty else pd.DataFrame(columns=tbc_c.columns)
            
            # Ensure 'ვალუტა' column exists before filtering
            if 'ვალუტა' not in bog_date.columns:
                bog_date['ვალუტა'] = ''
            if 'ვალუტა' not in tbc_date.columns:
                tbc_date['ვალუტა'] = ''
                
            bog_gel = bog_date[bog_date['ვალუტა'] == 'GEL'] if not bog_date.empty else pd.DataFrame(columns=bog_date.columns)
            bog_other = bog_date[bog_date['ვალუტა'] != 'GEL'] if not bog_date.empty else pd.DataFrame(columns=bog_date.columns)
            tbc_gel = tbc_date[tbc_date['ვალუტა'] == 'GEL'] if not tbc_date.empty else pd.DataFrame(columns=tbc_date.columns)
            tbc_other = tbc_date[tbc_date['ვალუტა'] != 'GEL'] if not tbc_date.empty else pd.DataFrame(columns=tbc_date.columns)
            write_excel(company, date, bog_gel, bog_other, tbc_gel, tbc_other)

if __name__ == '__main__':
    main() 