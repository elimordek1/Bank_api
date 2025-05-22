import logging
from logging.handlers import RotatingFileHandler
from bank_api.api.bog_api import get_all_transactions as get_bog_transactions, write_transactions_to_sqlite as write_bog_to_sqlite
from bank_api.api.tbc_api import get_all_transactions as get_tbc_transactions, write_transactions_to_sqlite as write_tbc_to_sqlite
from bank_api.data.get_currency_daily import get_daily_currency_rates, write_rates_to_sqlite

def setup_logger():
    logger = logging.getLogger("bank_api_etl")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler("etl.log", maxBytes=2*1024*1024, backupCount=3)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(formatter)
    if not logger.hasHandlers():
        logger.addHandler(handler)
    return logger

def main():
    logger = setup_logger()
    logger.info("Starting ETL process")
    start_date = '2025-04-04'
    end_date = '2025-05-05'
    try:
        logger.info(f"Fetching BOG transactions from {start_date} to {end_date}")
        bog_df = get_bog_transactions(start_date, end_date)
        logger.info(f"Fetched {len(bog_df)} BOG transactions")
        write_bog_to_sqlite(bog_df)
        logger.info("BOG transactions written to SQLite")
    except Exception as e:
        logger.error(f"Error processing BOG transactions: {e}", exc_info=True)
    try:
        logger.info(f"Fetching TBC transactions from {start_date} to {end_date}")
        tbc_df = get_tbc_transactions(start_date, end_date)
        logger.info(f"Fetched {len(tbc_df)} TBC transactions")
        write_tbc_to_sqlite(tbc_df)
        logger.info("TBC transactions written to SQLite")
    except Exception as e:
        logger.error(f"Error processing TBC transactions: {e}", exc_info=True)
    try:
        currencies = ['USD', 'EUR', 'GBP', 'TRY']
        logger.info(f"Fetching daily currency rates for {currencies}")
        rates_df = get_daily_currency_rates(currencies)
        logger.info(f"Fetched {len(rates_df)} currency rate records")
        write_rates_to_sqlite(rates_df)
        logger.info("Currency rates written to SQLite")
    except Exception as e:
        logger.error(f"Error processing currency rates: {e}", exc_info=True)
    logger.info("ETL complete. Data written to bank_data.db")
    # Print and export TLS certificate checklist for TBC
    try:
        from bank_api.api.tbc_api import print_tls_certificate_checklist, export_tls_certificate_checklist_csv, export_tls_certificate_checklist_json, log_tls_certificate_checklist, create_missing_cert_folders
        print_tls_certificate_checklist()
        export_tls_certificate_checklist_csv()
        export_tls_certificate_checklist_json()
        log_tls_certificate_checklist(logger)
        create_missing_cert_folders()
        logger.info("Exported TLS certificate checklist as CSV and JSON. Created missing certificate folders if needed.")
    except Exception as e:
        logger.warning(f"Could not print/export TLS certificate checklist or create folders: {e}")

if __name__ == "__main__":
    main()
