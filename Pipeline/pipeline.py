from db_connector import Database
from yfinance_fetcher import YFinanceFetcher
from AIS_processor import AISPortVisitProcessor
from yfinance_cleaner import YFinanceCleaner
import os
import pandas as pd
from datetime import datetime, timedelta

class Pipeline:
    def __init__(self, db_config):
        self.db = Database(**db_config)

    def fetch_and_save_yfinance_data(self, start_date, end_date, replace):
        """
        1) Fetch S&P 500 info and save to PostgreSQL
        2) Fetch S&P 500 index prices and save to PostgreSQL
        3) Fetch all S&P 500 tickers’ prices and save only the “Close” columns
        4) Fetch all S&P 500 financial statements and save to PostgreSQL
        """
        data_fetcher = YFinanceFetcher(start_date, end_date)

        try:
            # 1. Fetch S&P 500 info
            sp500_info_df = data_fetcher.get_sp500_info()
            self.db.save_to_postgres(sp500_info_df, "sp500_info", replace=True)
            print("S&P 500 info data saved successfully to PostgreSQL")

            # 2. Fetch S&P 500 index prices
            sp500_index_df = data_fetcher.get_ticker_price("^GSPC", start_date, end_date)
            print(f"Fetched S&P 500 index data from {start_date} to {end_date}")
            self.db.save_to_postgres(sp500_index_df, "macro_prices", replace)
            print("Macroeconomic data saved successfully to PostgreSQL")

            # 3. Fetch all S&P 500 tickers’ prices
            sp500_tickers = data_fetcher.get_sp500_tickers()
            sp500_prices_df = data_fetcher.get_ticker_price(sp500_tickers, start_date, end_date)
            print(sp500_prices_df.head())

           
            close_cols = [f"{ticker}_Close" for ticker in sp500_tickers]
            to_save = sp500_prices_df[["Date"] + close_cols]
            self.db.save_to_postgres(to_save, "sp500_prices", replace)
            print("S&P 500 prices (Close only) saved successfully to PostgreSQL")
            

            # 4. Fetch S&P 500 financial statements
            income_df, balance_df, cashflow_df = data_fetcher.get_sp500_statements(sp500_tickers)
            self.db.save_to_postgres(income_df, "sp500_income_statements", replace)
            self.db.save_to_postgres(balance_df, "sp500_balance_sheets", replace)
            self.db.save_to_postgres(cashflow_df, "sp500_cashflow_statements", replace)
            print("S&P 500 financial statements data saved successfully to PostgreSQL")

        except Exception as e:
            print(f"Error saving YFinance data to PostgreSQL: {e}")
            raise e
    
    def fetch_and_save_ais_data(self, start_date, end_date, save_folder, output_csv_path, replace):
        """
        Download AIS data in weekly chunks (year by year), process each chunk into a single CSV,
        then write each weekly chunk to Postgres (with replace only on the first chunk).
        Finally, delete all downloaded ZIPs.
        """
        processor = AISPortVisitProcessor()
        # Ensure directories exist
        os.makedirs(save_folder, exist_ok=True)
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

        # Parse input dates once
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")

        first_chunk = True
        for year in range(start_dt.year, end_dt.year + 1):
            # Determine the start/end of this year slice
            if year == start_dt.year:
                year_start = start_dt
            else:
                year_start = datetime(year, 1, 1)

            if year == end_dt.year:
                year_end = end_dt
            else:
                year_end = datetime(year, 12, 31)

            num_days = (year_end - year_start).days + 1
            num_weeks = (num_days // 7) + (1 if num_days % 7 > 0 else 0)
            print(f"Number of weeks to process for {year}: {num_weeks}")

            for week in range(num_weeks):
                week_start_dt = year_start + timedelta(weeks=week)
                week_end_dt = week_start_dt + timedelta(days=6)
                if week_end_dt > year_end:
                    week_end_dt = year_end

                week_start_str = week_start_dt.strftime("%Y-%m-%d")
                week_end_str = week_end_dt.strftime("%Y-%m-%d")

                print(f"==> AIS: Processing {week_start_str} through {week_end_str}")
                week_df = processor.run(week_start_str, week_end_str, save_folder, output_csv_path)
                print(f"AIS data fetched and processed successfully for {week_start_str} to {week_end_str}")

                try:
                    save_replace = replace if first_chunk else False
                    print(f"==> AIS: Saving data for {week_start_str} to PostgreSQL (replace={save_replace})")
                    self.db.save_to_postgres(week_df, "ais_port_visits", save_replace)
                    first_chunk = False
                except Exception as e:
                    print(f"Error saving AIS data for {week_start_str} to PostgreSQL: {e}")

        processor.delete_zips(save_folder)
        print("All AIS ZIPs deleted.")

    def load_data(self, table_name):
        print(f"Loading data from PostgreSQL table: {table_name}")
        data = self.db.fetch_from_postgres(table_name)
        print(f"Data loaded successfully from {table_name} (records: {len(data) if hasattr(data, 'shape') else 'unknown'})")
        return data

    def fetch_and_save_locally(self, start_date, end_date, save_folder):
        """
        1) Download & process AIS port visits into a local CSV.
        2) Download and save all YFinance data locally into CSVs under save_folder.
        """
        processor = AISPortVisitProcessor(buffer_degrees=1)
        # Ensure folder exists
        os.makedirs(save_folder, exist_ok=True)

        # 1. AIS locally
        visits_df = processor.run(start_date, end_date, save_folder, save_folder)
        visits_path = os.path.join(save_folder, "port_visits.csv")
        visits_df.to_csv(visits_path, index=False)
        print(f"Port visits saved to {visits_path} (records: {len(visits_df)})")
        processor.delete_zips(save_folder)

        # 2. YFinance locally
        data_fetcher = YFinanceFetcher(start_date=start_date, end_date=end_date)
        try:
            # S&P 500 info
            sp500_info_df = data_fetcher.get_sp500_info()
            info_path = os.path.join(save_folder, "sp500_info.csv")
            sp500_info_df.to_csv(info_path, index=False)
            print(f"S&P 500 info saved to {info_path}")

            # S&P 500 index prices
            macro_prices_df = data_fetcher.get_ticker_price("^GSPC", start_date, end_date)
            macro_path = os.path.join(save_folder, "macro_prices.csv")
            macro_prices_df.to_csv(macro_path, index=False)
            print(f"Macro prices saved to {macro_path}")

            # All S&P 500 tickers’ prices
            sp500_tickers = data_fetcher.get_sp500_tickers()
            sp500_prices_df = data_fetcher.get_ticker_price(sp500_tickers, start_date, end_date)
            sp500_prices_path = os.path.join(save_folder, "sp500_prices.csv")
            sp500_prices_df.to_csv(sp500_prices_path, index=False)
            print(f"S&P 500 prices saved to {sp500_prices_path}")

            # All S&P 500 financial statements
            income_df, balance_df, cashflow_df = data_fetcher.get_sp500_statements(sp500_tickers)
            income_path = os.path.join(save_folder, "sp500_income_statements.csv")
            balance_path = os.path.join(save_folder, "sp500_balance_sheets.csv")
            cashflow_path = os.path.join(save_folder, "sp500_cashflow_statements.csv")
            income_df.to_csv(income_path, index=False)
            balance_df.to_csv(balance_path, index=False)
            cashflow_df.to_csv(cashflow_path, index=False)
            print("All S&P 500 statements saved locally.")

        except Exception as e:
            print(f"Error saving YFinance data locally: {e}")

    def __concat_yfinance_data(self, statements, prices, info):
        """
        Concatenate YFinance data into a single DataFrame.
        statements: list of DataFrames containing financial statements
        prices: list of DataFrames containing prices
        info: DataFrame containing S&P 500 info
        Returns a single DataFrame with all data combined.
        """
        # initialize yfinance_cleaner
        income = statements[0]
        balance = statements[1]
        cashflow = statements[2]
        cleaner = YFinanceCleaner(info,income,balance,cashflow,prices[0],prices[1])
        df = cleaner.run()
        return df

    def load_and_concat_yfinance_data(self):
        # Load data from PostgreSQL
        try:
            income = self.load_data("sp500_income_statements")
            balance = self.load_data("sp500_balance_sheets")
            cashflow = self.load_data("sp500_cashflow_statements")
            prices = self.load_data("sp500_prices")
            info = self.load_data("sp500_info")
            macro_prices = self.load_data("macro_prices")
            print("Data loaded successfully from PostgreSQL")
        except Exception as e:
            print(f"Error loading data from PostgreSQL: {e}")
            import traceback
            traceback.print_exc()

        # Concatenate YFinance data
        try:
            statements = [income, balance, cashflow]
            print(macro_prices.head())
            prices_list = [prices, macro_prices]
            df = pipeline.__concat_yfinance_data(statements, prices_list, info)
            print("YFinance data concatenated successfully")
            return df
        except Exception as e:
            print(f"Error concatenating YFinance data: {e}")
            import traceback
            traceback.print_exc()
            return None

if __name__ == "__main__":
    db_config = {
        "user": "ais_sp500_db_user",
        "password": "lFmNQ7l0yiXhSc1Bet3OHw8EuUeDuv7F",
        "host": "dpg-d0rflvndiees73c05ea0-a.oregon-postgres.render.com",
        "port": 5432,
        "database": "ais_sp500_db"
    }
    pipeline = Pipeline(db_config)
    
    # start_end_tuples = [
    #     ("2020-01-01", "2020-07-01"),
    #     ("2021-01-01", "2021-07-01"),
    #     ("2024-01-01", "2024-07-01"),
    #     ("2025-01-01", "2025-07-01")
    # ]
    # save_folder = "../assets/ais_data"
    # output_csv = "../assets/ais_data/port_visits_first_arrivals.csv"
    # for i in range(len(start_end_tuples)):
    #     start_date, end_date = start_end_tuples[i]
    #     replace = True if i == 0 else False
    #     try:
    #         pipeline.fetch_and_save_yfinance_data(start_date, end_date, replace)
    #         pipeline.fetch_and_save_ais_data(start_date, end_date, save_folder, output_csv, replace)
    #     except Exception as e:
    #         import traceback
    #         traceback.print_exc()

    # # Load and concatenate YFinance data
    # concatenated_df = pipeline.load_and_concat_yfinance_data()

    # Load AIS data
    ais_data = pipeline.load_data("ais_port_visits")
    ais_data.to_csv("ais_port_visits.csv", index=False)

    
    