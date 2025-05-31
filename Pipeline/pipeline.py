from db_connector import Database
from yfinance_fetcher import YFinanceFetcher
from AIS_processor import AISPortVisitProcessor
import os
import pandas as pd

class Pipeline:
    def __init__(self, db_config):
        self.db = Database(**db_config)

    def fetch_and_save_yfinance_data(self, start_date, end_date):
        #1 Fetch Macroeconomic data
        data_fetcher = YFinanceFetcher(start_date, end_date)
        try:
            #1 Fetch S&P 500 info and save to PostgreSQL
            data = data_fetcher.get_sp500_info()
            self.db.save_to_postgres(data, "sp500_info")
            print("S&P 500 info data saved successfully to PostgreSQL")
            #2 Fetch S&P 500 prices and save to PostgreSQL
            data = data_fetcher.get_ticker_price("^GSPC", start_date, end_date)
            print(f"Fetched S&P 500 data from {start_date} to {end_date}")
            self.db.save_to_postgres(data,"macro_prices")
            print("Macroeconomic data saved successfully to PostgreSQL")
            #3 Fetch S&P 500 tickers and prices, save to PostgreSQL
            sp500_tickers = data_fetcher.get_sp500_tickers()
            data = data_fetcher.get_ticker_price(sp500_tickers, start_date, end_date)
            print(data.head())
            self.db.save_to_postgres(data, "sp500_prices")
            print("S&P 500 prices data saved successfully to PostgreSQL")
            #4 Fetch S&P 500 financial statements and save to PostgreSQL
            income_df, balance_df, cashflow_df = data_fetcher.get_sp500_statements(sp500_tickers)
            self.db.save_to_postgres(income_df,  "sp500_income_statements")
            self.db.save_to_postgres(balance_df, "sp500_balance_sheets")
            self.db.save_to_postgres(cashflow_df,"sp500_cashflow_statements")
            print("Data saved successfully to PostgreSQL")
        except Exception as e:
            print(f"Error saving data to PostgreSQL: {e}")

    def fetch_and_save_ais_data(self, start_date, end_date, save_folder, output_csv_path):
        """
        Download AIS data in weekly chunks, clean it, save one CSV per chunk
        (always to `output_csv_path`), then concatenate them all into `data`.
        Finally, write `data` to PostgreSQL.
        """

        processor = AISPortVisitProcessor()
        data = pd.DataFrame()

        # 1) Make sure the base directory for ZIPs and CSVs exists:
        os.makedirs(save_folder, exist_ok=True)
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)

        # 2) If the dates span multiple years, break them up year by year.
        start_year = int(start_date.split("-")[0])
        end_year   = int(end_date.split("-")[0])

        if start_year != end_year:
            # Loop over each year in the range [start_year .. end_year], inclusive.
            # For the very first year, run from `start_date` to `YYYY-12-31`.
            # For each middle year, run `YYYY-01-01` to `YYYY-12-31`.
            # For the final year, run from `YYYY-01-01` to `end_date`.
            #
            # In each year, we will still process week-by-week inside AIS_processor.run(),
            # but here we call run(...) once per-year; it already loops weekly internally.
            cur_start = start_date
            for year in range(start_year, end_year + 1):
                if year == start_year:
                    cur_end = f"{year}-12-31"
                elif year == end_year:
                    cur_start = f"{year}-01-01"
                    cur_end   = end_date
                else:
                    cur_start = f"{year}-01-01"
                    cur_end   = f"{year}-12-31"

                print(f"==> AIS: Processing {cur_start} through {cur_end}")

                # Call run(...) exactly once per year, passing the single CSV path:
                year_df = processor.run(cur_start, cur_end, save_folder, output_csv_path)
                print(f"AIS data fetched and processed successfully for {cur_start} to {cur_end}")

                # Accumulate
                data = pd.concat([data, year_df], ignore_index=True)

        else:
            # Single‐year case: just call run() once.
            print(f"==> AIS: Processing single year {start_year}")

            data = processor.run(start_date, end_date, save_folder, output_csv_path)
            print("AIS data fetched and processed successfully")

        # Remove any leftover ZIPs
        processor.delete_zips(save_folder)

        # 3) At this point, `data` is the concatenation of every year‐chunk’s cleaned DataFrame.
        #    Save that to Postgres.
        try:
            self.db.save_to_postgres(data, "ais_port_visits")
            print("AIS data saved successfully to PostgreSQL")
        except Exception as e:
            print(f"Error saving AIS data to PostgreSQL: {e}")
            
    def load_data(self, table_name):
        print(f"Loading data from PostgreSQL table: {table_name}")
        data = self.db.fetch_from_postgres(table_name)
        print(f"Data loaded successfully from {table_name}")
        return data

    def fetch_and_save_locally(self, start_date, end_date, save_folder):
        
        processor = AISPortVisitProcessor(buffer_degrees=1)
        visits_df = processor.run(start_date, end_date, save_folder, save_folder)
        visits_df.to_csv("port_visits", index=False)
        print(f"Port visits saved to {save_folder} (records: {len(visits_df)})")
        # uncomment to delete the downloaded zips
        processor.delete_zips(save_folder)

        data_fetcher = YFinanceFetcher(start_date=start_date, end_date=end_date)
        sp500_tickers = data_fetcher.get_sp500_tickers()
        data = data_fetcher.get_ticker_price("^GSPC", start_date, end_date)
        try:
            data = data_fetcher.get_sp500_info()
            data.to_csv("sp500_info.csv", index=False)
            sp500_tickers = data_fetcher.get_sp500_tickers()
            print(f"Fetched S&P 500 data from {start_date} to {end_date}")
            data = data_fetcher.get_ticker_price("^GSPC", start_date, end_date)
            data.to_csv("macro_prices", index=False)
            data = data_fetcher.get_ticker_price(sp500_tickers, start_date, end_date)
            data.to_csv("sp500_prices", index=False)
            data = data_fetcher.get_sp500_statements(sp500_tickers)
            data.to_csv("sp500_statements", index=False)
            print("Data saved successfully to PostgreSQL")
        except Exception as e:
            print(f"Error saving data to PostgreSQL: {e}")



if __name__ == "__main__":
    
    db_config = {
        "user": "ais_sp500_db_user",
        "password": "lFmNQ7l0yiXhSc1Bet3OHw8EuUeDuv7F",
        "host": "dpg-d0rflvndiees73c05ea0-a.oregon-postgres.render.com",
        "port": 5432,
        "database": "ais_sp500_db"
    }
    pipeline = Pipeline(db_config)
    start_date = "2020-01-11"
    end_date = "2020-01-12"
    save_folder = "../assets/ais_data"
    output_csv = "../assets/ais_data/port_visits_first_arrivals.csv"
    
    try:
        pipeline.fetch_and_save_yfinance_data(start_date, end_date)
    except Exception as e:
        import traceback
        traceback.print_exc()
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    data = pipeline.load_data("sp500_income_statements")
    print("Columns:", data.columns)
    print(data.head())

    
    try: 
        pipeline.fetch_and_save_ais_data(start_date, end_date, save_folder, output_csv)
    except Exception as e:
        import traceback
        traceback.print_exc()
    
    data = pipeline.load_data("ais_port_visits")
    print(data.head())
    data = None