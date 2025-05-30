from AIS.Pipeline.db_connector import Database
from AIS.Pipeline.yfinance_fetcher import YFinanceFetcher
from AIS.Pipeline.AIS_processor import AISPortVisitProcessor
import os

class Pipeline:
    def __init__(self, db_config):
        self.db = Database(**db_config)

    def fetch_and_save_yfinance_data(self, start_date, end_date):
        #1 Fetch Macroeconomic data
        data_fetcher = YFinanceFetcher(start_date, end_date)
        #2 Save data to PostgreSQL
        try:   
            sp500_tickers = data_fetcher.get_sp500_tickers()
            print(f"Fetched S&P 500 data from {start_date} to {end_date}")
            data = data_fetcher.get_ticker_price("^GSPC", start_date, end_date)
            self.db.save_to_postgres(data,"macro_prices")
            data = data_fetcher.get_ticker_price(sp500_tickers, start_date, end_date)
            self.db.save_to_postgres(data, "sp500_prices")
            data = data_fetcher.get_sp500_statements(sp500_tickers)
            self.db.save_to_postgres(data, "sp500_statements")
            print("Data saved successfully to PostgreSQL")
        except Exception as e:
            print(f"Error saving data to PostgreSQL: {e}")

    def fetch_and_save_ais_data(self, start_date, end_date, save_folder):
        processor = AISPortVisitProcessor(buffer_degrees=1)
        processor.download_data(start_date, end_date, save_folder)
        visits_df = processor.concat_all_zips(save_folder)
        self.db.save_to_postgres(visits_df, "port_visits")
        # Delete downloaded ZIP files after processing
        processor.delete_zips(save_folder)

    def fetch_and_save_locally(self, start_date, end_date, save_folder):
        
        processor = AISPortVisitProcessor(buffer_degrees=1)
        processor.download_data(start_date, end_date, save_folder)
        visits_df = processor.concat_all_zips(save_folder)
        visits_df.to_csv("port_visits", index=False)
        print(f"Port visits saved to {save_folder} (records: {len(visits_df)})")
        # uncomment to delete the downloaded zips
        processor.delete_zips(save_folder)

        data_fetcher = YFinanceFetcher(start_date=start_date, end_date=end_date)
        sp500_tickers = data_fetcher.get_sp500_tickers()
        data = data_fetcher.get_ticker_price("^GSPC", start_date, end_date)
        try:   
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


    def load_data(self, table_name):
        print(f"Loading data from PostgreSQL table: {table_name}")
        data = self.db.fetch_from_postgres(table_name)
        print(f"Data loaded successfully from {table_name}")
        return data


if __name__ == "__main__":
    
    db_config = {
        "user": "ais_sp500_db_user",
        "password": "lFmNQ7l0yiXhSc1Bet3OHw8EuUeDuv7F",
        "host": "dpg-d0rflvndiees73c05ea0-a.oregon-postgres.render.com",
        "port": 5432,
        "database": "ais_sp500_db"
    }
    pipeline = Pipeline(db_config)
    start_date = "2020-01-01"
    end_date = "2023-10-01"
    pipeline.fetch_and_save_yfinance_data(start_date, end_date)
    save_folder = "../assets/ais_data"
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)
    pipeline.fetch_and_save_ais_data(start_date, end_date, save_folder)
    data = pipeline.load_data("sp500_prices")
    print(data.head())
    data = pipeline.load_data("port_visits")
    print(data.head())