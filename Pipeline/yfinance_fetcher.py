#This script will pull data from yfinance's API, save as csv and send to a DB
import yfinance as yf
import pandas as pd

class YFinanceFetcher:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def get_sp500_data(start_date, end_date):
        def update_data(df,fetched_dfs):
            for fetched_df in fetched_dfs:
                fetched_df = fetched_df.transpose()
                fetched_df['Ticker'] = df['Ticker']
                df = pd.concat([df, fetched_df], axis=0)
            return df
            
        # Use Wikipedia list for reference
        sp500_tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]

        symbols = sp500_tickers['Symbol'].tolist()
        # Get the stock data for all the stocks in the S&P 500 index from 2010 to 2020
        # Create an empty DataFrame to store the data
        data = pd.DataFrame()

        # Loop through each ticker and get the data
        for ticker in symbols:
            try:
                # Get the stock data for the ticker
                stock_data = yf.Ticker(ticker).history(start=start_date, end=end_date)
                # Add a column for the ticker symbol
                stock_data['Ticker'] = ticker
                # Append the data to the DataFrame
                data = pd.concat([data, stock_data], axis=0)
                # Now we do income statement
                income_statement = yf.Ticker(ticker).get_income_statement()
                # Now we do balance sheet
                balance_sheet = yf.Ticker(ticker).get_balance_sheet()
                # Now we do cash flow
                cash_flow = yf.Ticker(ticker).get_cashflow()
                fetched_dfs = [income_statement, balance_sheet, cash_flow]
                # Update the data with the fetched data
                data = update_data(data, fetched_dfs)
            
            except Exception as e:
                print(f"Error retrieving data for {ticker}: {e}")

        data.reset_index(inplace=True)

        # add the details from sp500_tickers to data
        data = data.merge(sp500_tickers, left_on='Ticker', right_on='Symbol', how='left')

        return data
    
    def get_SP500_data_macro(start,end):
        """
        Fetches historical stock data for all S&P 500 companies from YFinance.
        """
        # Fetch the S&P 500 data
        sp500 = yf.Ticker("^GSPC")
        # Get the historical data for the S&P 500
        sp500_data = sp500.history(start=start, end=end)
        return sp500_data
    
    def save_to_csv(data, filename):
        """
        Saves the DataFrame to a CSV file.
        """
        data.to_csv(filename, index=False)
        print(f"Data saved to {filename}")