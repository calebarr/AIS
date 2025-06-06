#This script will pull data from yfinance's API, save as csv and send to a DB
import yfinance as yf
import pandas as pd

class YFinanceFetcher:
    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def get_sp500_info(self):
        """
        Returns wiki's dataframe of S&P 500 companies.
        """
        sp500 = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        return sp500

    def get_sp500_tickers(self):
        """
        Fetches the list of S&P 500 companies' symbols from Wikipedia.
        """
        sp500 = self.get_sp500_info()
        symbols = sp500["Symbol"].tolist()
        return symbols
    
    

    def get_ticker_price(self,tickers,start, end):
        """
        Fetches historical stock data for a given ticker from YFinance.
        """
        # download the data
        df = yf.download(tickers, start=start, end=end)
        # reset index to make 'Date' a column
        if isinstance(df.columns, pd.MultiIndex):
                print("MultiIndex")
                df.columns = [
                    f"{ticker}_{field}".replace(" ", "_")
                    for field, ticker in df.columns
                ]
        # Only keep Close prices (ends with _Close)
        df = df.reset_index()
        close_cols = [col for col in df.columns if col.endswith("_Close")]
        if not close_cols:
            raise ValueError("No Close prices found in the data.")#
        print(df.columns)
        df = df[["Date"] + close_cols]
        return df
    
    def get_sp500_statements(self,symbols):
        income, balance, cashflow = [], [], []

        for ticker in symbols:
            tk = yf.Ticker(ticker)
            # mapping of label → DataFrame
            stmt_map = {
                "income":    tk.financials,        # quarterly or annual income statement
                "balance":   tk.balance_sheet,     # quarterly or annual balance sheet
                "cashflow":  tk.cashflow           # quarterly or annual cash-flow
            }

            for stmt_name, df in stmt_map.items():
                try:
                    # transpose so periods become rows
                    df_flat = (
                        df.T
                        .reset_index()
                        .rename(columns={"index": "Period"})
                        .assign(Ticker=ticker, Statement=stmt_name)
                    )
                    if stmt_name == "income":
                        income.append(df_flat)
                    elif stmt_name == "balance":
                        balance.append(df_flat)
                    else:
                        cashflow.append(df_flat)
                    print(f"✅ {stmt_name} for {ticker} fetched successfully")
                except Exception as e:
                    print(f"⚠️  {stmt_name} for {ticker} failed: {e}")
        
        # iterate over the dataframes
        for df in [income, balance, cashflow]:
            #check if there are any columns with all values as NaN
            df = [d for d in df if not d.empty and not d.isnull().all().all()]

        return (
            pd.concat(income,   ignore_index=True, sort=False),
            pd.concat(balance,  ignore_index=True, sort=False),
            pd.concat(cashflow, ignore_index=True, sort=False)
        )
    