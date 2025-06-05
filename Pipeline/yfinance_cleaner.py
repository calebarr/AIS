import pandas as pd
income_cols_to_drop = columns_to_drop = [
    "Restructuring And Mergern Acquisition",
    "Depreciation Amortization Depletion Income Statement",
    "Depreciation And Amortization In Income Statement",
    "Amortization",
    "Amortization Of Intangibles Income Statement",
    "Otherunder Preferred Stock Dividend",
    "Other Special Charges",
    "Other Operating Expenses",
    "Average Dilution Earnings",
    "Selling And Marketing Expense",
    "Earnings From Equity Interest",
    "Gain On Sale Of Ppe",
    "Write Off",
    "Loss Adjustment Expense",
    "Net Policyholder Benefits And Claims",
    "Policyholder Benefits Gross",
    "Policyholder Benefits Ceded",
    "Provision For Doubtful Accounts",
    "Depreciation Income Statement",
    "Preferred Stock Dividends",
    "Other Taxes",
    "Excise Taxes",
    "Occupancy And Equipment",
    "Professional Expense And Contract Services Expense",
    "Other Non Interest Expense",
    "Total Other Finance Cost",
    "Rent Expense Supplemental",
    "Rent And Landing Fees",
    "Insurance And Claims",
    "Depletion Income Statement",
    "Net Income From Tax Loss Carryforward",
    "Net Income Extraordinary",
    "Securities Amortization",
    "Statement"
]
columns_to_drop_balance = [
    "Foreign Currency Translation Adjustments",
    "Minimum Pension Liabilities",
    "Preferred Stock",
    "Non Current Accrued Expenses",
    "Long Term Provisions",
    "Current Provisions",
    "Inventories Adjustments Allowances",
    "Other Payable",
    "Dividends Payable",
    "Other Investments",
    "Available For Sale Securities",
    "Other Equity Interest",
    "Non Current Deferred Revenue",
    "Leases",
    "Interest Payable",
    "Cash Equivalents",
    "Cash Financial",
    "Dueto Related Parties Current",
    "Non Current Prepaid Assets",
    "Duefrom Related Parties Current",
    "Preferred Stock Equity",
    "Preferred Securities Outside Stock Equity",
    "Non Current Note Receivables",
    "Investments In Other Ventures Under Equity Method",
    "Investmentsin Associatesat Cost",
    "Unrealized Gain Loss",
    "Loans Receivable",
    "Derivative Product Liabilities",
    "Line Of Credit",
    "Financial Assets",
    "Current Deferred Assets",
    "Taxes Receivable",
    "Current Deferred Taxes Liabilities",
    "Receivables Adjustments Allowances",
    "Investmentsin Joint Venturesat Cost",
    "Investment Properties",
    "Notes Receivable",
    "Preferred Shares Number",
    "Other Inventories",
    "Current Notes Payable",
    "Held To Maturity Securities",
    "Cash Cash Equivalents And Federal Funds Sold",
    "Trading Securities",
    "Financial Assets Designatedas Fair Value Through Profitor Loss ",
    "Investmentsin Subsidiariesat Cost",
    "Current Deferred Taxes Assets",
    "Accrued Interest Receivable",
    "Dueto Related Parties Non Current",
    "Duefrom Related Parties Non Current",
    "Restricted Common Stock",
    "Total Partnership Capital", 
    "Limited Partnership Capital",
    "Statement",
]

text = "Gain Loss On Investment Securities	Short Term Debt Payments	Short Term Debt Issuance	Provisionand Write Offof Assets	Issuance Of Capital Stock	Common Stock Issuance	Change In Other Current Liabilities	Unrealized Gain Loss On Investment Securities	Gain Loss On Sale Of PPE	Earnings Losses From Equity Investments	Other Cash Adjustment Inside Changein Cash	Net Preferred Stock Issuance	Preferred Stock Issuance	Net Foreign Currency Exchange Gain Loss	Amortization Of Securities	Net Intangibles Purchase And Sale	Purchase Of Intangibles	Cash From Discontinued Financing Activities	Cash From Discontinued Investing Activities	Cash From Discontinued Operating Activities	Preferred Stock Dividend Paid	Dividend Received Cfo	Preferred Stock Payments	Net Investment Properties Purchase And Sale	Sale Of Investment Properties	Purchase Of Investment Properties	Other Cash Adjustment Outside Changein Cash	Change In Interest Payable	Dividends Received Cfi	Sale Of Intangibles	Excess Tax Benefit From Stock Based Compensation	Interest Received Cfi	Cash Flow From Discontinued Operation	Depletion	Interest Paid Cff	Taxes Refund Paid	Cash Flowsfromusedin Operating Activities Direct	Classesof Cash Payments	Other Cash Paymentsfrom Operating Activities	Paymentson Behalfof Employees	Classesof Cash Receiptsfrom Operating Activities	Other Cash Receiptsfrom Operating Activities	Dividend Paid Cfo	Interest Paid Cfo"
cashlow_cols_to_drop = text.split("	")

class YFinanceCleaner:
    def __init__(self, info, income, balance, cashflow, price, macro_price):
        self.sp500_info = info
        self.income = self.__clean_sp500_dataframe(income, columns_to_drop, 10)
        self.balance = self.__clean_sp500_dataframe(balance, columns_to_drop_balance, 10)
        self.cashflow = self.__clean_sp500_dataframe(cashflow, cashlow_cols_to_drop, 10)
        self.price, self.dropped_price_cols = self.__clean_sp500_price_df(price)
        self.macro_price, self.dropped_macro_price_cols = self.__clean_sp500_price_df(macro_price)
    def __clean_sp500_dataframe(self, df: pd.DataFrame, columns: list, threshold:int):
        """Clean SP500 DF
        Input:
        df: either sp500_info or sp500 income statement/cashflow (not price)
        columns: list of columns to drop
        threshold: min amount of values per row to consider keeping
        """
        if "Symbol" in df.columns:
            df.rename(columns={"Symbol": "Ticker"}, inplace=True)
        df.drop(columns=columns,inplace=True)
        df.dropna(thresh=threshold, inplace= True)
        if "Headquarters Location" in df.columns:
            df["Headquarters Location"] = df["Headquarters Location"].apply(
                lambda x: x.split(", ")[-1] if isinstance(x, str) else x
            )
    
        df["Ticker"] = df["Ticker"].astype("category")
        if "Period" in df.columns:
            df["Period"] = pd.to_datetime(df["Period"], errors='coerce')

        return df

    def __clean_sp500_price_df(self,df):
            """
            To use for price based dataframe. Returns tuple with list of dropped columns (if applicable)
            Input: df: either sp500_prices or macro prices
            Output: (df, list of dropped cols)
            """
            nan_columns = df.columns[df.isna().any()].tolist()
            df.drop(columns=nan_columns, inplace=True) 
            return (df, nan_columns)

    def __build_combined_quarterly(self,dfs: list[pd.DataFrame]) -> pd.DataFrame:
        # Each df in “dfs” must have columns ["Ticker", "Period", ...fundamentals...]
        indexed = []
        for df in dfs:
            temp = df.copy()
            temp = temp.set_index(["Ticker", "Period"], drop=False)
            indexed.append(temp)
        # Start from the first
        combined = indexed[0]
        for other in indexed[1:]:
            # Drop "Ticker" and "Period" from `other` so we don't duplicate those columns
            combined = combined.join(
                other.drop(columns=["Ticker", "Period"]),
                how="outer",
                lsuffix="",
                rsuffix=""
            )
        # Bring "Ticker" and "Period" back into columns
        combined = combined.reset_index(drop=True)
        # Sort for merge_asof
        combined.sort_values(["Ticker", "Period"], inplace=True)
        return combined

    def run(self):
        combined_quarterly = self.__build_combined_quarterly([self.income, self.balance, self.cashflow])
        # 1) Optional: keep only one row per (Ticker, Period) in the quarterly table
        combined_quarterly = combined_quarterly.drop_duplicates(subset=["Ticker","Period"])
        combined_quarterly.to_csv("sp500_combined_quarterly.csv", index=False)
        #5) Take your daily price table (sp500_prices) and turn it into long form.
        prices = self.price.copy()
        prices = prices.rename(columns={"Date": "Period"})
        prices["Period"] = pd.to_datetime(prices["Period"])

        # Drop any column that is completely NaN (e.g. maybe some tickers have no data)
        nan_cols = prices.columns[prices.isna().all()].tolist()
        prices.drop(columns=nan_cols, inplace=True)

        # Remove "_Close" suffix so each price column exactly matches Ticker
        prices.columns = prices.columns.str.replace("_Close", "", regex=False)

        # Make “Ticker” a category and pivot from wide→long via .stack()
        prices.set_index("Period", inplace=True)
        prices_long = (
        prices
        .stack(dropna=False)           # multi-index: (Period, Ticker) → Price
        .reset_index()
        .rename(columns={0: "Price",        # rename the stacked‐value column
                        "level_1": "Ticker"}) 
        )
        
        # 0) identical dtype against; coerce & sort
        for df in (prices_long, combined_quarterly):
            df["Period"] = pd.to_datetime(df["Period"], errors="coerce")
        prices_long        = prices_long.sort_values(["Ticker", "Period"])
        combined_quarterly = combined_quarterly.sort_values(["Ticker", "Period"])

        # 1) build hierarchical indices
        daily_idx   = prices_long.set_index(["Ticker", "Period"])
        quarter_idx = combined_quarterly.set_index(["Ticker", "Period"])

        # 2) left-join then forward-fill within each ticker
        df_daily_fund = (
        daily_idx
        .join(quarter_idx, how="left")        # align on exact dates
        .groupby(level=0)                     # level 0 = Ticker
        .ffill()                              # carry last available quarter fwd
        .reset_index()                        # back to flat columns
        )  

        # 3) drop all rows from self.dropped_price_cols
        df_daily_fund.drop(columns=self.dropped_price_cols, inplace=True, errors='ignore')
        
        macro = (
        self.macro_price
            .rename(columns={"Date": "Period"})
            .assign(Period=lambda d: pd.to_datetime(d["Period"]))
    )
        
        self.sp500_info["Symbol"]   = self.sp500_info["Symbol"].astype(str).str.upper()
        df_daily_fund["Ticker"] = df_daily_fund["Ticker"].astype(str).str.upper()

        # 3)  Merge
        df_daily_fund = (
            df_daily_fund
            .merge(
                self.sp500_info,
                left_on="Ticker",          # daily-fundamental side
                right_on="Symbol",         # info side
                how="left",                # keep every trading day even if a ticker is missing from info
            )
            .drop(columns=["Symbol"])      # “Ticker” already covers it
        )

        # 4)  (Optional) re-order columns so company info sits together
    # If macro series isn’t daily-complete, merge_asof will back-fill the
    # most-recent macro value prior to each trading day.
        df_daily_fund = pd.merge_asof(
            df_daily_fund.sort_values("Period"),
            macro.sort_values("Period"),
            on="Period",
            direction="backward"
        ).drop(columns=self.dropped_macro_price_cols, errors="ignore")
        
        return df_daily_fund