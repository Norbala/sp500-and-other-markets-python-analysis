import yfinance as yf
import pandas as pd
from datetime import datetime
import os
 
# block (un)comment this part

url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies' # URL of Wikipedia with the S&P 500 companies list
tables = pd.read_html(url, storage_options={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}) # Read all tables from the Wikipedia page
# Find the table that contains a column with ticker information
sp500_table = None

for t in tables:
    cols = [str(c).lower() for c in t.columns]  # <-- FIXED: convert everything to string

    if any(c in ["symbol", "ticker", "ticker symbol"] for c in cols):
        sp500_table = t
        break

if sp500_table is None:
    raise RuntimeError("Could not find S&P 500 table on Wikipedia. The page structure may have changed.")

# block (un)comment this part


# block (un)comment this part

""" df = pd.read_csv("sp500_tickers.csv", header=None)
df[['date', 'Symbol']] = df[0].str.split(",", n=1, expand=True)
df = df.drop(columns=[0])
sp500_table = df["Symbol"].str.strip('"').str.split(",").explode().reset_index(drop=True)
sp500_table = sp500_table.dropna().reset_index(drop=True) """

possible_symbol_cols = ["Symbol", "Ticker", "Ticker symbol", "Ticker Symbol", "Security"]

ticker_col = None
for col in sp500_table.columns:
    if any(col.strip().lower() == p.lower() for p in possible_symbol_cols):
        ticker_col = col
        break

dfStocks = sp500_table[ticker_col]

# Clean tickers
dfStocks = dfStocks.astype(str)                 # ensure all are strings
dfStocks = dfStocks.str.replace(".", "-")       # BRK.B → BRK-B
dfStocks = dfStocks.str.strip()                 # remove spaces
dfStocks = dfStocks[dfStocks != ""]             # remove empty strings
dfStocks = dfStocks.dropna().unique()           # remove duplicates + convert to numpy array
allStocksDict = []
todayDate = datetime.today().strftime('%Y_%m_%d')

for stock in dfStocks:
    try:
        singleStock = yf.Ticker(stock)
        singleStockData = singleStock.info
        allStocksDict.append(singleStockData)
        print(singleStockData)
    except Exception as e:
        print(f"Error fetching data: {e}")

required_cols = ["symbol", "sector", "industry", "marketCap", "sharesOutstanding",
 "currentPrice", "enterpriseValue", "bookValue", "totalCash", "totalDebt",
 "ebitda", "enterpriseToEbitda", "debtToEquity", "quickRatio", "currentRatio",
 "forwardPE", "trailingEps", "trailingPegRatio", "revenueGrowth",
 "profitMargins", "grossMargins", "operatingMargins", "fiftyTwoWeekHigh",
 "fiftyTwoWeekLow", "fiftyDayAverage", "twoHundredDayAverage",
 "dividendYield", "freeCashflow"]

df = pd.DataFrame(allStocksDict)

# Add missing columns with NaN
for col in required_cols:
    if col not in df.columns:
        df[col] = None

df = df[required_cols]


df["dividendYield"] /= 100
df["currentPrice / twoHundredDayAverage - 1"] = df["currentPrice"].replace(0, float("nan")) / df["twoHundredDayAverage"].replace(0, float("nan")) - 1 
df["enterpriseValue / freeCashflow"] = df["enterpriseValue"].replace(0, float("nan")) / df["freeCashflow"].replace(0, float("nan"))
df["marketCap / freeCashflow"] = df["marketCap"].replace(0, float("nan")) / df["freeCashflow"].replace(0, float("nan"))


nameCounter = 1
spString = "SP-500-companies_"
todayDate = todayDate
extension = ".xlsx"
excelFileName = spString+todayDate+extension
while True:
    path = "./" + excelFileName
    check_file = os.path.isfile(path)
    if not check_file:
        writer = pd.ExcelWriter(excelFileName, engine='xlsxwriter')
        df.to_excel(writer, index=False, startrow=1, header=False, sheet_name="Sheet1")
        workbook = writer.book
        worksheet = writer.sheets["Sheet1"]
        (max_row, max_col) = df.shape

        column_settings = []
        for header in df.columns:
            column_settings.append({"header": header})

        worksheet.add_table(0, 0, max_row, max_col-1, {"columns": column_settings})

        for i, col in enumerate(df.columns):
            width = max(df[col].apply(lambda x: len(str(x))).max(), len(col))
            worksheet.set_column(i, i, width)

        percentage_format = workbook.add_format({"num_format": "0.0%"}) # type: ignore
        dividendYieldIndex = df.columns.get_loc("dividendYield")
        OperatingMarginTTMIndex = df.columns.get_loc("operatingMargins")
        GrossMarginIndex = df.columns.get_loc("grossMargins")
        ProfitMarginIndex = df.columns.get_loc("profitMargins")
        revenueGrowthIndex = df.columns.get_loc("revenueGrowth")
        PriceTo200dayAverageIndex = df.columns.get_loc("currentPrice / twoHundredDayAverage - 1")
        worksheet.set_column(dividendYieldIndex, dividendYieldIndex, None, percentage_format)
        worksheet.set_column(ProfitMarginIndex, ProfitMarginIndex, None, percentage_format)
        worksheet.set_column(OperatingMarginTTMIndex, OperatingMarginTTMIndex, None, percentage_format)
        worksheet.set_column(GrossMarginIndex, GrossMarginIndex, None, percentage_format)
        worksheet.set_column(revenueGrowthIndex, revenueGrowthIndex, None, percentage_format)
        worksheet.set_column(PriceTo200dayAverageIndex, PriceTo200dayAverageIndex, None, percentage_format)

        writer.close()
        break
    else:
        excelFileName = spString + todayDate + "_" + str(nameCounter) + extension
        nameCounter += 1

# izvedenice:     "EV/NetDebt",   
#
# companyInfo["netDebt"] = companyInfo["enterpriseValue"]-companyInfo["marketCap"]
# companyInfo["EVtoNetDebt"] = companyInfo["enterpriseValue"]/companyInfo["netDebt"]
# if companyInfo["netIncomeToCommon"] > 0:
#     companyInfo["PEratio"] = round(companyInfo["currentPrice"]/companyInfo["netIncomeToCommon"])

# df = pd.DataFrame(companyInfo["underlyingSymbol", "sector", "industry",
#                              "marketCap", "sharesOutstanding", "currentPrice", "enterpriseValue", "bookValue",
#                              "EVtoNetDebt", "ebitda", "enterpriseToEbitda", "PEratio", "52WeekChange"])

