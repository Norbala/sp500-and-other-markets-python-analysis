import yfinance as yf
import pandas as pd
from datetime import datetime
import os

# block (un)comment this part
'''
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies' # URL of Wikipedia with the S&P 500 companies list
tables = pd.read_html(url) # Read all tables from the Wikipedia page
sp500_table = tables[0] # The first table on the page contains the list of S&P 500 companies

'''
stocks = ["AAPL", "BIIB", "INTC", "AMD", "NVDA", "QCOM"] # , "INTC", "AMD", "NVDA", "QCOM"

allStocksDict = []

for stock in stocks:
    try:
        singleStock = yf.Ticker(stock)
        singleStockData = singleStock.info
        allStocksDict.append(singleStockData)
        print(singleStockData)
    except Exception as e:
        print(f"Error fetching data: {e}")

df = pd.DataFrame(allStocksDict)

df = df[["symbol", "sector", "industry", "marketCap", "currentPrice", "fiftyTwoWeekHigh", "fiftyTwoWeekLow",
 "fiftyDayAverage", "twoHundredDayAverage"]]


