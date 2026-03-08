import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import os
from datetime import date

# === Get S&P 500 tickers ===
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]
    return df[['Symbol', 'Security', 'GICS Sector', 'GICS Sub-Industry']].rename(
        columns={'Symbol': 'Ticker', 'Security': 'Name', 'GICS Sector': 'Sector', 'GICS Sub-Industry': 'Industry'}
    )

# === Fetch yfinance data ===
def fetch_yfinance_data(ticker_list):
    records = []
    for ticker in ticker_list:
        try:
            yf_ticker = yf.Ticker(ticker)
            info = yf_ticker.info
            records.append({
                'Ticker': ticker,
                'MarketCap': info.get('marketCap', 0),
                'Price': info.get('currentPrice', None),
                '50DayAvg': info.get('fiftyDayAverage', None),
                '200DayAvg': info.get('twoHundredDayAverage', None)
            })
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")
    return pd.DataFrame(records)

# === Compute weighted % changes ===
def compute_weighted_changes(df, group_cols):
    df = df.dropna(subset=['MarketCap', 'Price', '50DayAvg', '200DayAvg'])
    df = df[df['MarketCap'] > 0]
    
    def calc(group):
        mcap = group['MarketCap']
        price = group['Price']
        pct_50 = ((price - group['50DayAvg']) / group['50DayAvg']) * 100
        pct_200 = ((price - group['200DayAvg']) / group['200DayAvg']) * 100
        
        weighted_50 = (pct_50 * mcap).sum() / mcap.sum()
        weighted_200 = (pct_200 * mcap).sum() / mcap.sum()
        
        return pd.Series({
            'Change_to_50Day_MA (%)': weighted_50,
            'Change_to_200Day_MA (%)': weighted_200
        })
    
    result = df.groupby(group_cols).apply(calc).reset_index()
    return result

# === Save CSV ===
def save_csv(df, filename):
    df.to_csv(filename, index=False, sep=';', float_format='%.4f')
    print(f"Saved: {filename}")

# === Save XLSX ===
def save_excel(df, filename):
    df.to_excel(filename, index=False, float_format='%.2f')
    print(f"Saved Excel: {filename}")


# === Plot sector-level ===
def plot_sector_level(df):
    labels = df['Sector']
    x = range(len(labels))
    
    plt.figure(figsize=(12, 6))
    plt.bar([i - 0.2 for i in x], df['Change_to_50Day_MA (%)'], width=0.4, label='50-day MA')
    plt.bar([i + 0.2 for i in x], df['Change_to_200Day_MA (%)'], width=0.4, label='200-day MA')
    
    plt.xticks(x, labels, rotation=45, ha='right')
    plt.ylabel('% Change (Market Cap Weighted)')
    plt.title('S&P 500 Sector Weighted Price Change vs Moving Averages')
    plt.legend()
    plt.tight_layout()
    plt.savefig('sp500_sector_price_changes.png')
    plt.close()
    print("Saved: sp500_sector_price_changes.png")


def get_unique_path(path: str) -> str:
    today_str = date.today().isoformat()
    path = today_str + "_" + path

    if os.path.exists(path):
        base, ext = os.path.splitext(path)
        i = 1
        while True:
            new_path = f"{base}_{i}{ext}"
            if not os.path.exists(new_path):
                return new_path
            i += 1
    return path


# === Plot per sector-industry ===
def plot_sector_industry(df):
    os.makedirs(get_unique_path("sector_industry_charts"))
    sectors = df['Sector'].unique()
    
    for sector in sectors:
        subset = df[df['Sector'] == sector]
        labels = subset['Industry']
        x = range(len(labels))
        
        plt.figure(figsize=(max(8, len(labels) * 0.6), 6))
        plt.bar([i - 0.2 for i in x], subset['Change_to_50Day_MA (%)'], width=0.4, label='50-day MA')
        plt.bar([i + 0.2 for i in x], subset['Change_to_200Day_MA (%)'], width=0.4, label='200-day MA')
        
        plt.xticks(x, labels, rotation=45, ha='right')
        plt.ylabel('% Change (Market Cap Weighted)')
        plt.title(f'{sector}: Weighted Price Change vs Moving Averages')
        plt.legend()
        plt.tight_layout()
        
        fname = f"{get_unique_path("sector_industry_charts")}/{sector.replace('/', '_')}.png"
        plt.savefig(fname)
        plt.close()
        print(f"Saved: {fname}")

# === MAIN ===
def main():
    # 1. Get tickers
    sp500_info = get_sp500_tickers()
    tickers = sp500_info['Ticker'].tolist()
    print(f"Found {len(tickers)} tickers.")
    
    # 2. Fetch stock data
    stock_data = fetch_yfinance_data(tickers)
    
    # 3. Merge info
    full_data = pd.merge(sp500_info, stock_data, on='Ticker')
    
    # 4. Sector-level
    sector_changes = compute_weighted_changes(full_data, ['Sector'])
    save_excel(sector_changes, get_unique_path('sp500_sector_price_changes.xlsx'))
    plot_sector_level(sector_changes)
    
    # 5. Sector-industry level
    sector_industry_changes = compute_weighted_changes(full_data, ['Sector', 'Industry'])
    save_excel(sector_industry_changes, get_unique_path('sp500_sector_industry_price_changes.xlsx'))
    plot_sector_industry(sector_industry_changes)

if __name__ == "__main__":
    main()
