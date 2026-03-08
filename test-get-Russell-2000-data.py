import pandas as pd
import yfinance as yf
import time
import math
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional


# Path to your downloaded file
file_path = "IWM_holdings.csv"

# Read CSV while skipping metadata rows
df = pd.read_csv(file_path, skiprows=9)

# Drop completely empty rows (e.g. the blank line before the copyright)
df = df.dropna(how="all")

# Remove the copyright/footer line (which doesn't have a valid ticker)
# The tickers are usually short uppercase strings; copyright lines are long text
df = df[df["Ticker"].str.match(r"^[A-Z0-9.-]+$", na=False)]

# Optional: reset index
df = df.reset_index(drop=True)
df = df[["Ticker"]]
print("Number of constituents:", len(df))
print(df.head())

tickers = df["Ticker"].tolist()



# Requested fields (as the user provided)
DESIRED_FIELDS = [
    "symbol", "sector", "industry", "marketCap", "sharesOutstanding",
    "currentPrice", "enterpriseValue", "bookValue", "totalCash",
    "totalDebt", "ebitda", "enterpriseToEbitda", "debtToEquity",
    "quickRatio", "currentRatio", "forwardPE", "trailingEps",
    "trailingPegRatio", "revenueGrowth", "profitMargins",
    "grossMargins", "operatingMargins", "fiftyTwoWeekHigh",
    "fiftyTwoWeekLow", "fiftyDayAverage", "twoHundredDayAverage",
    "dividendYield", "freeCashflow"
]

def _safe_get(d: Dict[str, Any], key: str) -> Any:
    """safe dict getter that handles nested or alternative keys if needed."""
    return d.get(key, None)

def fetch_single_ticker(ticker: str,
                        desired_fields: List[str] = DESIRED_FIELDS,
                        retries: int = 3,
                        retry_backoff: float = 1.0,
                        timeout_per_call: float = 15.0) -> Dict[str, Any]:
    """
    Fetch info for a single ticker using yfinance.Ticker.
    Retries on exceptions with exponential backoff.
    Returns a dict whose keys are desired_fields (plus 'fetch_error' if failed).
    """
    out = {k: None for k in desired_fields}
    out["symbol"] = ticker  # ensure symbol present even if fetch fails

    attempt = 0
    while attempt <= retries:
        attempt += 1
        try:
            t = yf.Ticker(ticker)
            # Prefer fast_info for some core fields (if present) because it's quicker
            fast = {}
            try:
                fast = getattr(t, "fast_info", {}) or {}
            except Exception:
                fast = {}

            # main info
            info = {}
            try:
                info = getattr(t, "info", {}) or {}
            except Exception:
                # sometimes .info fails; leave as empty dict and rely on fast_info or other attributes
                info = {}

            # Some fields are only in fast_info
            # Define mapping from desired field -> where to look (info, fast)
            field_value_map = {}
            for f in desired_fields:
                val = None
                # Common quick sources:
                # - fast_info keys: last_price, market_cap, currency, etc.
                # - info keys: enterpriseValue, bookValue, totalCash, etc.
                if f in ("currentPrice",):
                    # fast_info often has 'last_price' or use info['regularMarketPrice']
                    val = fast.get("last_price", None) or _safe_get(info, "regularMarketPrice")
                elif f in ("marketCap",):
                    val = fast.get("market_cap", None) or _safe_get(info, "marketCap")
                elif f == "sharesOutstanding":
                    val = _safe_get(info, "sharesOutstanding")
                elif f == "symbol":
                    val = _safe_get(info, "symbol") or ticker
                else:
                    # default to info dict
                    val = _safe_get(info, f)
                    # fallback to some typical alternate keys
                    if val is None:
                        alt_map = {
                            "ebitda": "ebitda",
                            "enterpriseValue": "enterpriseValue",
                            "enterpriseToEbitda": "enterpriseToEbitda",
                            "debtToEquity": "debtToEquity",
                            "bookValue": "bookValue",
                            "totalCash": "totalCash",
                            "totalDebt": "totalDebt",
                            "forwardPE": "forwardPE",
                            "trailingEps": "trailingEps",
                            "trailingPegRatio": "pegRatio",
                            "revenueGrowth": "revenueGrowth",
                            "profitMargins": "profitMargins",
                            "grossMargins": "grossMargins",
                            "operatingMargins": "operatingMargins",
                            "fiftyTwoWeekHigh": "fiftyTwoWeekHigh",
                            "fiftyTwoWeekLow": "fiftyTwoWeekLow",
                            "fiftyDayAverage": "fiftyDayAverage",
                            "twoHundredDayAverage": "twoHundredDayAverage",
                            "dividendYield": "dividendYield",
                            "freeCashflow": "freeCashflow",
                            "sector": "sector",
                            "industry": "industry"
                        }
                        mapped_key = alt_map.get(f)
                        if mapped_key:
                            val = _safe_get(info, mapped_key)
                field_value_map[f] = val

            # assign to out
            for f, v in field_value_map.items():
                out[f] = v

            # if we got here, we consider it a success (even if many fields None)
            out["fetch_error"] = None
            return out

        except Exception as exc:
            err = exc
            if attempt > retries:
                out["fetch_error"] = f"Failed after {retries} retries: {err}"
                return out
            # exponential backoff sleep
            sleep_time = retry_backoff * (2 ** (attempt - 1))
            # jitter a little
            sleep_time = sleep_time + (0.1 * attempt)
            time.sleep(sleep_time)
            continue

    # fallback, shouldn't reach here
    out["fetch_error"] = "Unknown error"
    return out

def fetch_many_tickers(tickers: List[str],
                       desired_fields: List[str] = DESIRED_FIELDS,
                       max_workers: int = 8,
                       retries: int = 3,
                       retry_backoff: float = 1.0,
                       cache_file: Optional[str] = "yfinance_russell2000_info.pkl",
                       save_every: int = 200) -> pd.DataFrame:
    """
    Fetch info for many tickers in parallel. Uses ThreadPoolExecutor.
    Caches intermediate results to `cache_file` to allow resuming.
    Returns DataFrame with rows for each ticker and columns = desired_fields + fetch_error.
    """
    # load cache if exists
    cache: Dict[str, Dict[str, Any]] = {}
    try:
        if cache_file:
            with open(cache_file, "rb") as f:
                cache = pickle.load(f)
    except Exception:
        cache = {}

    # determine tickers to fetch
    to_fetch = [t for t in tickers if (t not in cache)]
    print(f"Total tickers: {len(tickers)}, already cached: {len(tickers)-len(to_fetch)}, to fetch: {len(to_fetch)}")

    if to_fetch:
        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            future_to_ticker = {
                exe.submit(fetch_single_ticker, ticker, desired_fields, retries, retry_backoff): ticker
                for ticker in to_fetch
            }
            completed = 0
            for fut in as_completed(future_to_ticker):
                ticker = future_to_ticker[fut]
                try:
                    result = fut.result()
                except Exception as e:
                    result = {k: None for k in desired_fields}
                    result["symbol"] = ticker
                    result["fetch_error"] = f"Unhandled exception: {e}"
                cache[ticker] = result
                completed += 1
                if completed % 50 == 0:
                    print(f"Fetched {completed} / {len(to_fetch)} new tickers...")
                # periodically save cache to disk
                if cache_file and (completed % save_every == 0):
                    try:
                        with open(cache_file, "wb") as f:
                            pickle.dump(cache, f)
                    except Exception as e:
                        print("Warning: failed to save cache:", e)
            # final save
            if cache_file:
                with open(cache_file, "wb") as f:
                    pickle.dump(cache, f)

    # convert cache -> DataFrame (keep original tickers order)
    rows = []
    for t in tickers:
        rows.append(cache.get(t, {**{k: None for k in desired_fields}, "symbol": t, "fetch_error": "missing"}))
    df = pd.DataFrame(rows)
    # ensure desired fields come first in columns
    cols_order = desired_fields + [c for c in df.columns if c not in desired_fields]
    df = df.reindex(columns=cols_order)
    return df

# Example usage:
if __name__ == "__main__":
    # Suppose df is your ETF CSV loaded earlier and contains column 'Ticker'
    # Example minimal list:
    # tickers = ["AAPL", "MSFT", "AMZN"]   # replace with df["Ticker"].tolist()
    # But for Russell2000 you will pass the full list from your CSV:
    # tickers = df["Ticker"].astype(str).tolist()
    # tickers = ["AAPL", "MSFT", "AMZN"]  # placeholder

    # fetch (adjust max_workers for your environment; 8 is a safe default)
    df_info = fetch_many_tickers(tickers,
                                desired_fields=DESIRED_FIELDS,
                                max_workers=8,
                                retries=2,
                                retry_backoff=0.8,
                                cache_file="iwm_info_cache.pkl",
                                save_every=100)

    # show results
    print(df_info.head())
    # Save as CSV for downstream analysis:
    df_info.to_csv("iwm_tickers_in_depth_info.csv", index=False)
