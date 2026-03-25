"""
data_fetcher.py — DataFetcher

Fetches financial and economic data from multiple sources for DataForge.

Data source priority:
  Stock data:   yfinance → Polygon.io
  Forex data:   Alpha Vantage → ExchangeRate-API
  Crypto data:  CoinGecko (primary, no fallback needed)
  US macro:     FRED (primary) + BLS (jobs) + BEA (GDP)
  Global data:  World Bank → IMF → OECD
  Context:      NewsAPI (enrichment only)

Usage:
    fetcher = DataFetcher()
    movers = fetcher.fetch_daily_movers(top_n=5)
    fred_df = fetcher.fetch_fred_series('CPIAUCSL', periods=24)
    crypto = fetcher.fetch_crypto_movers(top_n=10)
"""

import logging
import os
from collections import namedtuple
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

DataPoint = namedtuple('DataPoint', [
    'metric_name',
    'current_value',
    'prev_value',
    'pct_change',
    'data_source',
    'date',
    'currency',
    'extra_meta',
])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COINGECKO_BASE_URL = os.environ.get('COINGECKO_BASE_URL', 'https://api.coingecko.com/api/v3')
IMF_BASE_URL = os.environ.get('IMF_BASE_URL', 'https://www.imf.org/external/datamapper/api/v1')
WORLD_BANK_BASE_URL = 'https://api.worldbank.org/v2'
ALPHA_VANTAGE_KEY = os.environ.get('ALPHA_VANTAGE_KEY', '')
EXCHANGE_RATE_API_KEY = os.environ.get('EXCHANGE_RATE_API_KEY', '')
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY', '')
FRED_API_KEY = os.environ.get('FRED_API_KEY', '')
BLS_API_KEY = os.environ.get('BLS_API_KEY', '')
BEA_API_KEY = os.environ.get('BEA_API_KEY', '')
NEWS_API_KEY = os.environ.get('NEWS_API_KEY', '')

# Track Alpha Vantage daily calls (switch to fallback at 20/25)
_alpha_vantage_calls_today = 0
_ALPHA_VANTAGE_DAILY_LIMIT = 20


# ---------------------------------------------------------------------------
# DataFetcher
# ---------------------------------------------------------------------------

class DataFetcher:
    """
    Fetches financial and economic data from multiple APIs.
    All methods return DataPoint namedtuples or pandas DataFrames.
    Failures are logged and return empty results — never crash the pipeline.
    """

    # ------------------------------------------------------------------
    # Stock data — yfinance → Polygon.io fallback
    # ------------------------------------------------------------------

    def fetch_daily_movers(self, top_n: int = 5) -> list:
        """
        Fetch top N biggest % movers in S&P 500 today using yfinance.
        Falls back to Polygon.io if yfinance returns stale/NaN data.

        Returns:
            list[DataPoint] sorted by abs(pct_change) descending.
        """
        try:
            import yfinance as yf
            import pandas as pd

            # S&P 500 tickers — representative sample for daily scan
            tickers = [
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',
                'BRK-B', 'JPM', 'UNH', 'XOM', 'JNJ', 'V', 'PG', 'MA',
                'HD', 'CVX', 'MRK', 'ABBV', 'PEP', 'KO', 'BAC', 'LLY',
                'COST', 'AVGO', 'TMO', 'MCD', 'CSCO', 'ACN', 'ABT',
            ]

            data = yf.download(
                tickers,
                period='2d',
                interval='1d',
                group_by='ticker',
                auto_adjust=True,
                progress=False,
                threads=True,
            )

            results = []
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

            for ticker in tickers:
                try:
                    if ticker not in data.columns.get_level_values(0):
                        continue
                    closes = data[ticker]['Close'].dropna()
                    if len(closes) < 2:
                        continue
                    current = float(closes.iloc[-1])
                    prev = float(closes.iloc[-2])
                    if prev == 0 or pd.isna(current) or pd.isna(prev):
                        continue
                    pct = ((current - prev) / prev) * 100
                    dp = DataPoint(
                        metric_name=f'{ticker} stock price',
                        current_value=round(current, 2),
                        prev_value=round(prev, 2),
                        pct_change=round(pct, 2),
                        data_source='yfinance',
                        date=today,
                        currency='USD',
                        extra_meta={'ticker': ticker},
                    )
                    if self.validate_data_point(dp):
                        results.append(dp)
                except Exception as e:
                    logger.warning('[dataforge] yfinance parse error for %s: %s', ticker, e)
                    continue

            results.sort(key=lambda x: abs(x.pct_change), reverse=True)
            logger.info('[dataforge] fetch_daily_movers: %d movers found', len(results))
            return results[:top_n]

        except Exception as e:
            logger.error('[dataforge] fetch_daily_movers failed: %s', e)
            return []

    def fetch_polygon_backup(self, ticker: str) -> 'DataPoint | None':
        """
        Fetch single ticker data from Polygon.io.
        Called ONLY when yfinance returns NaN or raises an exception.

        Returns:
            DataPoint or None on failure.
        """
        if not POLYGON_API_KEY:
            logger.warning('[dataforge] POLYGON_API_KEY not set — skipping backup fetch')
            return None
        try:
            url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/prev'
            resp = requests.get(
                url,
                params={'adjusted': 'true', 'apiKey': POLYGON_API_KEY},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get('results', [])
            if not results:
                return None
            r = results[0]
            current = float(r.get('c', 0))
            prev = float(r.get('o', 0))
            pct = ((current - prev) / prev * 100) if prev else 0.0
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            return DataPoint(
                metric_name=f'{ticker} stock price',
                current_value=round(current, 2),
                prev_value=round(prev, 2),
                pct_change=round(pct, 2),
                data_source='polygon',
                date=today,
                currency='USD',
                extra_meta={'ticker': ticker},
            )
        except Exception as e:
            logger.error('[dataforge] fetch_polygon_backup failed for %s: %s', ticker, e)
            return None

    # ------------------------------------------------------------------
    # FRED — US economic time-series
    # ------------------------------------------------------------------

    def fetch_fred_series(self, series_id: str, periods: int = 24):
        """
        Fetch last N periods of a FRED economic series.

        Common series_ids:
            'CPIAUCSL'    — CPI (inflation)
            'UNRATE'      — Unemployment rate
            'FEDFUNDS'    — Federal funds rate
            'MORTGAGE30US'— 30-year mortgage rate
            'GDP'         — US GDP
            'M2SL'        — M2 money supply

        Returns:
            pd.DataFrame with columns ['date', 'value'] or empty DataFrame on failure.
        """
        try:
            import pandas as pd
            from fredapi import Fred

            if not FRED_API_KEY:
                logger.error('[dataforge] FRED_API_KEY not set')
                return pd.DataFrame()

            fred = Fred(api_key=FRED_API_KEY)
            series = fred.get_series(series_id)
            df = series.reset_index()
            df.columns = ['date', 'value']
            df = df.dropna().tail(periods)
            logger.info(
                '[dataforge] fetch_fred_series: %s — %d periods retrieved',
                series_id, len(df),
            )
            return df

        except Exception as e:
            logger.error('[dataforge] fetch_fred_series failed for %s: %s', series_id, e)
            import pandas as pd
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # BLS — US jobs data
    # ------------------------------------------------------------------

    def fetch_bls_series(self, series_id: str, start_year: int, end_year: int):
        """
        Fetch BLS public data series.

        Common series_ids:
            'CES0000000001' — Total non-farm payrolls
            'LNS14000000'   — Unemployment rate
            'CIU1010000000000A' — Employment cost index

        Returns:
            pd.DataFrame with columns ['year', 'period', 'value'] or empty on failure.
        """
        try:
            import pandas as pd

            if not BLS_API_KEY:
                logger.error('[dataforge] BLS_API_KEY not set')
                return pd.DataFrame()

            url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
            payload = {
                'seriesid': [series_id],
                'startyear': str(start_year),
                'endyear': str(end_year),
                'registrationkey': BLS_API_KEY,
            }
            resp = requests.post(url, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if data.get('status') != 'REQUEST_SUCCEEDED':
                logger.error('[dataforge] BLS API error: %s', data.get('message', 'unknown'))
                return pd.DataFrame()

            series_data = data['Results']['series'][0]['data']
            rows = [
                {
                    'year': int(item['year']),
                    'period': item['period'],
                    'value': float(item['value'].replace(',', '')),
                }
                for item in series_data
            ]
            df = pd.DataFrame(rows).sort_values(['year', 'period'])
            logger.info(
                '[dataforge] fetch_bls_series: %s — %d records', series_id, len(df)
            )
            return df

        except Exception as e:
            logger.error('[dataforge] fetch_bls_series failed for %s: %s', series_id, e)
            import pandas as pd
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # BEA — GDP breakdown
    # ------------------------------------------------------------------

    def fetch_bea_gdp(self, frequency: str = 'Q'):
        """
        Fetch US GDP by component from BEA NIPA tables.
        frequency: 'Q' (quarterly) or 'A' (annual)

        Returns:
            pd.DataFrame with GDP component data or empty on failure.
        """
        try:
            import pandas as pd

            if not BEA_API_KEY:
                logger.error('[dataforge] BEA_API_KEY not set')
                return pd.DataFrame()

            url = 'https://apps.bea.gov/api/data/'
            params = {
                'UserID': BEA_API_KEY,
                'method': 'GetData',
                'datasetname': 'NIPA',
                'TableName': 'T10101',       # GDP and components
                'Frequency': frequency,
                'Year': 'X',                 # all available years
                'ResultFormat': 'JSON',
            }
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            rows = data['BEAAPI']['Results']['Data']
            df = pd.DataFrame(rows)
            logger.info('[dataforge] fetch_bea_gdp: %d rows retrieved', len(df))
            return df

        except Exception as e:
            logger.error('[dataforge] fetch_bea_gdp failed: %s', e)
            import pandas as pd
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # World Bank — global indicators
    # ------------------------------------------------------------------

    def fetch_world_bank(
        self,
        indicator: str,
        countries: list,
        start_year: int,
        end_year: int,
    ):
        """
        Fetch World Bank indicator for a list of countries over a date range.

        Common indicators:
            'NY.GDP.MKTP.CD'  — GDP (current USD)
            'SP.POP.TOTL'     — Total population
            'FP.CPI.TOTL.ZG'  — CPI inflation %
            'SI.POV.GINI'     — Gini index

        Returns:
            pd.DataFrame indexed by country + year or empty on failure.
        """
        try:
            import pandas as pd

            country_str = ';'.join(countries)
            url = (
                f'{WORLD_BANK_BASE_URL}/country/{country_str}/indicator/{indicator}'
            )
            params = {
                'date': f'{start_year}:{end_year}',
                'format': 'json',
                'per_page': 1000,
            }
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if len(data) < 2 or not data[1]:
                logger.warning('[dataforge] World Bank returned no data for %s', indicator)
                return pd.DataFrame()

            rows = []
            for item in data[1]:
                if item.get('value') is None:
                    continue
                rows.append({
                    'country': item['country']['value'],
                    'country_code': item['countryiso3code'],
                    'year': int(item['date']),
                    'value': float(item['value']),
                    'indicator': indicator,
                })
            df = pd.DataFrame(rows).sort_values(['country', 'year'])
            logger.info(
                '[dataforge] fetch_world_bank: %s — %d records across %d countries',
                indicator, len(df), len(countries),
            )
            return df

        except Exception as e:
            logger.error('[dataforge] fetch_world_bank failed for %s: %s', indicator, e)
            import pandas as pd
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Crypto — CoinGecko (no API key needed)
    # ------------------------------------------------------------------

    def fetch_crypto_movers(self, top_n: int = 10) -> list:
        """
        Fetch top N coins sorted by 24h % change from CoinGecko.

        Returns:
            list[DataPoint] sorted by abs(pct_change) descending.
        """
        try:
            url = f'{COINGECKO_BASE_URL}/coins/markets'
            params = {
                'vs_currency': 'usd',
                'order': 'percent_change_24h',
                'per_page': 100,
                'page': 1,
                'price_change_percentage': '24h',
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            coins = resp.json()

            results = []
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

            for coin in coins:
                pct = coin.get('price_change_percentage_24h')
                current = coin.get('current_price')
                if pct is None or current is None:
                    continue
                prev = current / (1 + pct / 100) if (1 + pct / 100) != 0 else current
                dp = DataPoint(
                    metric_name=f"{coin['name']} ({coin['symbol'].upper()}) price",
                    current_value=round(float(current), 6),
                    prev_value=round(float(prev), 6),
                    pct_change=round(float(pct), 2),
                    data_source='coingecko',
                    date=today,
                    currency='USD',
                    extra_meta={
                        'coin_id': coin['id'],
                        'symbol': coin['symbol'],
                        'market_cap': coin.get('market_cap'),
                        'rank': coin.get('market_cap_rank'),
                    },
                )
                if self.validate_data_point(dp):
                    results.append(dp)

            results.sort(key=lambda x: abs(x.pct_change), reverse=True)
            logger.info('[dataforge] fetch_crypto_movers: %d movers found', len(results))
            return results[:top_n]

        except Exception as e:
            logger.error('[dataforge] fetch_crypto_movers failed: %s', e)
            return []

    # ------------------------------------------------------------------
    # Forex — Alpha Vantage → ExchangeRate-API fallback
    # ------------------------------------------------------------------

    def fetch_forex(self, from_currency: str, to_currency: str) -> 'DataPoint | None':
        """
        Fetch current exchange rate.
        Primary: Alpha Vantage. Switches to ExchangeRate-API at 20 daily calls.

        Returns:
            DataPoint or None on failure.
        """
        global _alpha_vantage_calls_today

        if _alpha_vantage_calls_today < _ALPHA_VANTAGE_DAILY_LIMIT and ALPHA_VANTAGE_KEY:
            result = self._fetch_forex_alpha_vantage(from_currency, to_currency)
            if result:
                _alpha_vantage_calls_today += 1
                return result
            logger.warning('[dataforge] Alpha Vantage forex failed, trying fallback')

        return self._fetch_forex_exchangerate(from_currency, to_currency)

    def _fetch_forex_alpha_vantage(
        self, from_currency: str, to_currency: str
    ) -> 'DataPoint | None':
        try:
            url = 'https://www.alphavantage.co/query'
            params = {
                'function': 'CURRENCY_EXCHANGE_RATE',
                'from_currency': from_currency,
                'to_currency': to_currency,
                'apikey': ALPHA_VANTAGE_KEY,
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            rate_data = data.get('Realtime Currency Exchange Rate', {})
            if not rate_data:
                return None
            rate = float(rate_data.get('5. Exchange Rate', 0))
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            return DataPoint(
                metric_name=f'{from_currency}/{to_currency} exchange rate',
                current_value=round(rate, 4),
                prev_value=0.0,
                pct_change=0.0,
                data_source='alphavantage',
                date=today,
                currency=to_currency,
                extra_meta={'from': from_currency, 'to': to_currency},
            )
        except Exception as e:
            logger.warning('[dataforge] Alpha Vantage forex error: %s', e)
            return None

    def _fetch_forex_exchangerate(
        self, from_currency: str, to_currency: str
    ) -> 'DataPoint | None':
        try:
            if not EXCHANGE_RATE_API_KEY:
                logger.error('[dataforge] EXCHANGE_RATE_API_KEY not set')
                return None
            url = f'https://v6.exchangerate-api.com/v6/{EXCHANGE_RATE_API_KEY}/pair/{from_currency}/{to_currency}'
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get('result') != 'success':
                return None
            rate = float(data.get('conversion_rate', 0))
            today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            return DataPoint(
                metric_name=f'{from_currency}/{to_currency} exchange rate',
                current_value=round(rate, 4),
                prev_value=0.0,
                pct_change=0.0,
                data_source='exchangerate-api',
                date=today,
                currency=to_currency,
                extra_meta={'from': from_currency, 'to': to_currency},
            )
        except Exception as e:
            logger.error('[dataforge] ExchangeRate-API forex error: %s', e)
            return None

    # ------------------------------------------------------------------
    # NewsAPI — context enrichment (not primary data)
    # ------------------------------------------------------------------

    def fetch_news_context(self, query: str, max_results: int = 3) -> list:
        """
        Fetch financial headlines for script context injection.
        Used AFTER a story is selected to enrich the Claude prompt.

        Returns:
            list[str] of headline strings. Empty list on failure.
        """
        try:
            if not NEWS_API_KEY:
                logger.warning('[dataforge] NEWS_API_KEY not set — skipping news context')
                return []
            url = 'https://newsapi.org/v2/everything'
            params = {
                'q': query,
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': max_results,
                'apiKey': NEWS_API_KEY,
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            headlines = [
                article.get('title', '')
                for article in data.get('articles', [])
                if article.get('title')
            ]
            logger.info(
                '[dataforge] fetch_news_context: %d headlines for "%s"',
                len(headlines), query,
            )
            return headlines[:max_results]

        except Exception as e:
            logger.warning('[dataforge] fetch_news_context failed: %s', e)
            return []

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_data_point(self, dp: DataPoint) -> bool:
        """
        Sanity check on a DataPoint.
        Flags >30% moves as suspicious — logs warning but does NOT auto-reject.
        Pipeline can decide whether to post flagged stories.

        Returns:
            True if data point passes basic checks, False if fundamentally invalid.
        """
        if dp.current_value is None or dp.current_value == 0:
            return False
        if dp.pct_change is not None and abs(dp.pct_change) > 30:
            logger.warning(
                '[dataforge] FLAGGED: %s has %.1f%% change — verify before posting',
                dp.metric_name, dp.pct_change,
            )
        return True


# ---------------------------------------------------------------------------
# Local test
# ---------------------------------------------------------------------------

def test_data_fetcher():
    """Quick smoke test — run locally before deploying."""
    logging.basicConfig(level=logging.INFO)
    fetcher = DataFetcher()

    print('\n--- Daily Movers (yfinance) ---')
    movers = fetcher.fetch_daily_movers(top_n=3)
    for m in movers:
        print(f'  {m.metric_name}: ${m.current_value} ({m.pct_change:+.2f}%)')

    print('\n--- FRED: CPI (last 6 periods) ---')
    cpi = fetcher.fetch_fred_series('CPIAUCSL', periods=6)
    print(cpi.to_string(index=False) if not cpi.empty else '  No data returned')

    print('\n--- Crypto Movers (top 3) ---')
    crypto = fetcher.fetch_crypto_movers(top_n=3)
    for c in crypto:
        print(f'  {c.metric_name}: ${c.current_value} ({c.pct_change:+.2f}%)')

    print('\n--- Forex: USD/NGN ---')
    fx = fetcher.fetch_forex('USD', 'NGN')
    if fx:
        print(f'  {fx.metric_name}: {fx.current_value} (source: {fx.data_source})')
    else:
        print('  No forex data returned')

    print('\n--- News Context: Apple stock ---')
    headlines = fetcher.fetch_news_context('Apple stock', max_results=2)
    for h in headlines:
        print(f'  {h}')

    print('\n[dataforge] data_fetcher smoke test complete.')


if __name__ == '__main__':
    test_data_fetcher()
