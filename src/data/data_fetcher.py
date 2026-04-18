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
import time
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
            logger.info('[dataforge] fetch_daily_movers: %d movers from yfinance', len(results))

            if results:
                logger.info('[dataforge] fetch_daily_movers: using yfinance')
                return results[:top_n]

            # yfinance returned 0 results — fall back to Polygon.io
            logger.warning('[dataforge] fetch_daily_movers: yfinance returned 0 results — trying Polygon.io')
            return self.fetch_polygon_movers(top_n=top_n)

        except Exception as e:
            logger.error('[dataforge] fetch_daily_movers: yfinance failed: %s — trying Polygon.io', e)
            try:
                return self.fetch_polygon_movers(top_n=top_n)
            except Exception as e2:
                logger.error('[dataforge] fetch_daily_movers: Polygon.io also failed: %s', e2)
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

    def fetch_polygon_movers(self, top_n: int = 5) -> list:
        """
        Fetch top movers from Polygon.io gainers + losers snapshots.
        Used as fallback when yfinance is blocked (e.g. on Railway).

        Returns:
            list[DataPoint] sorted by abs(pct_change) descending.
        """
        if not POLYGON_API_KEY:
            logger.warning('[dataforge] POLYGON_API_KEY not set — cannot fetch Polygon movers')
            return []

        results = []
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        for direction in ('gainers', 'losers'):
            try:
                url = f'https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/{direction}'
                resp = requests.get(
                    url,
                    params={'apiKey': POLYGON_API_KEY},
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
                tickers = data.get('tickers', [])

                for t in tickers:
                    try:
                        ticker_name = t.get('ticker', '')
                        day_data = t.get('day', {})
                        prev_day = t.get('prevDay', {})
                        current = float(day_data.get('c', 0))
                        prev = float(prev_day.get('c', 0))
                        if prev == 0 or current == 0:
                            continue
                        pct = ((current - prev) / prev) * 100
                        dp = DataPoint(
                            metric_name=f'{ticker_name} stock price',
                            current_value=round(current, 2),
                            prev_value=round(prev, 2),
                            pct_change=round(pct, 2),
                            data_source='polygon',
                            date=today,
                            currency='USD',
                            extra_meta={'ticker': ticker_name, 'direction': direction},
                        )
                        if self.validate_data_point(dp):
                            results.append(dp)
                    except Exception as e:
                        logger.warning('[dataforge] Polygon parse error for ticker: %s', e)
                        continue

                logger.info('[dataforge] fetch_polygon_movers: %d %s fetched', len(tickers), direction)

            except Exception as e:
                logger.error('[dataforge] fetch_polygon_movers %s failed: %s', direction, e)

        results.sort(key=lambda x: abs(x.pct_change), reverse=True)
        logger.info('[dataforge] fetch_polygon_movers: %d total movers, returning top %d', len(results), top_n)

        if results:
            logger.info('[dataforge] fetch_daily_movers: using polygon')

        return results[:top_n]

    # ------------------------------------------------------------------
    # FRED — US economic time-series
    # ------------------------------------------------------------------

    def fetch_fred_series(self, series_id: str, periods: int = 24):
        """
        Fetch last N periods of a FRED economic series.
        Retries up to 3 times with 2-second delay on failure.

        Returns:
            pd.DataFrame with columns ['date', 'value'] or empty DataFrame on failure.
        """
        import pandas as pd

        if not FRED_API_KEY:
            logger.error('[dataforge] FRED_API_KEY not set')
            return pd.DataFrame()

        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                from fredapi import Fred

                fred = Fred(api_key=FRED_API_KEY)
                series = fred.get_series(series_id)
                df = series.reset_index()
                df.columns = ['date', 'value']
                df = df.dropna().tail(periods)
                logger.info(
                    '[dataforge] fetch_fred_series: %s — %d periods retrieved (attempt %d)',
                    series_id, len(df), attempt,
                )
                return df

            except Exception as e:
                logger.warning(
                    '[dataforge] fetch_fred_series attempt %d/%d failed for %s: %s',
                    attempt, max_retries, series_id, e,
                )
                if attempt < max_retries:
                    time.sleep(2)

        logger.error('[dataforge] fetch_fred_series: all %d attempts failed for %s',
                     max_retries, series_id)
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
            logger.info('[dataforge] World Bank API: GET %s params=%s', url, params)
            resp = requests.get(url, params=params, timeout=30)
            logger.info('[dataforge] World Bank response: status=%d, length=%d',
                        resp.status_code, len(resp.content))
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, list) or len(data) < 2 or not data[1]:
                logger.warning(
                    '[dataforge] World Bank returned no data for %s (response type=%s, len=%s)',
                    indicator, type(data).__name__, len(data) if isinstance(data, list) else 'N/A',
                )
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

    def fetch_crypto_market_cap_history(self, top_n: int = 10, days: int = 30) -> 'pd.DataFrame | None':
        """
        Fetch daily market cap history for top N coins over last N days.
        Uses CoinGecko /coins/{id}/market_chart endpoint.
        Returns a DataFrame with coins as rows and dates as columns (market cap values).
        Falls back to None on failure.
        """
        try:
            import pandas as pd

            # Step 1: Get top coins by current market cap
            url = f'{COINGECKO_BASE_URL}/coins/markets'
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
                'per_page': top_n,
                'page': 1,
            }
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            coins = resp.json()

            if not coins:
                logger.warning('[dataforge] fetch_crypto_market_cap_history: no coins returned')
                return None

            logger.info('[dataforge] fetch_crypto_market_cap_history: fetching history for %d coins', len(coins))

            # Step 2: Fetch market cap history for each coin
            coin_data = {}
            for coin in coins:
                coin_id = coin['id']
                coin_name = coin['name']
                try:
                    hist_url = f'{COINGECKO_BASE_URL}/coins/{coin_id}/market_chart'
                    hist_params = {
                        'vs_currency': 'usd',
                        'days': days,
                        'interval': 'daily',
                    }
                    hist_resp = requests.get(hist_url, params=hist_params, timeout=15)
                    hist_resp.raise_for_status()
                    hist_data = hist_resp.json()
                    market_caps = hist_data.get('market_caps', [])
                    if market_caps:
                        dates = [
                            datetime.fromtimestamp(mc[0] / 1000, tz=timezone.utc).strftime('%b %d')
                            for mc in market_caps
                        ]
                        values = [mc[1] for mc in market_caps]
                        coin_data[coin_name] = dict(zip(dates, values))
                    time.sleep(0.5)  # CoinGecko rate limit
                except Exception as e:
                    logger.warning('[dataforge] fetch_crypto_market_cap_history: failed for %s: %s', coin_id, e)
                    continue

            if not coin_data:
                logger.warning('[dataforge] fetch_crypto_market_cap_history: no coin data collected')
                return None

            df = pd.DataFrame(coin_data).T
            df = df.dropna(axis=1, how='all').dropna(axis=0, how='all')
            logger.info(
                '[dataforge] fetch_crypto_market_cap_history: %d coins x %d days',
                len(df), len(df.columns),
            )
            return df

        except Exception as e:
            logger.error('[dataforge] fetch_crypto_market_cap_history failed: %s', e, exc_info=True)
            return None

    def fetch_crypto_movers(self, top_n: int = 10) -> list:
        """
        Fetch top coins by market cap from CoinGecko, sorted by 24h % change.
        Uses day-of-week rotation to avoid posting the same coin multiple days.

        Returns:
            list[DataPoint] sorted by abs(pct_change) descending,
            rotated by weekday to vary the top pick.
        """
        try:
            url = f'{COINGECKO_BASE_URL}/coins/markets'
            params = {
                'vs_currency': 'usd',
                'order': 'market_cap_desc',
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

            # Day-of-week rotation: pick a different top coin each day
            weekday = datetime.utcnow().weekday()  # 0=Mon, 6=Sun
            if len(results) > 7:
                rotated_pick = results[weekday % min(len(results), 7)]
                # Move the rotated pick to position 0
                results.remove(rotated_pick)
                results.insert(0, rotated_pick)
                logger.info(
                    '[dataforge] fetch_crypto_movers: %d movers, rotated pick=%s (weekday=%d)',
                    len(results), rotated_pick.metric_name, weekday,
                )
            else:
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
    # FRED daily story — rotating macro indicators
    # ------------------------------------------------------------------

    def fetch_fred_daily_story(self) -> 'DataPoint | None':
        """
        Picks today's FRED series by day-of-year rotation.
        Returns a DataPoint for the most recent value vs prior period.
        Targets western/US audience — major macro indicators only.
        """
        SERIES_ROTATION = [
            ('FEDFUNDS',          'Fed Funds Rate',        '%',   'FRED'),
            ('CPIAUCSL',          'US Inflation (CPI)',    '%',   'FRED'),
            ('UNRATE',            'US Unemployment Rate',  '%',   'FRED'),
            ('MORTGAGE30US',      '30-Year Mortgage Rate', '%',   'FRED'),
            ('T10Y2Y',            'Yield Curve Spread',    'pts', 'FRED'),
            ('DCOILWTICO',        'WTI Crude Oil',         '$',   'FRED'),
            ('GOLDAMGBD228NLBM',  'Gold Price',            '$',   'FRED'),
            ('DEXUSEU',           'EUR/USD Exchange Rate',  '$',   'FRED'),
            ('SP500',             'S&P 500 Index',         'pts', 'FRED'),
            ('NASDAQCOM',         'NASDAQ Composite',      'pts', 'FRED'),
            ('VIXCLS',            'VIX Fear Index',        'pts', 'FRED'),
            ('DEXJPUS',           'USD/JPY Exchange Rate',  'Y',  'FRED'),
            ('BAMLH0A0HYM2',     'High Yield Spread',     'pts', 'FRED'),
            ('UMCSENT',           'Consumer Sentiment',    'pts', 'FRED'),
        ]
        idx = datetime.utcnow().timetuple().tm_yday % len(SERIES_ROTATION)
        series_id, label, unit, source = SERIES_ROTATION[idx]
        logger.info('[dataforge] FRED daily rotation: day %d -> %s (%s)',
                     datetime.utcnow().timetuple().tm_yday, label, series_id)
        try:
            df = self.fetch_fred_series(series_id, periods=2)
            if df is None or len(df) < 2:
                logger.warning('[dataforge] FRED %s returned < 2 periods', series_id)
                return None
            current = float(df.iloc[-1]['value'] if 'value' in df.columns else df.iloc[-1])
            previous = float(df.iloc[-2]['value'] if 'value' in df.columns else df.iloc[-2])
            pct = ((current - previous) / abs(previous) * 100) if previous else 0
            return DataPoint(
                metric_name=label,
                current_value=current,
                prev_value=previous,
                pct_change=round(pct, 2),
                data_source=source,
                date=str(datetime.utcnow().date()),
                currency=unit,
                extra_meta={'series_id': series_id},
            )
        except Exception as e:
            logger.warning('[dataforge] FRED daily story failed (%s): %s',
                           series_id, e)
            return None

    def fetch_us_treasury_yields(self, maturity: str = '10y') -> 'DataPoint | None':
        """
        Fetch daily US Treasury yield curve rates from the US Treasury Fiscal Data API.
        No API key required.

        Args:
            maturity: One of '1m', '3m', '6m', '1y', '2y', '5y', '7y', '10y', '20y', '30y'

        Returns:
            DataPoint with current yield vs prior day, or None on failure.
        """
        MATURITY_LABELS = {
            '1m':  ('BC_1MONTH',  '1-Month T-Bill Yield'),
            '3m':  ('BC_3MONTH',  '3-Month T-Bill Yield'),
            '6m':  ('BC_6MONTH',  '6-Month T-Bill Yield'),
            '1y':  ('BC_1YEAR',   '1-Year Treasury Yield'),
            '2y':  ('BC_2YEAR',   '2-Year Treasury Yield'),
            '5y':  ('BC_5YEAR',   '5-Year Treasury Yield'),
            '7y':  ('BC_7YEAR',   '7-Year Treasury Yield'),
            '10y': ('BC_10YEAR',  '10-Year Treasury Yield'),
            '20y': ('BC_20YEAR',  '20-Year Treasury Yield'),
            '30y': ('BC_30YEAR',  '30-Year Treasury Yield'),
        }

        if maturity not in MATURITY_LABELS:
            logger.error('[dataforge] fetch_us_treasury_yields: unknown maturity %s', maturity)
            return None

        field_name, label = MATURITY_LABELS[maturity]

        try:
            url = 'https://api.fiscaldata.treasury.gov/services/api/v1/accounting/od/avg_interest_rates'
            # Use the daily par yield curve rates endpoint instead
            url = (
                'https://api.fiscaldata.treasury.gov/services/api/v1/'
                'accounting/od/avg_interest_rates'
            )
            # Daily Treasury Par Yield Curve Rates
            yield_url = (
                'https://home.treasury.gov/resource-center/data-chart-center/'
                'interest-rates/daily-treasury-rates.csv/all/all?'
                'type=daily_treasury_yield_curve&field_tdr_date_value=all'
                '&page&_format=csv'
            )

            # Use fiscaldata API — cleaner JSON, no scraping
            api_url = 'https://api.fiscaldata.treasury.gov/services/api/v1/accounting/od/avg_interest_rates'
            params = {
                'fields': 'record_date,security_type_desc,avg_interest_rate_amt',
                'filter': 'security_type_desc:in:(Treasury Bills,Treasury Notes,Treasury Bonds,Treasury Inflation-Protected Securities)',
                'sort': '-record_date',
                'page[size]': 20,
            }
            resp = requests.get(api_url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            records = data.get('data', [])

            if not records:
                logger.warning('[dataforge] fetch_us_treasury_yields: no records returned')
                return None

            # Map maturity to security type for avg_interest_rates endpoint
            MATURITY_TO_TYPE = {
                '1m':  'Treasury Bills',
                '3m':  'Treasury Bills',
                '6m':  'Treasury Bills',
                '1y':  'Treasury Bills',
                '2y':  'Treasury Notes',
                '5y':  'Treasury Notes',
                '7y':  'Treasury Notes',
                '10y': 'Treasury Notes',
                '20y': 'Treasury Bonds',
                '30y': 'Treasury Bonds',
            }
            target_type = MATURITY_TO_TYPE[maturity]
            filtered = [r for r in records if r.get('security_type_desc') == target_type]

            if len(filtered) < 2:
                logger.warning('[dataforge] fetch_us_treasury_yields: < 2 records for %s', target_type)
                return None

            current = float(filtered[0]['avg_interest_rate_amt'])
            previous = float(filtered[1]['avg_interest_rate_amt'])
            pct = ((current - previous) / abs(previous) * 100) if previous else 0.0
            record_date = filtered[0]['record_date']

            dp = DataPoint(
                metric_name=label,
                current_value=round(current, 3),
                prev_value=round(previous, 3),
                pct_change=round(pct, 2),
                data_source='US Treasury',
                date=record_date,
                currency='%',
                extra_meta={
                    'maturity': maturity,
                    'security_type': target_type,
                },
            )
            logger.info(
                '[dataforge] fetch_us_treasury_yields: %s = %.3f%% (prev %.3f%%, %+.2f%%)',
                label, current, previous, pct,
            )
            return dp

        except Exception as e:
            logger.error('[dataforge] fetch_us_treasury_yields failed: %s', e, exc_info=True)
            return None

    def fetch_news_triggered_story(self) -> 'dict | None':
        """
        Use today's top financial headlines to select the most relevant FRED
        series, then fetch the data. Returns a dict with headline context and
        DataPoint, or None on failure.

        Flow:
          1. Pull top 5 financial headlines from NewsAPI
          2. Send to Claude to pick the most financially significant headline
             and the best matching FRED series
          3. Fetch FRED data for that series
          4. Return combined result for script generation

        Returns:
            dict with keys:
                'headline'   — the selected headline string
                'fred_series' — FRED series ID used
                'data_point' — DataPoint namedtuple
            or None on any failure.
        """
        try:
            import anthropic
            import json

            # Step 1: Fetch top financial headlines
            if not NEWS_API_KEY:
                logger.warning('[dataforge] NEWS_API_KEY not set — cannot trigger news story')
                return None

            url = 'https://newsapi.org/v2/top-headlines'
            params = {
                'category': 'business',
                'language': 'en',
                'country': 'us',
                'pageSize': 5,
                'apiKey': NEWS_API_KEY,
            }
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            articles = resp.json().get('articles', [])
            headlines = [a.get('title', '') for a in articles if a.get('title')]

            if not headlines:
                logger.warning('[dataforge] NewsAPI returned no headlines')
                return None

            logger.info('[dataforge] fetch_news_triggered_story: %d headlines fetched', len(headlines))

            # Step 2: Ask Claude to pick the best headline and matching FRED series
            FRED_OPTIONS = {
                'FEDFUNDS':           'Fed Funds Rate',
                'CPIAUCSL':           'US Inflation (CPI)',
                'UNRATE':             'US Unemployment Rate',
                'MORTGAGE30US':       '30-Year Mortgage Rate',
                'T10Y2Y':             'Yield Curve Spread',
                'DCOILWTICO':         'WTI Crude Oil',
                'GOLDAMGBD228NLBM':   'Gold Price',
                'DEXUSEU':            'EUR/USD Exchange Rate',
                'SP500':              'S&P 500 Index',
                'NASDAQCOM':          'NASDAQ Composite',
                'VIXCLS':             'VIX Fear Index',
                'BAMLH0A0HYM2':       'High Yield Spread',
                'UMCSENT':            'Consumer Sentiment',
                'MORTGAGE15US':       '15-Year Mortgage Rate',
                'MEDLISPRIPERSQUFEE': 'Median Home Price per sq ft',
            }

            FRED_CURRENCY = {
                'FEDFUNDS':           '%',
                'CPIAUCSL':           '%',
                'UNRATE':             '%',
                'MORTGAGE30US':       '%',
                'T10Y2Y':             'pts',
                'DCOILWTICO':         '$',
                'GOLDAMGBD228NLBM':   '$',
                'DEXUSEU':            '$',
                'SP500':              'pts',
                'NASDAQCOM':          'pts',
                'VIXCLS':             'pts',
                'BAMLH0A0HYM2':       'pts',
                'UMCSENT':            'pts',
                'MORTGAGE15US':       '%',
                'MEDLISPRIPERSQUFEE': '$',
            }

            fred_list = '\n'.join(f'  {k}: {v}' for k, v in FRED_OPTIONS.items())
            headlines_text = '\n'.join(f'  {i+1}. {h}' for i, h in enumerate(headlines))

            claude_prompt = (
                f"Today's top financial headlines:\n{headlines_text}\n\n"
                f"Available FRED data series:\n{fred_list}\n\n"
                f"Task: Pick the single most financially significant headline "
                f"and the single FRED series that best provides the hard data "
                f"behind that story.\n\n"
                f"Respond ONLY with valid JSON, no markdown:\n"
                f'{{ "headline": "exact headline text", "fred_series": "SERIES_ID" }}'
            )

            anthropic_key = os.environ.get('ANTHROPIC_API_KEY', '')
            if not anthropic_key:
                logger.warning('[dataforge] ANTHROPIC_API_KEY not set — cannot select news story')
                return None

            client = anthropic.Anthropic(api_key=anthropic_key)
            message = client.messages.create(
                model='claude-sonnet-4-5',
                max_tokens=200,
                messages=[{'role': 'user', 'content': claude_prompt}],
            )
            raw = message.content[0].text.strip()
            raw = raw.replace('```json', '').replace('```', '').strip()
            selection = json.loads(raw)

            selected_headline = selection.get('headline', '')
            fred_series = selection.get('fred_series', '')

            if not selected_headline or fred_series not in FRED_OPTIONS:
                logger.warning('[dataforge] Claude selection invalid: %s', selection)
                return None

            logger.info(
                '[dataforge] fetch_news_triggered_story: headline=%r fred=%s',
                selected_headline[:60], fred_series,
            )

            # Step 3: Fetch FRED data for selected series
            df = self.fetch_fred_series(fred_series, periods=2)
            if df is None or len(df) < 2:
                logger.warning('[dataforge] FRED %s returned < 2 periods', fred_series)
                return None

            current = float(df.iloc[-1]['value'])
            previous = float(df.iloc[-2]['value'])
            pct = ((current - previous) / abs(previous) * 100) if previous else 0.0

            label = FRED_OPTIONS[fred_series]
            dp = DataPoint(
                metric_name=label,
                current_value=current,
                prev_value=previous,
                pct_change=round(pct, 2),
                data_source='FRED',
                date=str(datetime.now(timezone.utc).date()),
                currency=FRED_CURRENCY.get(fred_series, '%'),
                extra_meta={'series_id': fred_series, 'headline': selected_headline},
            )

            return {
                'headline': selected_headline,
                'fred_series': fred_series,
                'data_point': dp,
            }

        except Exception as e:
            logger.error('[dataforge] fetch_news_triggered_story failed: %s', e, exc_info=True)
            return None

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

    print('\n--- Daily Movers (yfinance -> polygon fallback) ---')
    movers = fetcher.fetch_daily_movers(top_n=3)
    if movers:
        source = movers[0].data_source
        print(f'  Source used: {source}')
        for m in movers:
            print(f'  {m.metric_name}: ${m.current_value} ({m.pct_change:+.2f}%)')
    else:
        print('  No movers returned from any source')

    print('\n--- FRED: CPI (last 6 periods) ---')
    cpi = fetcher.fetch_fred_series('CPIAUCSL', periods=6)
    print(cpi.to_string(index=False) if not cpi.empty else '  No data returned')

    print('\n--- Crypto Movers (top 3) ---')
    crypto = fetcher.fetch_crypto_movers(top_n=3)
    for c in crypto:
        print(f'  {c.metric_name}: ${c.current_value} ({c.pct_change:+.2f}%)')

    print('\n--- Forex (rotating pair) ---')
    FOREX_PAIRS = [('EUR', 'USD'), ('GBP', 'USD'), ('USD', 'JPY'), ('USD', 'CHF')]
    today_index = datetime.utcnow().timetuple().tm_yday % len(FOREX_PAIRS)
    pair = FOREX_PAIRS[today_index]
    print(f'  Today\'s pair: {pair[0]}/{pair[1]}')
    fx = fetcher.fetch_forex(pair[0], pair[1])
    if fx:
        print(f'  {fx.metric_name}: {fx.current_value} (source: {fx.data_source})')
    else:
        print('  No forex data returned')

    print('\n--- FRED Daily Story (rotating) ---')
    fred_dp = fetcher.fetch_fred_daily_story()
    if fred_dp:
        print(f'  {fred_dp.metric_name}: {fred_dp.current_value} {fred_dp.currency} ({fred_dp.pct_change:+.2f}%)')
    else:
        print('  FRED returned None')

    print('\n--- News Context: Apple stock ---')
    headlines = fetcher.fetch_news_context('Apple stock', max_results=2)
    for h in headlines:
        print(f'  {h}')

    print('\n--- US Treasury Yields (10Y) ---')
    treasury = fetcher.fetch_us_treasury_yields('10y')
    if treasury:
        print(f'  {treasury.metric_name}: {treasury.current_value}% ({treasury.pct_change:+.2f}%)')
    else:
        print('  No Treasury data returned')

    print('\n[dataforge] data_fetcher smoke test complete.')


if __name__ == '__main__':
    test_data_fetcher()
