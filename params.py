from sqlalchemy import types
import utils

rename_columns = {"fiscalDateEnding": 'fiscal_date_ending',
                  "reportedDate": 'reported_date',
                  "reportedEPS": 'reported_eps',
                  "estimatedEPS": 'estimated_eps',
                  "surprise": 'surprise',
                  "surprisePercentage": 'surprise_percentage'}

column_types = {'fiscal_date_ending': types.Date,
                'reported_date': types.Date,
                'reported_eps': types.Numeric,
                'estimated_eps': types.Numeric,
                'surprise': types.Numeric,
                'surprise_percentage': types.Numeric}

intervals = ['1d']

earnings_tickers = {row['symbol']: row['internal_symbol'] for _, row in utils.get_american_symbols().iterrows()}

ratios = ['pe-ratio', 'price-sales', 'price-book', 'price-fcf', 'current-ratio',
          'quick-ratio', 'debt-equity-ratio', 'roe', 'roa', 'roi', 'return-on-tangible-equity']

tickers = {
    "BTC-USD": "btcusd",
    "ETH-USD": "ethusd",
    "BNB-USD": "bnbusd",
    "ADA-USD": "adausd",
    "FTT-USD": "fttusd",
    "DOT-USD": "dotusd",
    "LTC-USD": "ltcusd",
    "DOGE-USD": "dogeusd",
    "MATIC-USD": "maticusd",
    "XRP-USD": "xrpusd",
    "SOL-USD": "solusd",
    "HEX-USD": "hexusd",
    "WTRX-USD": "wtrsusd",
    "AVAX-USD": "avaxusd",
    "SHIB-USD": "shibusd",
    "TRX-USD": "trxusd",
    "LEO-USD": "leousd",
    "YOUC-USD": "youcusd",
    "UNI-USD": "uniusd",
    "ETC-USD": "etcusd",
    "LINK-USD": "linkusd",
    "CRO-USD": "crousd",
    "NEAR-USD": "nearusd",
    "XLM-USD": "xlmusd",
    "XMR-USD": "xmrusd",
    "ATOM-USD": "atomusd",
    "BCH-USD": "bchusd",
    "ALGO-USD": "algousd",

    "^GSPC": "sp500",
    "^DJI": "dowjones30",
    "^IXIC": "nasdaq",
    "^RUT": "russel2000",
    "GC=F": "gold",
    "CL=F": "oil",
    "^CMC200": "cmc_crypto_200",
    "^FTSE": "ftse100",
    "^TNX": "treasury_yield_10_years",
    "^VIX": "vix",

    "EURUSD=X": "eurusd",
    "ARS=X": "arsusd"

}
tickers.update(earnings_tickers)


