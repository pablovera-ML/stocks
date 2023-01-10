from sqlalchemy import types

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

intervals = ['1d', '1wk', '1mo', '3mo']

earnings_tickers = {"TSLA": "tsla",
                    "META": "meta",
                    "MELI": "meli",
                    "VALE": "vale",
                    "SPY": "spy",
                    "AAPL": "aapl",
                    "MSFT": "msft",
                    "KO": "ko",
                    "GOOG": "goog",
                    "MSTR": "mstr"}

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

    "BITO": "pro_shares_btc_etf",

    "TSLA": "tsla",
    "META": "meta",
    "MELI": "meli",
    "VALE": "vale",
    "SPY": "spy",
    "AAPL": "aapl",
    "MSFT": "msft",
    "KO": "ko",
    "GOOG": "goog",
    "MSTR": "mstr",

    "PHO": "pho",
    "FIW": "fiw",
    "CGW": "cgw",
    "AWK": "awk",
    "WTRG": "wtrg",
    "XYL": "xyl",
    "ECL": "ecl",

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
    "XLF": "xlf",
    "XLE": "xle",
    "EEM": "eem",
    "QQQ": "qqq_nasdaq_100",
    "IWM": "iwm_russel",
    "DIA": "dia",

    "EURUSD=X": "eurusd",
    "ARS=X": "arsusd"

}


