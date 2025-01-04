"""
Download spot market data for specific symbols
"""

from binance_bulk_downloader.downloader import BinanceBulkDownloader

# Download single symbol (BTCUSDT) from spot market
downloader = BinanceBulkDownloader(
    data_type="klines",
    data_frequency="1h",
    asset="spot",
    timeperiod_per_file="daily",
    symbols="BTCUSDT",
)
downloader.run_download()

# Download multiple symbols (BTCUSDT and ETHUSDT) from spot market
downloader = BinanceBulkDownloader(
    data_type="klines",
    data_frequency="1d",
    asset="spot",
    timeperiod_per_file="monthly",
    symbols=["BTCUSDT", "ETHUSDT"],
)
downloader.run_download()

# Download trades data for specific symbols
downloader = BinanceBulkDownloader(
    data_type="trades",
    asset="spot",
    timeperiod_per_file="daily",
    symbols=["BTCUSDT", "ETHUSDT", "BNBUSDT"],
)
downloader.run_download()

# Download aggTrades data for specific symbols
downloader = BinanceBulkDownloader(
    data_type="aggTrades",
    asset="spot",
    timeperiod_per_file="monthly",
    symbols=["BTCUSDT", "ETHUSDT"],
)
downloader.run_download()
