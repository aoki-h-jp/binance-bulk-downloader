# import binance_bulk_downloader
from binance_bulk_downloader.downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="bookTicker")

# download bookTicker
downloader.run_download()

# download bookTicker (asset="um", timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(
    data_type="bookTicker", timeperiod_per_file="monthly"
)
downloader.run_download()

# download bookTicker (asset="cm")
downloader = BinanceBulkDownloader(data_type="bookTicker", asset="cm")
downloader.run_download()

# download bookTicker (asset="cm", timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(
    data_type="bookTicker", asset="cm", timeperiod_per_file="monthly"
)
downloader.run_download()
