# import binance_bulk_downloader
from binance_bulk_downloader.downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="trades")

# download trades (asset="um")
downloader.run_download()

# download monthly trades (timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(data_type="trades", timeperiod_per_file="monthly")
downloader.run_download()

# download trades (asset="cm")
downloader = BinanceBulkDownloader(data_type="trades", asset="cm")
downloader.run_download()

# download monthly trades (asset="cm", timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(
    data_type="trades", asset="cm", timeperiod_per_file="monthly"
)
downloader.run_download()

# download trades (asset="spot")
downloader = BinanceBulkDownloader(data_type="trades", asset="spot")
downloader.run_download()

# download monthly trades (asset="spot", timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(
    data_type="trades", asset="spot", timeperiod_per_file="monthly"
)
downloader.run_download()
