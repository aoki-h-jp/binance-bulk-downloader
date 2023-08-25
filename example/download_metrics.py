# import binance_bulk_downloader
from binance_bulk_downloader.downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="metrics")

# download metrics
downloader.run_download()

# download metrics (asset="cm")
downloader = BinanceBulkDownloader(data_type="metrics", asset="cm")
downloader.run_download()
