# import binance_bulk_downloader
from binance_bulk_downloader.downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="EOHSummary", asset="option")

# download EOHSummary
downloader.run_download()
