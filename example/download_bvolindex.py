# import binance_bulk_downloader
from binance_bulk_downloader.downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="BVOLIndex", asset="option")

# download BVOLIndex
downloader.run_download()
