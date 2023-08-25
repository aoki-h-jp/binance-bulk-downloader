# import binance_bulk_downloader
from binance_bulk_downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="liquidationSnapshot")

# download liquidationSnapshot
downloader.run_download()

# download liquidationSnapshot (asset="cm")
downloader = BinanceBulkDownloader(data_type="liquidationSnapshot", asset="cm")
downloader.run_download()
