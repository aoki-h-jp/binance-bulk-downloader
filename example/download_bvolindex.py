# import downloader
from downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="BVOLIndex", asset="option")

# download BVOLIndex
downloader.run_download()
