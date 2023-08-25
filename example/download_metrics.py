# import downloader
from downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="metrics")

# download metrics
downloader.run_download()

# download metrics (asset="cm")
downloader = BinanceBulkDownloader(data_type="metrics", asset="cm")
downloader.run_download()
