# import downloader
from downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="bookDepth")

# download bookDepth
downloader.run_download()

# download bookDepth (asset="cm")
downloader = BinanceBulkDownloader(data_type="bookDepth", asset="cm")
downloader.run_download()