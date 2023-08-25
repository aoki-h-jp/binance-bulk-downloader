# import downloader
from downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="EOHSummary", asset="option")

# download EOHSummary
downloader.run_download()
