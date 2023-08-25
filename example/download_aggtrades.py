# import downloader
from downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="aggTrades")

# download aggTrades (asset="um")
downloader.run_download()

# download monthly aggTrades (timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(data_type="aggTrades", timeperiod_per_file="monthly")
downloader.run_download()

# download aggTrades (asset="cm")
downloader = BinanceBulkDownloader(data_type="aggTrades", asset="cm")
downloader.run_download()

# download monthly aggTrades (asset="cm", timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(data_type="aggTrades", asset="cm", timeperiod_per_file="monthly")
downloader.run_download()
