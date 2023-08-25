# import binance_bulk_downloader
from binance_bulk_downloader.downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(
    data_type="fundingRate", timeperiod_per_file="monthly"
)

# download fundingRate
downloader.run_download()

# download fundingRate (asset="cm")
downloader = BinanceBulkDownloader(
    data_type="fundingRate", asset="cm", timeperiod_per_file="monthly"
)
downloader.run_download()
