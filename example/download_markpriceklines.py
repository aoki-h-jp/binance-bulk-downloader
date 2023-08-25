# import binance_bulk_downloader
from binance_bulk_downloader.downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="markPriceKlines")

# download markPriceKlines (frequency: "1m", asset="um")
downloader.run_download()

# download markPriceKlines (frequency: "5m", asset="um")
downloader = BinanceBulkDownloader(data_type="markPriceKlines", data_frequency="5m")
downloader.run_download()

# download markPriceKlines (frequency: "1d", asset="um")
downloader = BinanceBulkDownloader(data_type="markPriceKlines", data_frequency="1d")
downloader.run_download()

# download monthly markPriceKlines (frequency: "1m", asset="um", timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(
    data_type="markPriceKlines", data_frequency="1m", timeperiod_per_file="monthly"
)
downloader.run_download()

# download markPriceKlines (frequency: "1m", asset="cm")
downloader = BinanceBulkDownloader(
    data_type="markPriceKlines", data_frequency="1m", asset="cm"
)
downloader.run_download()

# download markPriceKlines (frequency: "5m", asset="cm")
downloader = BinanceBulkDownloader(
    data_type="markPriceKlines", data_frequency="5m", asset="cm"
)
downloader.run_download()

# download markPriceKlines (frequency: "1d", asset="cm")
downloader = BinanceBulkDownloader(
    data_type="markPriceKlines", data_frequency="1d", asset="cm"
)
downloader.run_download()

# download monthly markPriceKlines (frequency: "1m", asset="cm", timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(
    data_type="markPriceKlines",
    data_frequency="1m",
    asset="cm",
    timeperiod_per_file="monthly",
)
downloader.run_download()
