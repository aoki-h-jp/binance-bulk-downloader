# import downloader
from downloader import BinanceBulkDownloader

# generate instance
downloader = BinanceBulkDownloader(data_type="markpriceKlines")

# download markpriceKlines (frequency: "1m", asset="um")
downloader.run_download()

# download markpriceKlines (frequency: "5m", asset="um")
downloader = BinanceBulkDownloader(data_type="markpriceKlines", data_frequency="5m")
downloader.run_download()

# download markpriceKlines (frequency: "1d", asset="um")
downloader = BinanceBulkDownloader(data_type="markpriceKlines", data_frequency="1d")
downloader.run_download()

# download monthly markpriceKlines (frequency: "1m", asset="um", timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(data_type="markpriceKlines", data_frequency="1m", timeperiod_per_file="monthly")
downloader.run_download()

# download markpriceKlines (frequency: "1m", asset="cm")
downloader = BinanceBulkDownloader(data_type="markpriceKlines", data_frequency="1m", asset="cm")
downloader.run_download()

# download markpriceKlines (frequency: "5m", asset="cm")
downloader = BinanceBulkDownloader(data_type="markpriceKlines", data_frequency="5m", asset="cm")
downloader.run_download()

# download markpriceKlines (frequency: "1d", asset="cm")
downloader = BinanceBulkDownloader(data_type="markpriceKlines", data_frequency="1d", asset="cm")
downloader.run_download()

# download monthly markpriceKlines (frequency: "1m", asset="cm", timeperiod_per_file="monthly")
downloader = BinanceBulkDownloader(data_type="markpriceKlines", data_frequency="1m", asset="cm", timeperiod_per_file="monthly")
downloader.run_download()
