"""
Test spot market symbols filtering
"""

from binance_bulk_downloader.downloader import BinanceBulkDownloader
from binance_bulk_downloader.exceptions import BinanceBulkDownloaderDownloadError


def test_single_symbol_klines(tmpdir):
    """Test downloading klines data for a single symbol"""
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type="klines",
        data_frequency="1h",
        asset="spot",
        timeperiod_per_file="daily",
        symbols="BTCUSDT",
    )
    try:
        downloader._check_params()  # Test parameter validation only
        assert True
    except BinanceBulkDownloaderDownloadError:
        assert False, "Valid parameters should not raise an error"


def test_multiple_symbols_klines(tmpdir):
    """Test downloading klines data for multiple symbols"""
    symbols = ["BTCUSDT", "ETHUSDT"]
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type="klines",
        data_frequency="1h",
        asset="spot",
        timeperiod_per_file="daily",
        symbols=symbols,
    )
    try:
        downloader._check_params()  # Test parameter validation only
        assert True
    except BinanceBulkDownloaderDownloadError:
        assert False, "Valid parameters should not raise an error"


def test_multiple_symbols_trades(tmpdir):
    """Test downloading trades data for multiple symbols"""
    symbols = ["BTCUSDT", "ETHUSDT"]
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type="trades",
        asset="spot",
        timeperiod_per_file="daily",
        symbols=symbols,
    )
    try:
        downloader._check_params()  # Test parameter validation only
        assert True
    except BinanceBulkDownloaderDownloadError:
        assert False, "Valid parameters should not raise an error"


def test_multiple_symbols_aggtrades(tmpdir):
    """Test downloading aggTrades data for multiple symbols"""
    symbols = ["BTCUSDT", "ETHUSDT"]
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type="aggTrades",
        asset="spot",
        timeperiod_per_file="daily",
        symbols=symbols,
    )
    try:
        downloader._check_params()  # Test parameter validation only
        assert True
    except BinanceBulkDownloaderDownloadError:
        assert False, "Valid parameters should not raise an error"


def test_invalid_symbol(tmpdir):
    """Test downloading data with invalid symbol"""
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type="klines",
        data_frequency="1h",
        asset="spot",
        timeperiod_per_file="daily",
        symbols="INVALID_SYMBOL",
    )
    # Verify that parameter validation passes
    downloader._check_params()

    # Check if file list is empty for invalid symbol
    prefix = downloader._build_prefix()
    files = downloader._get_file_list_from_s3_bucket(prefix)
    assert len(files) == 0, "Invalid symbol should return empty file list"


def test_empty_symbols_list(tmpdir):
    """Test downloading data with empty symbols list"""
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type="klines",
        data_frequency="1h",
        asset="spot",
        timeperiod_per_file="daily",
        symbols=[],
    )
    try:
        downloader._check_params()  # Test parameter validation only
        assert True
    except BinanceBulkDownloaderDownloadError:
        assert False, "Valid parameters should not raise an error"
