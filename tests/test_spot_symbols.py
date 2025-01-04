"""
Test spot market symbols filtering
"""

import os

import pytest

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
    downloader.run_download()

    # Check if downloaded files contain only BTCUSDT
    for file in os.listdir(tmpdir):
        if file.endswith(".csv"):
            assert "BTCUSDT" in file


def test_multiple_symbols_klines(tmpdir):
    """Test downloading klines data for multiple symbols"""
    symbols = ["BTCUSDT", "ETHUSDT"]
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type="klines",
        data_frequency="1d",
        asset="spot",
        timeperiod_per_file="monthly",
        symbols=symbols,
    )
    downloader.run_download()

    # Check if downloaded files contain only specified symbols
    for file in os.listdir(tmpdir):
        if file.endswith(".csv"):
            assert any(symbol in file for symbol in symbols)


def test_multiple_symbols_trades(tmpdir):
    """Test downloading trades data for multiple symbols"""
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type="trades",
        asset="spot",
        timeperiod_per_file="daily",
        symbols=symbols,
    )
    downloader.run_download()

    # Check if downloaded files contain only specified symbols
    for file in os.listdir(tmpdir):
        if file.endswith(".csv"):
            assert any(symbol in file for symbol in symbols)


def test_multiple_symbols_aggtrades(tmpdir):
    """Test downloading aggTrades data for multiple symbols"""
    symbols = ["BTCUSDT", "ETHUSDT"]
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type="aggTrades",
        asset="spot",
        timeperiod_per_file="monthly",
        symbols=symbols,
    )
    downloader.run_download()

    # Check if downloaded files contain only specified symbols
    for file in os.listdir(tmpdir):
        if file.endswith(".csv"):
            assert any(symbol in file for symbol in symbols)


def test_invalid_symbol(tmpdir):
    """Test downloading data with invalid symbol"""
    with pytest.raises(BinanceBulkDownloaderDownloadError):
        downloader = BinanceBulkDownloader(
            destination_dir=tmpdir,
            data_type="klines",
            data_frequency="1h",
            asset="spot",
            timeperiod_per_file="daily",
            symbols="INVALID_SYMBOL",
        )
        downloader.run_download()


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
    downloader.run_download()

    # Check if files are downloaded (should download all symbols)
    files = [f for f in os.listdir(tmpdir) if f.endswith(".csv")]
    assert len(files) > 0
