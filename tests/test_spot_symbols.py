"""
Test spot market symbols filtering
"""

import pytest
from unittest.mock import patch

from binance_bulk_downloader.downloader import BinanceBulkDownloader


@pytest.fixture
def mock_s3_response():
    """Mock S3 response"""
    return {
        "BTCUSDT": [
            "data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-01.zip",
            "data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-01-02.zip",
        ],
        "ETHUSDT": [
            "data/spot/daily/klines/ETHUSDT/1h/ETHUSDT-1h-2024-01-01.zip",
            "data/spot/daily/klines/ETHUSDT/1h/ETHUSDT-1h-2024-01-02.zip",
        ],
    }


def dynamic_spot_symbols_test_params():
    """
    Generate params for spot symbols tests
    :return: test parameters
    """
    test_cases = [
        # Single symbol klines
        ("klines", "1h", "daily", "BTCUSDT", True),
        # Multiple symbols klines
        ("klines", "1h", "daily", ["BTCUSDT", "ETHUSDT"], True),
        # Multiple symbols trades
        ("trades", None, "daily", ["BTCUSDT", "ETHUSDT"], True),
        # Multiple symbols aggTrades
        ("aggTrades", None, "daily", ["BTCUSDT", "ETHUSDT"], True),
        # Invalid symbol
        ("klines", "1h", "daily", "INVALID_SYMBOL", False),
        # Empty symbols list (no filtering)
        ("klines", "1h", "daily", [], True),
    ]

    for (
        data_type,
        data_frequency,
        timeperiod_per_file,
        symbols,
        should_pass,
    ) in test_cases:
        yield pytest.param(
            data_type,
            data_frequency,
            timeperiod_per_file,
            symbols,
            should_pass,
            id=f"{data_type}-{symbols}-{should_pass}",
        )


@pytest.mark.parametrize(
    "data_type, data_frequency, timeperiod_per_file, symbols, should_pass",
    dynamic_spot_symbols_test_params(),
)
def test_spot_symbols(
    mock_s3_response,
    tmpdir,
    data_type,
    data_frequency,
    timeperiod_per_file,
    symbols,
    should_pass,
):
    """
    Test spot market symbols filtering
    :param mock_s3_response: mock S3 response
    :param tmpdir: temporary directory
    :param data_type: type of data to download
    :param data_frequency: frequency of data
    :param timeperiod_per_file: time period per file
    :param symbols: symbol or list of symbols
    :param should_pass: whether the test should pass validation
    """
    params = {
        "destination_dir": tmpdir,
        "data_type": data_type,
        "asset": "spot",
        "timeperiod_per_file": timeperiod_per_file,
        "symbols": symbols,
    }
    if data_frequency:
        params["data_frequency"] = data_frequency

    downloader = BinanceBulkDownloader(**params)
    downloader._check_params()

    # Build prefix
    prefix = downloader._build_prefix()
    assert isinstance(prefix, str), "Prefix should be a string"
    assert prefix.startswith("data/spot"), "Prefix should start with data/spot"

    # Mock file list
    def mock_get_file_list(self, prefix):
        if isinstance(symbols, str):
            return mock_s3_response.get(symbols, [])
        elif not symbols:
            # Empty symbols list means no filtering
            all_files = []
            for files in mock_s3_response.values():
                all_files.extend(files)
            return all_files
        else:
            # Multiple symbols means combine files for specified symbols
            files = []
            for symbol in symbols:
                files.extend(mock_s3_response.get(symbol, []))
            return files

    # Mock _get_file_list_from_s3_bucket
    with patch.object(
        BinanceBulkDownloader, "_get_file_list_from_s3_bucket", mock_get_file_list
    ):
        file_list = downloader._get_file_list_from_s3_bucket(prefix)

        if not should_pass:
            assert (
                len(file_list) == 0
            ), f"File list should be empty for invalid symbol {symbols}"
            return

        if isinstance(symbols, str):
            symbol_list = [symbols]
        elif not symbols:
            # Empty symbols list means no filtering
            assert len(file_list) > 0, "File list should not be empty for no filtering"
            return
        else:
            symbol_list = symbols

        # Check if each file in the file list contains one of the specified symbols
        for file in file_list:
            assert any(
                symbol in file for symbol in symbol_list
            ), f"File {file} should contain one of the symbols {symbol_list}"
