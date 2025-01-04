"""
Test spot market symbols filtering
"""

import pytest

from binance_bulk_downloader.downloader import BinanceBulkDownloader


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
    tmpdir,
    data_type,
    data_frequency,
    timeperiod_per_file,
    symbols,
    should_pass,
):
    """
    Test spot market symbols filtering
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

    # Get file list and verify symbol filtering
    prefix = downloader._build_prefix()
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
        # Get file list without any filtering for comparison
        unfiltered_downloader = BinanceBulkDownloader(
            destination_dir=tmpdir,
            data_type=data_type,
            asset="spot",
            timeperiod_per_file=timeperiod_per_file,
        )
        if data_frequency:
            unfiltered_downloader._data_frequency = data_frequency
        unfiltered_file_list = unfiltered_downloader._get_file_list_from_s3_bucket(
            prefix
        )
        assert len(file_list) == len(
            unfiltered_file_list
        ), "File list with empty symbols should match unfiltered file list"
        assert (
            set(file_list) == set(unfiltered_file_list)
        ), "File list with empty symbols should contain the same files as unfiltered list"
        return
    else:
        symbol_list = symbols

    # Verify that all files in the list contain one of the specified symbols
    for file in file_list:
        assert any(
            symbol in file for symbol in symbol_list
        ), f"File {file} should contain one of the symbols {symbol_list}"
