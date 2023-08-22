# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from downloader.downloader import *


def dynamic_klines_test_params():
    """
    Generate params for klines tests
    :return:
    """
    assets = list(BinanceBulkDownloader._FUTURES_ASSET + BinanceBulkDownloader._ASSET)
    for asset in assets:
        for data_type in BinanceBulkDownloader._DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE:
            for data_frequency in BinanceBulkDownloader._DATA_FREQUENCY:
                for timeperiod_per_file in ["daily", "monthly"]:
                    yield pytest.param(
                        asset,
                        data_type,
                        data_frequency,
                        timeperiod_per_file,
                        id=f"{asset}-{data_type}-{data_frequency}-{timeperiod_per_file}",
                    )


@pytest.mark.parametrize(
    "asset, data_type, data_frequency, timeperiod_per_file",
    dynamic_klines_test_params(),
)
def test_klines(tmpdir, asset, data_type, data_frequency, timeperiod_per_file):
    """
    Test klines
    :param tmpdir:
    :param asset: asset (BTCUSDT, etc.)
    :param data_type: data type (aggTrades, klines, etc.)
    :param data_frequency: data frequency (1m, 1h, etc.)
    :param timeperiod_per_file: time period per file (daily, monthly)
    :return:
    """
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        data_type=data_type,
        data_frequency=data_frequency,
        asset=asset,
    )
    downloader._download(symbol="BTCUSDT", historical_date="2022-01-01")

    # If exists csv file on destination dir, test is passed.
    assert not os.path.exists(
        downloader._build_destination_path(
            symbol="BTCUSDT",
            historical_date="2020-01-01",
            extension=".csv",
            exclude_filename=False,
        )
    )
