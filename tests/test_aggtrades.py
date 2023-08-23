# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from downloader.downloader import *


def dynamic_aggtrades_test_params():
    """
    Generate params for aggtrades tests
    :return:
    """
    assets = list(BinanceBulkDownloader._FUTURES_ASSET + BinanceBulkDownloader._ASSET)
    for asset in assets:
        for data_type in ["aggTrades"]:
            for timeperiod_per_file in ["daily", "monthly"]:
                yield pytest.param(
                    asset,
                    data_type,
                    timeperiod_per_file,
                    id=f"{asset}-{data_type}-{timeperiod_per_file}",
                )


@pytest.mark.parametrize(
    "asset, data_type, timeperiod_per_file",
    dynamic_aggtrades_test_params(),
)
def test_aggtrades(tmpdir, asset, data_type, timeperiod_per_file):
    """
    Test aggtrades
    :param tmpdir:
    :param asset: asset (spot, um, cm)
    :param data_type: data type (aggTrades)
    :param timeperiod_per_file: time period per file (daily, monthly)
    :return:
    """
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        asset=asset,
        data_type=data_type,
        timeperiod_per_file=timeperiod_per_file,
    )
    if timeperiod_per_file == "daily":
        downloader._download(symbol="BTCUSDT", historical_date="2022-01-01")
        print(downloader._build_destination_path(
                symbol="BTCUSDT",
                historical_date="2022-01-01",
                extension=".csv",
                exclude_filename=False,
            ))
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(
            downloader._build_destination_path(
                symbol="BTCUSDT",
                historical_date="2022-01-01",
                extension=".csv",
                exclude_filename=False,
            )
        )
    elif timeperiod_per_file == "monthly":
        print(downloader._build_destination_path(
            symbol="BTCBUSD",
            historical_date="2021-01",
            extension=".csv",
            exclude_filename=False,
        ))
        # BTCUSDT aggTrades monthly data is very heavy so use BTCBUSD for testing instead.
        downloader._download(symbol="BTCBUSD", historical_date="2021-01")
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(
            downloader._build_destination_path(
                symbol="BTCBUSD",
                historical_date="2021-01",
                extension=".csv",
                exclude_filename=False,
            )
        )

