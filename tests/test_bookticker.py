# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from downloader.downloader import *
from downloader.exceptions import BinanceBulkDownloaderParamsError


def dynamic_bookticker_test_params():
    """
    Generate params for bookTicker tests
    :return:
    """
    for asset in BinanceBulkDownloader._FUTURES_ASSET:
        for data_type in ["bookTicker"]:
            for timeperiod_per_file in ["daily", "monthly"]:
                yield pytest.param(
                    asset,
                    data_type,
                    timeperiod_per_file,
                    id=f"{asset}-{data_type}-{timeperiod_per_file}",
                )


@pytest.mark.parametrize(
    "asset, data_type, timeperiod_per_file",
    dynamic_bookticker_test_params(),
)
def test_bookticker(tmpdir, asset, data_type, timeperiod_per_file):
    """
    Test bookTicker
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
        downloader._download(symbol="BNBBUSD", historical_date="2023-05-16")
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(
            downloader._build_destination_path(
                symbol="BNBBUSD",
                historical_date="2023-05-16",
                extension=".csv",
                exclude_filename=False,
            )
        )
    elif timeperiod_per_file == "monthly":
        downloader._download(symbol="BNBBUSD", historical_date="2023-05")
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(
            downloader._build_destination_path(
                symbol="BNBBUSD",
                historical_date="2023-05",
                extension=".csv",
                exclude_filename=False,
            )
        )
