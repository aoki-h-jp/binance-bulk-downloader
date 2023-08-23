# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from downloader.downloader import *
from downloader.exceptions import BinanceBulkDownloaderParamsError


def dynamic_fundingrate_test_params():
    """
    Generate params for fundingRate tests
    :return:
    """
    for asset in BinanceBulkDownloader._FUTURES_ASSET:
        for data_type in ["fundingRate"]:
            for timeperiod_per_file in ["daily", "monthly"]:
                yield pytest.param(
                    asset,
                    data_type,
                    timeperiod_per_file,
                    id=f"{asset}-{data_type}-{timeperiod_per_file}",
                )


@pytest.mark.parametrize(
    "asset, data_type, timeperiod_per_file", dynamic_fundingrate_test_params()
)
def test_fundingrate(tmpdir, asset, data_type, timeperiod_per_file):
    """
    Test fundingRate
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
        # fundingRate daily data is unavailable.
        with pytest.raises(BinanceBulkDownloaderParamsError):
            downloader._download(symbol="BTCUSDT", historical_date="2023-01-01")

    elif timeperiod_per_file == "monthly":
        # If exists csv file on destination dir, test is passed.
        downloader._download(symbol="BTCUSDT", historical_date="2023-01")
        assert os.path.exists(
            downloader._build_destination_path(
                symbol="BTCUSDT",
                historical_date="2023-01",
                extension=".csv",
                exclude_filename=False,
            )
        )
