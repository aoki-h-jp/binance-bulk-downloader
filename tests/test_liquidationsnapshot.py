# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from downloader.downloader import *
from downloader.exceptions import BinanceBulkDownloaderParamsError


def dynamic_liquidationsnapshot_test_params():
    """
    Generate params for liquidationSnapshot tests
    :return:
    """
    for asset in BinanceBulkDownloader._FUTURES_ASSET:
        for data_type in ["liquidationSnapshot"]:
            for timeperiod_per_file in ["daily", "monthly"]:
                yield pytest.param(
                    asset,
                    data_type,
                    timeperiod_per_file,
                    id=f"{asset}-{data_type}-{timeperiod_per_file}",
                )


@pytest.mark.parametrize(
    "asset, data_type, timeperiod_per_file",
    dynamic_liquidationsnapshot_test_params()
)
def test_liquidationsnapshot(tmpdir, asset, data_type, timeperiod_per_file):
    """
    Test liquidationSnapshot
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
        # If exists csv file on destination dir, test is passed.
        downloader._download(symbol="BTCUSDT", historical_date="2023-07-01")
        assert os.path.exists(
            downloader._build_destination_path(
                symbol="BTCUSDT",
                historical_date="2023-07-01",
                extension=".csv",
                exclude_filename=False,
            )
        )

    elif timeperiod_per_file == "monthly":
        # liquidationSnapshot monthly data is unavailable.
        with pytest.raises(BinanceBulkDownloaderParamsError):
            downloader._download(symbol="BTCUSDT", historical_date="2023-07")
