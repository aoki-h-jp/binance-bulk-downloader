# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from binance_bulk_downloader.downloader import BinanceBulkDownloader


def dynamic_bookdepth_test_params():
    """
    Generate params for bookDepth tests
    :return:
    """
    for asset in BinanceBulkDownloader._FUTURES_ASSET:
        for data_type in ["bookDepth"]:
            for timeperiod_per_file in ["daily"]:
                yield pytest.param(
                    asset,
                    data_type,
                    timeperiod_per_file,
                    id=f"{asset}-{data_type}-{timeperiod_per_file}",
                )


@pytest.mark.parametrize(
    "asset, data_type, timeperiod_per_file",
    dynamic_bookdepth_test_params(),
)
def test_bookdepth(tmpdir, asset, data_type, timeperiod_per_file):
    """
    Test bookDepth
    :param tmpdir:
    :param asset: asset (um, cm)
    :param data_type: data type (bookDepth)
    :param timeperiod_per_file: time period per file (daily, monthly)
    :return:
    """
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        asset=asset,
        data_type=data_type,
        timeperiod_per_file=timeperiod_per_file,
    )
    prefix = downloader._build_prefix()
    if timeperiod_per_file == "daily":
        if asset == "um":
            single_download_prefix = (
                prefix + "/BTCUSDT/BTCUSDT-bookDepth-2023-01-01.zip"
            )
        elif asset == "cm":
            single_download_prefix = (
                prefix + "/BTCUSD_PERP/BTCUSD_PERP-bookDepth-2023-01-01.zip"
            )
        else:
            raise ValueError(f"asset {asset} is not supported.")
        destination_path = tmpdir.join(single_download_prefix.replace(".zip", ".csv"))
        downloader._download(single_download_prefix)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(destination_path)
