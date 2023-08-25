# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from binance_bulk_downloader.downloader import BinanceBulkDownloader


def dynamic_bvolindex_test_params():
    """
    Generate params for BVOLIndex tests
    :return:
    """
    for asset in BinanceBulkDownloader._OPTIONS_ASSET:
        for data_type in ["BVOLIndex"]:
            for timeperiod_per_file in ["daily"]:
                yield pytest.param(
                    asset,
                    data_type,
                    timeperiod_per_file,
                    id=f"{asset}-{data_type}-{timeperiod_per_file}",
                )


@pytest.mark.parametrize(
    "asset, data_type, timeperiod_per_file", dynamic_bvolindex_test_params()
)
def test_bvolindex(tmpdir, asset, data_type, timeperiod_per_file):
    """
    Test BVOLIndex
    :param tmpdir:
    :param asset: asset (option)
    :param data_type: data type (BVOLIndex)
    :param timeperiod_per_file: time period per file (daily)
    :return:
    """
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        asset=asset,
        data_type=data_type,
        timeperiod_per_file=timeperiod_per_file,
    )
    prefix = downloader._build_prefix()
    single_download_prefix = (
        prefix + "/BTCBVOLUSDT/BTCBVOLUSDT-BVOLIndex-2023-07-01.zip"
    )
    destination_path = tmpdir.join(single_download_prefix.replace(".zip", ".csv"))
    downloader._download(single_download_prefix)
    # If exists csv file on destination dir, test is passed.
    assert os.path.exists(destination_path)
