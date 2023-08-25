# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from binance_bulk_downloader.downloader import BinanceBulkDownloader


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
    :param asset: asset (um, cm)
    :param data_type: data type (bookTicker)
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
                prefix + "/BTCUSDT/BTCUSDT-bookTicker-2023-06-01.zip"
            )
        elif asset == "cm":
            single_download_prefix = (
                prefix + "/BTCUSD_PERP/BTCUSD_PERP-bookTicker-2023-06-01.zip"
            )
        else:
            raise ValueError(f"asset {asset} is not supported.")
        destination_path = tmpdir.join(single_download_prefix.replace(".zip", ".csv"))
        downloader._download(single_download_prefix)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(destination_path)
    elif timeperiod_per_file == "monthly":
        # BTCUSDT_PERP monthly data is very large, so we use BNBUSD_PERP instead.
        if asset == "um":
            single_download_prefix = prefix + "/BNBUSDT/BNBUSDT-bookTicker-2023-05.zip"
        elif asset == "cm":
            single_download_prefix = (
                prefix + "/BNBUSD_PERP/BNBUSD_PERP-bookTicker-2023-05.zip"
            )
        else:
            raise ValueError(f"asset {asset} is not supported.")
        destination_path = tmpdir.join(single_download_prefix.replace(".zip", ".csv"))
        downloader._download(single_download_prefix)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(destination_path)
