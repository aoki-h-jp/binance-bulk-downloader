# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from downloader.downloader import *


def dynamic_indexpriceklines_test_params():
    """
    Generate params for indexpriceklines tests
    :return:
    """
    for asset in BinanceBulkDownloader._FUTURES_ASSET:
        for data_type in ["indexPriceKlines"]:
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
    dynamic_indexpriceklines_test_params(),
)
def test_indexpriceklines(
    tmpdir, asset, data_type, data_frequency, timeperiod_per_file
):
    """
    Test indexpriceKlines
    :param tmpdir:
    :param asset: asset (spot, um, cm)
    :param data_type: data type (indexPriceKlines)
    :param data_frequency: data frequency (1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo)
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
        if data_frequency in [
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
        ]:
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01-01.zip"
                )
            elif asset == "um":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01-01.zip"
                )
            elif asset == "cm":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSD/{data_frequency}/BTCUSD-{data_frequency}-2021-01-01.zip"
                )
            else:
                raise ValueError(f"asset {asset} is not supported.")
        elif data_frequency == "3d":
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2023-06-14.zip"
                )
                # Not test.
                return None
            elif asset == "um":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2023-06-14.zip"
                )
            elif asset == "cm":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSD/{data_frequency}/BTCUSD-{data_frequency}-2022-10-21.zip"
                )
            else:
                raise ValueError(f"asset {asset} is not supported.")
        elif data_frequency == "1w":
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2023-06-12.zip"
                )
                # Not test.
                return None
            elif asset == "um":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2023-06-12.zip"
                )
            elif asset == "cm":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSD/{data_frequency}/BTCUSD-{data_frequency}-2022-10-17.zip"
                )
            else:
                raise ValueError(f"asset {asset} is not supported.")
        elif data_frequency == "1mo":
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2023-05-01.zip"
                )
                # Not test.
                return None
            elif asset == "um":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2022-09-01.zip"
                )
            elif asset == "cm":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSD/{data_frequency}/BTCUSD-{data_frequency}-2022-09-01.zip"
                )
            else:
                raise ValueError(f"asset {asset} is not supported.")
        elif data_frequency == "1s":
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01-01.zip"
                )
            else:
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01-01.zip"
                )
                # Not test.
                return None
        else:
            raise ValueError(f"data_frequency {data_frequency} is not supported.")
        destination_path = tmpdir.join(single_download_prefix.replace(".zip", ".csv"))
        downloader._download(single_download_prefix)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(destination_path)
    elif timeperiod_per_file == "monthly":
        if data_frequency in [
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
        ]:
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
            elif asset == "um":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
            elif asset == "cm":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSD/{data_frequency}/BTCUSD-{data_frequency}-2021-01.zip"
                )
            else:
                raise ValueError(f"asset {asset} is not supported.")
        elif data_frequency == "3d":
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
                # Not test.
                return None
            elif asset == "um":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
            elif asset == "cm":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSD/{data_frequency}/BTCUSD-{data_frequency}-2021-01.zip"
                )
            else:
                raise ValueError(f"asset {asset} is not supported.")
        elif data_frequency == "1w":
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
                # Not test.
                return None
            elif asset == "um":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
            elif asset == "cm":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSD/{data_frequency}/BTCUSD-{data_frequency}-2021-01.zip"
                )
            else:
                raise ValueError(f"asset {asset} is not supported.")
        elif data_frequency == "1mo":
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
                # Not test.
                return None
            elif asset == "um":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
            elif asset == "cm":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSD/{data_frequency}/BTCUSD-{data_frequency}-2021-01.zip"
                )
            else:
                raise ValueError(f"asset {asset} is not supported.")
        elif data_frequency == "1s":
            if asset == "spot":
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
            else:
                single_download_prefix = (
                    prefix
                    + f"/BTCUSDT/{data_frequency}/BTCUSDT-{data_frequency}-2021-01.zip"
                )
                # Not test.
                return None
        else:
            raise ValueError(f"data_frequency {data_frequency} is not supported.")
        destination_path = tmpdir.join(single_download_prefix.replace(".zip", ".csv"))
        downloader._download(single_download_prefix)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(destination_path)
