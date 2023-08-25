# import standard libraries
import os

# import third party libraries
import pytest

# import my libraries
from binance_bulk_downloader.downloader import BinanceBulkDownloader


def dynamic_fundingrate_test_params():
    """
    Generate params for fundingRate tests
    :return:
    """
    for asset in BinanceBulkDownloader._FUTURES_ASSET:
        for data_type in ["fundingRate"]:
            for timeperiod_per_file in ["monthly"]:
                yield pytest.param(
                    asset,
                    data_type,
                    timeperiod_per_file,
                    id=f"{asset}-{data_type}-{timeperiod_per_file}",
                )


@pytest.mark.parametrize(
    "asset, data_type, timeperiod_per_file",
    dynamic_fundingrate_test_params(),
)
def test_fundingrate(tmpdir, asset, data_type, timeperiod_per_file):
    """
    Test fundingRate
    :param tmpdir:
    :param asset: asset (um, cm)
    :param data_type: data type (fundingRate)
    :param timeperiod_per_file: time period per file (monthly)
    :return:
    """
    downloader = BinanceBulkDownloader(
        destination_dir=tmpdir,
        asset=asset,
        data_type=data_type,
        timeperiod_per_file=timeperiod_per_file,
    )
    prefix = downloader._build_prefix()
    if timeperiod_per_file == "monthly":
        if asset == "um":
            single_download_prefix = prefix + "/BNBUSDT/BNBUSDT-fundingRate-2023-05.zip"
        elif asset == "cm":
            single_download_prefix = (
                prefix + "/BTCUSD_PERP/BTCUSD_PERP-fundingRate-2023-05.zip"
            )
        else:
            raise ValueError(f"asset {asset} is not supported.")
        destination_path = tmpdir.join(single_download_prefix.replace(".zip", ".csv"))
        downloader._download(single_download_prefix)
        # If exists csv file on destination dir, test is passed.
        assert os.path.exists(destination_path)
