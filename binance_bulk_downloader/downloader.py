"""
Binance Bulk Downloader
"""
# import standard libraries
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from xml.etree import ElementTree
from zipfile import BadZipfile

# import third-party libraries
import requests
from rich import print
from rich.progress import track

# import my libraries
from binance_bulk_downloader.exceptions import (
    BinanceBulkDownloaderDownloadError, BinanceBulkDownloaderParamsError)


class BinanceBulkDownloader:
    _CHUNK_SIZE = 100
    _BINANCE_DATA_S3_BUCKET_URL = (
        "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
    )
    _BINANCE_DATA_DOWNLOAD_BASE_URL = "https://data.binance.vision"
    _FUTURES_ASSET = ("um", "cm")
    _OPTIONS_ASSET = ("option",)
    _ASSET = ("spot",)
    _DATA_TYPE_BY_ASSET = {
        "um": {
            "daily": (
                "aggTrades",
                "bookDepth",
                "bookTicker",
                "indexPriceKlines",
                "klines",
                "liquidationSnapshot",
                "markPriceKlines",
                "metrics",
                "premiumIndexKlines",
                "trades",
            ),
            "monthly": (
                "aggTrades",
                "bookTicker",
                "fundingRate",
                "indexPriceKlines",
                "klines",
                "markPriceKlines",
                "premiumIndexKlines",
                "trades",
            ),
        },
        "cm": {
            "daily": (
                "aggTrades",
                "bookDepth",
                "bookTicker",
                "indexPriceKlines",
                "klines",
                "liquidationSnapshot",
                "markPriceKlines",
                "metrics",
                "premiumIndexKlines",
                "trades",
            ),
            "monthly": (
                "aggTrades",
                "bookTicker",
                "fundingRate",
                "indexPriceKlines",
                "klines",
                "markPriceKlines",
                "premiumIndexKlines",
                "trades",
            ),
        },
        "spot": {
            "daily": ("aggTrades", "klines", "trades"),
            "monthly": ("aggTrades", "klines", "trades"),
        },
        "option": {"daily": ("BVOLIndex", "EOHSummary")},
    }
    _DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE = (
        "klines",
        "markPriceKlines",
        "indexPriceKlines",
        "premiumIndexKlines",
    )
    _DATA_FREQUENCY = (
        "1s",
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
        "3d",
        "1w",
        "1mo",
    )

    def __init__(
        self,
        destination_dir=".",
        data_type="klines",
        data_frequency="1m",
        asset="um",
        timeperiod_per_file="daily",
    ) -> None:
        """
        :param destination_dir: Destination directory for downloaded files
        :param data_type: Type of data to download (klines, aggTrades, etc.)
        :param data_frequency: Frequency of data to download (1m, 1h, 1d, etc.)
        :param asset: Type of asset to download (um, cm, spot, option)
        :param timeperiod_per_file: Time period per file (daily, monthly)
        """
        self._destination_dir = destination_dir
        self._data_type = data_type
        self._data_frequency = data_frequency
        self._asset = asset
        self._timeperiod_per_file = timeperiod_per_file
        self.marker = None
        self.is_truncated = True
        self.downloaded_list = []

    def _check_params(self) -> None:
        """
        Check params
        :return: None
        """
        if (
            self._data_type
            not in self._DATA_TYPE_BY_ASSET[self._asset][self._timeperiod_per_file]
        ):
            raise BinanceBulkDownloaderParamsError(
                f"data_type must be {self._DATA_TYPE_BY_ASSET[self._asset][self._timeperiod_per_file]}."
            )

        if self._data_frequency not in self._DATA_FREQUENCY:
            raise BinanceBulkDownloaderParamsError(
                f"data_frequency must be {self._DATA_FREQUENCY}."
            )

        if self._asset not in self._ASSET + self._FUTURES_ASSET + self._OPTIONS_ASSET:
            raise BinanceBulkDownloaderParamsError(
                f"asset must be {self._ASSET + self._FUTURES_ASSET + self._OPTIONS_ASSET}."
            )

        if self._timeperiod_per_file not in ["daily", "monthly"]:
            raise BinanceBulkDownloaderParamsError(
                f"timeperiod_per_file must be daily or monthly."
            )

        if not self._data_type in self._DATA_TYPE_BY_ASSET.get(self._asset, None).get(
            self._timeperiod_per_file, None
        ):
            raise BinanceBulkDownloaderParamsError(
                f"data_type must be {self._DATA_TYPE_BY_ASSET[self._asset][self._timeperiod_per_file]}."
            )

        if self._data_frequency == "1s":
            if self._asset == "spot":
                pass
            else:
                raise BinanceBulkDownloaderParamsError(
                    f"data_frequency 1s is not supported for {self._asset}."
                )

    def _get_file_list_from_s3_bucket(self, prefix, marker=None, is_truncated=False):
        """
        Get file list from s3 bucket
        :param prefix: s3 bucket prefix
        :param marker: marker
        :param is_truncated: is truncated
        :return: list of files
        """
        print(f"[bold blue]Get file list[/bold blue]: " + prefix)
        params = {"prefix": prefix, "max-keys": 1000}
        if marker:
            params["marker"] = marker

        response = requests.get(self._BINANCE_DATA_S3_BUCKET_URL, params=params)
        tree = ElementTree.fromstring(response.content)

        files = []
        for content in tree.findall(
            "{http://s3.amazonaws.com/doc/2006-03-01/}Contents"
        ):
            key = content.find("{http://s3.amazonaws.com/doc/2006-03-01/}Key").text
            if key.endswith(".zip"):
                files.append(key)
                self.marker = key

        is_truncated_element = tree.find(
            "{http://s3.amazonaws.com/doc/2006-03-01/}IsTruncated"
        )
        self.is_truncated = is_truncated_element.text == "true"

        return files

    def _make_asset_type(self) -> str:
        """
        Convert asset to asset type
        :return:
        """
        if self._asset in "um":
            asset_type = "futures/um"
        elif self._asset in "cm":
            asset_type = "futures/cm"
        elif self._asset in self._OPTIONS_ASSET:
            asset_type = "option"
        elif self._asset in self._ASSET:
            asset_type = "spot"
        else:
            raise BinanceBulkDownloaderParamsError(
                "asset must be futures, options or spot."
            )
        return asset_type

    def _set_timeperiod_per_file(self, timeperiod_per_file) -> None:
        """
        Set timeperiod_per_file
        :param timeperiod_per_file: Time period per file (daily, monthly)
        :return:
        """
        self._timeperiod_per_file = timeperiod_per_file

    def _build_prefix(self) -> str:
        """
        Build prefix to download
        :return: s3 bucket prefix
        """
        url_parts = [
            "data",
            self._make_asset_type(),
            self._timeperiod_per_file,
            self._data_type,
        ]
        prefix = "/".join(url_parts)
        return prefix

    def _download(self, prefix) -> None:
        """
        Execute download
        :param prefix: s3 bucket prefix
        :return: None
        """
        self._check_params()
        zip_destination_path = os.path.join(self._destination_dir, prefix)
        csv_destination_path = os.path.join(
            self._destination_dir, prefix.replace(".zip", ".csv")
        )

        # Make directory if not exists
        if not os.path.exists(os.path.dirname(zip_destination_path)):
            os.makedirs(os.path.dirname(zip_destination_path))

        # Don't download if already exists
        if os.path.exists(csv_destination_path):
            print(f"[yellow]Already exists: {csv_destination_path}[/yellow]")
            return

        url = f"{self._BINANCE_DATA_DOWNLOAD_BASE_URL}/{prefix}"
        print(f"[bold blue]Downloading {url}[/bold blue]")
        try:
            response = requests.get(url, zip_destination_path)
            print(f"[green]Downloaded: {url}[/green]")
        except requests.exceptions.HTTPError:
            print(f"[red]HTTP Error: {url}[/red]")
            return None

        with open(zip_destination_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        try:
            unzipped_path = "/".join(zip_destination_path.split("/")[:-1])
            with zipfile.ZipFile(zip_destination_path) as existing_zip:
                existing_zip.extractall(
                    csv_destination_path.replace(csv_destination_path, unzipped_path)
                )
                print(f"[green]Unzipped: {zip_destination_path}[/green]")
        except BadZipfile:
            print(f"[red]Bad Zip File: {zip_destination_path}[/red]")
            os.remove(zip_destination_path)
            print(f"[green]Removed: {zip_destination_path}[/green]")
            raise BinanceBulkDownloaderDownloadError

        # Delete zip file
        os.remove(zip_destination_path)
        print(f"[green]Removed: {zip_destination_path}[/green]")

    @staticmethod
    def make_chunks(lst, n) -> list:
        """
        Make chunks
        :param lst: Raw list
        :param n: size of chunk
        :return: list of chunks
        """
        return [lst[i : i + n] for i in range(0, len(lst), n)]

    def run_download(self):
        """
        Download concurrently
        :return: None
        """
        print(f"[bold blue]Downloading {self._data_type}[/bold blue]")

        while self.is_truncated:
            file_list_generator = self._get_file_list_from_s3_bucket(
                self._build_prefix(), self.marker, self.is_truncated
            )
            if self._data_type in self._DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE:
                file_list_generator = [
                    prefix
                    for prefix in file_list_generator
                    if prefix.count(self._data_frequency) == 2
                ]
            for prefix_chunk in track(
                self.make_chunks(file_list_generator, self._CHUNK_SIZE),
                description="Downloading",
            ):
                with ThreadPoolExecutor() as executor:
                    executor.map(self._download, prefix_chunk)
                self.downloaded_list.extend(prefix_chunk)
