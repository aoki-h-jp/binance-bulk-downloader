# import standard libraries
import datetime
import json
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from zipfile import BadZipfile

# import third-party libraries
import pandas as pd
import requests
from rich import print
from rich.progress import track
from tqdm import tqdm


class BinanceBulkDownloader:
    _CHUNK_SIZE = 10
    _BINANCE_API_BASE_URL = "https://data-api.binance.vision"
    _BINANCE_DATA_BASE_URL = "https://data.binance.vision/data"
    # TODO: To be corrected in the future due to errors because of symbol discrepancies.
    # _FUTURES_ASSET = ("um", "cm")
    _FUTURES_ASSET = ("um",)
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
        # TODO: To be corrected in the future due to errors because of symbol discrepancies.
        # "cm": {
        #     "daily": (
        #         "aggTrades",
        #         "bookDepth",
        #         "bookTicker",
        #         "indexPriceKlines",
        #         "klines",
        #         "liquidationSnapshot",
        #         "markPriceKlines",
        #         "metrics",
        #         "premiumIndexKlines",
        #         "trades",
        #     ),
        #     "monthly": (
        #         "aggTrades",
        #         "bookTicker",
        #         "fundingRate",
        #         "indexPriceKlines",
        #         "klines",
        #         "markPriceKlines",
        #         "premiumIndexKlines",
        #         "trades",
        #     ),
        # },
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
        # TODO: To be corrected in the future due to errors because of date discrepancies.
        # "3d",
        # "1w",
        # "1mo",
    )
    _INITIAL_DATE = datetime.datetime(2020, 1, 1)
    TODAY = datetime.datetime.utcnow().today().strftime("%Y-%m-%d")
    ALL_DATE = (
        pd.date_range(_INITIAL_DATE, TODAY, freq="1d")
        .to_series()
        .dt.strftime("%Y-%m-%d")
        .tolist()
    )
    ALL_MONTH = (
        pd.date_range(_INITIAL_DATE, TODAY, freq="1M")
        .to_series()
        .dt.strftime("%Y-%m-%d")
        .tolist()
    )

    def __init__(
        self,
        destination_dir=".",
        data_type="klines",
        data_frequency="1m",
        asset="um",
        currency="USDT",
        timeperiod_per_file="daily",
    ) -> None:
        """
        :param destination_dir: Destination directory for downloaded files
        :param data_type: Type of data to download (klines, aggTrades, etc.)
        :param data_frequency: Frequency of data to download (1m, 1h, 1d, etc.)
        :param asset: Type of asset to download (um, cm, spot, option)
        :param currency: Type of currency to download (USDT, BTC, etc.)
        :param timeperiod_per_file: Time period per file (daily, monthly)
        """
        self._destination_dir = destination_dir
        self._data_type = data_type
        self._data_frequency = data_frequency
        self._asset = asset
        self._currency = currency
        self._timeperiod_per_file = timeperiod_per_file

    def _get_list_of_symbols(self) -> list:
        """
        Get list of symbols from Binance API (ExchangeInfo)
        :return: list of symbols
        """
        if self._asset == "spot":
            asset = "SPOT"
        else:
            asset = "MARGIN"

        url = "/".join(
            [self._BINANCE_API_BASE_URL, f"api/v3/exchangeInfo?permissions={asset}"]
        )
        response = requests.get(url).text
        return [
            s
            for s in map(
                lambda symbol: symbol["symbol"], json.loads(response)["symbols"]
            )
            if s.endswith(self._currency)
        ]

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
            asset_type = "options"
        elif self._asset in self._ASSET:
            asset_type = "spot"
        else:
            raise ValueError("asset must be futures, options or spot.")
        return asset_type

    def _set_timeperiod_per_file(self, timeperiod_per_file) -> None:
        """
        Set timeperiod_per_file
        :param timeperiod_per_file: Time period per file (daily, monthly)
        :return:
        """
        self._timeperiod_per_file = timeperiod_per_file

    def _build_url(self, symbol, historical_date) -> str:
        """
        Build URL to download
        :param symbol: symbol (BTCUSDT, etc.)
        :param historical_date: historical date (2020-01-01, etc.)
        :return: URL
        """
        if self._data_type in self._DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE:
            filename = f"{symbol}-{self._data_frequency}-{historical_date}.zip"
            url_parts = [
                self._make_asset_type(),
                self._timeperiod_per_file,
                self._data_type,
                symbol,
                self._data_frequency,
                filename,
            ]
        else:
            filename = f"{symbol}-{self._data_type}-{historical_date}.zip"
            url_parts = [
                self._make_asset_type(),
                self._timeperiod_per_file,
                self._data_type,
                symbol,
                filename,
            ]
        url_parts.insert(0, self._BINANCE_DATA_BASE_URL)
        url = "/".join(url_parts)
        return url

    def _build_destination_path(
        self, symbol, historical_date, extension=".zip", exclude_filename=False
    ) -> str:
        """
        Build destination path to save
        :param symbol: symbol (BTCUSDT, etc.)
        :param historical_date: historical date (2020-01-01, etc.)
        :param extension: extension (.zip, .csv)
        :param exclude_filename: exclude filename from path
        :return: destination path
        """
        if self._data_type in self._DATA_FREQUENCY_REQUIRED_BY_DATA_TYPE:
            filename = f"{symbol}-{self._data_frequency}-{historical_date}{extension}"
            destination_path_parts = [
                self._destination_dir,
                self._make_asset_type(),
                self._asset,
                self._timeperiod_per_file,
                self._data_type,
                symbol,
                self._data_frequency,
                filename,
            ]
        else:
            filename = f"{symbol}-{self._data_type}-{historical_date}{extension}"
            destination_path_parts = [
                self._destination_dir,
                self._make_asset_type(),
                self._asset,
                self._timeperiod_per_file,
                self._data_type,
                symbol,
                filename,
            ]
        if exclude_filename:
            destination_path_parts = destination_path_parts[:-1]
        destination_path = os.path.join(*destination_path_parts)
        return destination_path

    def _download(self, symbol, historical_date) -> None:
        """
        Execute download
        :param symbol: symbol (BTCUSDT, etc.)
        :param historical_date: historical date (2020-01-01, etc.)
        :return: None
        """
        url = self._build_url(symbol, historical_date)
        print(f"[bold blue]Downloading {url}[/bold blue]: " + url)
        zip_destination_path = self._build_destination_path(symbol, historical_date)
        csv_destination_path = self._build_destination_path(
            symbol, historical_date, extension=".csv"
        )
        # ディレクトリが存在しない場合は作る
        if not os.path.exists(os.path.dirname(zip_destination_path)):
            os.makedirs(os.path.dirname(zip_destination_path))

        # ファイルが既に存在する場合はダウンロードしない
        if os.path.exists(csv_destination_path):
            print(f"[yellow]Already exists: {csv_destination_path}[/yellow]")
            return

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
            with zipfile.ZipFile(zip_destination_path) as existing_zip:
                existing_zip.extractall(
                    csv_destination_path.replace(
                        csv_destination_path,
                        self._build_destination_path(
                            symbol, historical_date, extension="", exclude_filename=True
                        ),
                    )
                )
                print(f"[green]Unzipped: {zip_destination_path}[/green]")
        except BadZipfile:
            print(f"[red]Bad Zip File: {zip_destination_path}[/red]")
            os.remove(zip_destination_path)
            print(f"[green]Removed: {zip_destination_path}[/green]")
            return

        # 解凍したらZIPファイルは削除する
        os.remove(zip_destination_path)
        print(f"[green]Removed: {zip_destination_path}[/green]")

    def _download_concurrently(self, symbol) -> None:
        """
        Execute download concurrently
        :param symbol: symbol (BTCUSDT, etc.)
        :return: None
        """
        if self._timeperiod_per_file == "daily":
            historical_dates = self.ALL_DATE
        elif self._timeperiod_per_file == "monthly":
            historical_dates = self.ALL_MONTH
        else:
            raise ValueError("timeperiod_per_file must be daily or monthly.")

        for date_chunk in track(
            self.make_chunks(historical_dates, self._CHUNK_SIZE),
            description="Downloading",
        ):
            with ThreadPoolExecutor() as executor:
                executor.map(self._download, [symbol] * len(date_chunk), date_chunk)

    @staticmethod
    def make_chunks(lst, n):
        """
        Make chunks
        :param lst: Raw list
        :param n: size of chunk
        :return: list of chunks
        """
        return [lst[i : i + n] for i in range(0, len(lst), n)]

    def run_download(self) -> None:
        """
        Running download by symbol
        :return: None
        """
        symbols = self._get_list_of_symbols()
        for sym in tqdm(symbols, desc="Symbols"):
            print(f"[bold blue]Start download {sym}[/bold blue]")
            self._download_concurrently(sym)
            print(f"[green]Downloaded: {sym}[/green]")

        print(
            f"[bold blue]Finish download {self._data_type} {self._data_frequency}[/bold blue]"
        )
