"""
Test error cases for BinanceBulkDownloader
"""

import os
import pytest
import requests
from unittest.mock import patch, MagicMock
from zipfile import BadZipfile

from binance_bulk_downloader.downloader import BinanceBulkDownloader
from binance_bulk_downloader.exceptions import (
    BinanceBulkDownloaderDownloadError,
    BinanceBulkDownloaderParamsError,
)


class TestBinanceBulkDownloaderErrors:
    @pytest.fixture
    def downloader(self):
        return BinanceBulkDownloader()

    def test_invalid_data_type(self, downloader):
        """Test case for invalid data type"""
        downloader._data_type = "invalid_type"
        with pytest.raises(BinanceBulkDownloaderParamsError) as exc_info:
            downloader._check_params()
        assert "data_type must be" in str(exc_info.value)

    def test_invalid_asset(self, downloader):
        """Test case for invalid asset type"""
        downloader._asset = "invalid_asset"
        with pytest.raises(BinanceBulkDownloaderParamsError) as exc_info:
            downloader._check_params()
        assert "asset must be" in str(exc_info.value)

    def test_invalid_timeperiod(self, downloader):
        """Test case for invalid time period"""
        downloader._timeperiod_per_file = "invalid_period"
        with pytest.raises(BinanceBulkDownloaderParamsError) as exc_info:
            downloader._check_params()
        assert "timeperiod_per_file must be daily or monthly" in str(exc_info.value)

    def test_invalid_data_frequency(self, downloader):
        """Test case for invalid data frequency"""
        downloader._data_frequency = "invalid_frequency"
        with pytest.raises(BinanceBulkDownloaderParamsError) as exc_info:
            downloader._check_params()
        assert "data_frequency must be" in str(exc_info.value)

    def test_1s_frequency_non_spot(self, downloader):
        """Test case for using 1s frequency with non-spot asset"""
        downloader._data_frequency = "1s"
        downloader._asset = "um"
        with pytest.raises(BinanceBulkDownloaderParamsError) as exc_info:
            downloader._check_params()
        assert "data_frequency 1s is not supported" in str(exc_info.value)

    @patch("requests.get")
    @patch("os.path.exists")
    @patch("os.makedirs")
    def test_network_error(self, mock_makedirs, mock_exists, mock_get, downloader):
        """Test case for HTTP network error"""
        mock_exists.return_value = False
        mock_makedirs.return_value = None
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError()
        mock_get.return_value = mock_response
        with pytest.raises(BinanceBulkDownloaderDownloadError):
            downloader._download("test/prefix/file.zip")

    @patch("requests.get")
    @patch("os.path.exists")
    @patch("os.makedirs")
    def test_connection_timeout(self, mock_makedirs, mock_exists, mock_get, downloader):
        """Test case for connection timeout"""
        mock_exists.return_value = False
        mock_makedirs.return_value = None
        mock_get.side_effect = requests.exceptions.Timeout()
        with pytest.raises(BinanceBulkDownloaderDownloadError):
            downloader._download("test/prefix/file.zip")

    @patch("requests.get")
    @patch("os.path.exists")
    @patch("os.makedirs")
    def test_connection_error(self, mock_makedirs, mock_exists, mock_get, downloader):
        """Test case for connection error"""
        mock_exists.return_value = False
        mock_makedirs.return_value = None
        mock_get.side_effect = requests.exceptions.ConnectionError()
        with pytest.raises(BinanceBulkDownloaderDownloadError):
            downloader._download("test/prefix/file.zip")

    @patch("requests.get")
    @patch("os.path.exists")
    @patch("os.makedirs")
    @patch("zipfile.ZipFile")
    def test_bad_zip_file(
        self, mock_zipfile, mock_makedirs, mock_exists, mock_get, downloader
    ):
        """Test case for corrupted ZIP file"""
        mock_exists.return_value = False
        mock_makedirs.return_value = None
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"dummy content"]
        mock_get.return_value = mock_response
        mock_zipfile.side_effect = BadZipfile()
        with pytest.raises(BinanceBulkDownloaderDownloadError):
            downloader._download("test/prefix/file.zip")

    @patch("requests.get")
    @patch("os.path.exists")
    @patch("os.makedirs")
    def test_permission_error(self, mock_makedirs, mock_exists, mock_get, downloader):
        """Test case for directory permission error"""
        mock_exists.return_value = False
        mock_makedirs.side_effect = PermissionError()
        with pytest.raises(BinanceBulkDownloaderDownloadError):
            downloader._download("test/prefix/file.zip")

    @patch("requests.get")
    @patch("os.path.exists")
    @patch("os.makedirs")
    def test_disk_space_error(self, mock_makedirs, mock_exists, mock_get, downloader):
        """Test case for insufficient disk space"""
        mock_exists.return_value = False
        mock_makedirs.return_value = None
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [b"dummy content"]
        mock_get.return_value = mock_response

        m = patch("builtins.open", side_effect=OSError(28, "No space left on device"))
        with m:
            with pytest.raises(BinanceBulkDownloaderDownloadError):
                downloader._download("test/prefix/file.zip")
