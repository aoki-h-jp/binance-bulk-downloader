"""
BinanceBulkDownloader exceptions
"""


class BinanceBulkDownloaderParamsError(Exception):
    """
    BinanceBulkDownloader params error
    This exception is raised when BinanceBulkDownloader params are invalid.
    """

    pass


class BinanceBulkDownloaderDownloadError(Exception):
    """
    BinanceBulkDownloader download error
    This exception is raised when BinanceBulkDownloader download is failed.
    """

    pass
