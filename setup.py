from setuptools import setup

setup(
    name="binance-bulk-downloader",
    version="1.0.5",
    description="A Python library to efficiently and concurrently download historical data files from Binance. Supports all asset types (spot, futures, options) and all frequencies.",
    install_requires=["requests", "rich", "pytest"],
    author="aoki-h-jp",
    author_email="aoki.hirotaka.biz@gmail.com",
    license="MIT",
    packages=["binance_bulk_downloader"],
)
