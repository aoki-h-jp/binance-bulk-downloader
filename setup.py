from setuptools import find_packages, setup

setup(
    name="binance-bulk-downloader",
    version="1.0.2",
    description="A Python library to efficiently and concurrently download historical data files from Binance. Supports all asset types (spot, futures, options) and all frequencies.",
    install_requires=["requests", "rich", "pytest"],
    author="aoki-h-jp",
    author_email="aoki.hirotaka.biz@gmail.com",
    license="MIT",
    packages=find_packages(
        include=["binance_bulk_downloader"], exclude=["tests", "example"]
    ),
)
