from setuptools import find_packages, setup

setup(
    name="binance-bulk-downloader",
    version="1.0.0",
    description="A Python library to efficiently and concurrently download historical data files from Binance. Supports multiple asset types (spot, futures, options) and various data frequencies.",
    install_requires=[],
    author="aoki-h-jp",
    author_email="aoki.hirotaka.biz@gmail.com",
    license="MIT",
    packages=find_packages(include=["downloader"], exclude=["tests"]),
)
