# binance-bulk-downloader
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110//)

## Python library for bulk downloading Binance historical data
A Python library to efficiently and concurrently download historical data files from Binance.

Supports multiple asset types (spot, futures, options) and various data frequencies.

Status on available data types is shown below.

## Installation

```bash
pip install git+https://github.com/aoki-h-jp/binance-bulk-downloader
```

## Usage
### Download all klines 1m data (USDT-M futures)
```python
from downloader import BinanceBulkDownloader
downloader = BinanceBulkDownloader()
downloader.run_download()
```

### Download all klines 1h data (Spot)
```python
from downloader import BinanceBulkDownloader
downloader = BinanceBulkDownloader(data_frequency='1h', asset='spot')
downloader.run_download()
```

### Download all aggTrades data (USDT-M futures)
```python
from downloader import BinanceBulkDownloader
downloader = BinanceBulkDownloader(data_type='aggTrades')
downloader.run_download()
```

## Status
This library is under development. Not all unit tests have been completed yet and the behaviour is unstable.

✅: Implemented and tested. 🚧: Implemented but not tested. ❎: Not available on Binance.

### by data_type

| data_type           | spot | um   | cm   | options | 
| :------------------ | :--: | :--: | :--: | :-----: | 
| aggTrades           | ✅   | ✅   | 🚧 | ❎      | 
| bookDepth           | ❎   | 🚧 | 🚧 | ❎      | 
| bookTicker          | ❎   | 🚧 | 🚧 | ❎      | 
| fundingRate         | ❎   | 🚧 | 🚧 | ❎      | 
| indexPriceKlines    | ❎   | ✅   | 🚧 | ❎      | 
| klines              | ✅   | ✅   | 🚧 | ❎      | 
| liquidationSnapshot | ❎   | 🚧 | 🚧 | ❎      | 
| markPriceKlines     | ❎   | ✅   | 🚧 | ❎      | 
| metrics             | ❎   | 🚧 | 🚧 | ❎      | 
| premiumIndexKlines  | ❎   | ✅   | 🚧 | ❎      | 
| trades              | 🚧 | 🚧 | 🚧 | ❎      | 
| BVOLIndex           | ❎   | ❎   | ❎   | 🚧    | 
| EOHSummary          | ❎   | ❎   | ❎   | 🚧    | 

### by data_frequency (klines, indexPriceKlines, markPriceKlines, premiumIndexKlines)

| data_frequency | spot | um   | cm   | options |
| :------------- | :--: | :--: | :--: | :-----: |
| 1m             | ✅   | ✅   | 🚧 | ❎      |
| 3m             | ✅   | ✅   | 🚧 | ❎      |
| 5m             | ✅   | ✅   | 🚧 | ❎      |
| 15m            | ✅   | ✅   | 🚧 | ❎      |
| 30m            | ✅   | ✅   | 🚧 | ❎      |
| 1h             | ✅   | ✅   | 🚧 | ❎      |
| 2h             | ✅   | ✅   | 🚧 | ❎      |
| 4h             | ✅   | ✅   | 🚧 | ❎      |
| 6h             | ✅   | ✅   | 🚧 | ❎      |
| 8h             | ✅   | ✅   | 🚧 | ❎      |
| 12h            | ✅   | ✅   | 🚧 | ❎      |
| 1d             | ✅   | ✅   | 🚧 | ❎      |
| 3d             | 🚧   | 🚧   | 🚧 | ❎      |
| 1w             | 🚧   | 🚧   | 🚧 | ❎      |
| 1mo             | 🚧   | 🚧   | 🚧 | ❎      |


## Disclaimer
This project is for educational purposes only. You should not construe any such information or other material as legal,
tax, investment, financial, or other advice. Nothing contained here constitutes a solicitation, recommendation,
endorsement, or offer by me or any third party service provider to buy or sell any securities or other financial
instruments in this or in any other jurisdiction in which such solicitation or offer would be unlawful under the
securities laws of such jurisdiction.

Under no circumstances will I be held responsible or liable in any way for any claims, damages, losses, expenses, costs,
or liabilities whatsoever, including, without limitation, any direct or indirect damages for loss of profits.