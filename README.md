# binance-bulk-downloader
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110//)
[![Format code](https://github.com/aoki-h-jp/binance-bulk-downloader/actions/workflows/Formatter.yml/badge.svg?branch=main)](https://github.com/aoki-h-jp/binance-bulk-downloader/actions/workflows/Formatter.yml)
[![pytest](https://github.com/aoki-h-jp/binance-bulk-downloader/actions/workflows/pytest.yaml/badge.svg)](https://github.com/aoki-h-jp/binance-bulk-downloader/actions/workflows/pytest.yaml)
[![Github All Releases](https://img.shields.io/github/downloads/aoki-h-jp/binance-liquidation-feeder/total.svg)]()

## Python library for bulk downloading Binance historical data
A Python library to efficiently and concurrently download historical data files from Binance. Supports all asset types (spot, USDT-M, COIN-M, options) and all data frequencies.

## Installation

```bash
pip install git+https://github.com/aoki-h-jp/binance-bulk-downloader
```

## Usage
### Download all klines 1m data (USDT-M futures)

```python
from binance_bulk_downloader.downloader import BinanceBulkDownloader

downloader = BinanceBulkDownloader()
downloader.run_download()
```

### Download all klines 1h data (Spot)

```python
from binance_bulk_downloader.downloader import BinanceBulkDownloader

downloader = BinanceBulkDownloader(data_frequency='1h', asset='spot')
downloader.run_download()
```

### Download all aggTrades data (USDT-M futures)

```python
from binance_bulk_downloader.downloader import BinanceBulkDownloader

downloader = BinanceBulkDownloader(data_type='aggTrades')
downloader.run_download()
```

### Other examples
Please see /example directory.

```bash
python -m example.download_klines
```

## pytest

```bash
python -m pytest
```

## Available data types
✅: Implemented and tested. ❌: Not available on Binance.

### by data_type

| data_type           | spot | um   | cm   | options | 
| :------------------ | :--: | :--: | :--: | :-----: | 
| aggTrades           | ✅   | ✅   | ✅ | ❌      | 
| bookDepth           | ❌   | ✅   | ✅ | ❌      | 
| bookTicker          | ❌   | ✅   | ✅ | ❌      | 
| fundingRate         | ❌   | ✅   | ✅ | ❌      | 
| indexPriceKlines    | ❌   | ✅   | ✅ | ❌      | 
| klines              | ✅   | ✅   | ✅ | ❌      | 
| liquidationSnapshot | ❌   | ✅   | ✅ | ❌      | 
| markPriceKlines     | ❌   | ✅   | ✅ | ❌      | 
| metrics             | ❌   | ✅   | ✅ | ❌      | 
| premiumIndexKlines  | ❌   | ✅   | ✅ | ❌      | 
| trades              | ✅   | ✅   | ✅ | ❌      | 
| BVOLIndex           | ❌   | ❌   | ❌ | ✅      | 
| EOHSummary          | ❌   | ❌   | ❌ | ✅      | 

### by data_frequency (klines, indexPriceKlines, markPriceKlines, premiumIndexKlines)

| data_frequency | spot | um   | cm   | options |
| :------------- | :--: | :--: | :--: | :-----: |
| 1s             | ✅   | ❌   | ❌ | ❌      |
| 1m             | ✅   | ✅   | ✅ | ❌      |
| 3m             | ✅   | ✅   | ✅ | ❌      |
| 5m             | ✅   | ✅   | ✅ | ❌      |
| 15m            | ✅   | ✅   | ✅ | ❌      |
| 30m            | ✅   | ✅   | ✅ | ❌      |
| 1h             | ✅   | ✅   | ✅ | ❌      |
| 2h             | ✅   | ✅   | ✅ | ❌      |
| 4h             | ✅   | ✅   | ✅ | ❌      |
| 6h             | ✅   | ✅   | ✅ | ❌      |
| 8h             | ✅   | ✅   | ✅ | ❌      |
| 12h            | ✅   | ✅   | ✅ | ❌      |
| 1d             | ✅   | ✅   | ✅ | ❌      |
| 3d             | ✅   | ✅   | ✅ | ❌      |
| 1w             | ✅   | ✅   | ✅ | ❌      |
| 1mo            | ✅   | ✅   | ✅ | ❌      |

## If you want to report a bug or request a feature
Please create an issue on this repository!

## Disclaimer
This project is for educational purposes only. You should not construe any such information or other material as legal,
tax, investment, financial, or other advice. Nothing contained here constitutes a solicitation, recommendation,
endorsement, or offer by me or any third party service provider to buy or sell any securities or other financial
instruments in this or in any other jurisdiction in which such solicitation or offer would be unlawful under the
securities laws of such jurisdiction.

Under no circumstances will I be held responsible or liable in any way for any claims, damages, losses, expenses, costs,
or liabilities whatsoever, including, without limitation, any direct or indirect damages for loss of profits.
