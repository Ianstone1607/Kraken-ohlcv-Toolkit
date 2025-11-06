# Trading Tools - Crypto OHLCV Acquisition & Technical Indicators

A comprehensive Python toolkit for acquiring cryptocurrency OHLCV (Open, High, Low, Close, Volume) data and calculating technical indicators for algorithmic trading from the Kraken Broker.

## 🎯 Overview

This project provides a robust data acquisition pipeline for cryptocurrency trading analysis with:

- **Multi-source data fetching** (Database → ZIP archives → Kraken API)
- **Intelligent rate limiting** to prevent API blocks
- **Flexible technical indicator calculation** using pandas_ta
- **TimescaleDB integration** for efficient time-series storage
- **Automatic gap filling** and missing data handling

## ✨ Key Features

### 📊 OHLCV Data Acquisition
- Fetch current and historical cryptocurrency OHLCV data from multiple sources
- Automatic fallback chain: Database → ZIP files → Kraken API
- Smart timestamp alignment to timeframe intervals
- Missing data detection and reporting
- Rate-limited API calls with SQLite-based tracking

### 📈 Technical Indicators
- Wrapper for **pandas_ta** library (130+ indicators)
- Simple dict-based indicator configuration
- Automatic lookback period calculation
- Extensible base class for custom indicators
- Standardized output formatting

### 🗄️ Database Management
- TimescaleDB/PostgreSQL integration
- Efficient batch uploads with conflict handling
- Hypertable support for time-series optimization
- Automatic table creation and indexing

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/trading-tools.git
cd trading-tools

# Install dependencies
pip install -r requirements.txt
```

### Basic Usage

#### Fetch OHLCV Data

```python
from Tools.Kraken_ohlcv_acquisitions import fetch_ohlcv

# Fetch Bitcoin hourly data
found_df, missing_df = fetch_ohlcv(
    symbol="BTCUSD",
    timeframe="1h",
    start_ts=1722470400,  # Unix timestamp
    end_ts=1722481200
)

print(f"Found {len(found_df)} candles")
print(f"Missing {len(missing_df)} timestamps")
```

#### Calculate Technical Indicators

```python
from Tools.Pandas_ta_Wrapper import PandasTAIndicator

# Create RSI indicator
rsi_indicator = PandasTAIndicator.from_spec({
    "name": "rsi",
    "params": {"length": 14}
})

# Calculate from OHLCV DataFrame
result_df = rsi_indicator.calculate_from_df(ohlcv_df)
print(result_df[['timestamp_unix', 'RSI_14']].tail())
```

## 📦 Module Overview

### `Kraken_ohlcv_acquisitions.py`
Main data acquisition orchestrator that:
- Queries multiple data sources intelligently
- Handles timestamp alignment and validation
- Provides both found and missing data tracking
- Integrates with database and API rate limiting

**Key Functions:**
- `fetch_ohlcv()` - Main entry point for data fetching
- `fetch_ohlcv_from_db()` - Query TimescaleDB/PostgreSQL
- `fetch_ohlcv_from_zip()` - Read from local ZIP archives
- `fetch_ohlcv_from_api()` - Fetch from Kraken API with rate limiting

### `Pandas_ta_Wrapper.py` (Indicators)
Technical indicator framework with:
- Abstract `BaseIndicator` class for custom indicators
- `PandasTAIndicator` wrapper for pandas_ta library
- Automatic lookback calculation
- Flexible input/output handling

**Key Classes:**
- `BaseIndicator` - Abstract base class
- `PandasTAIndicator` - pandas_ta wrapper implementation

### `Tools/Infra/API_enforcer.py`
Rate limiting system to prevent API blocks:
- SQLite-based call tracking
- Configurable time windows and request limits
- Automatic pause when limits approached
- Thread-safe singleton pattern

**Key Class:**
- `KrakenAPIEnforcer` - Rate limiter for Kraken API

### `Tools/Infra/database_uploader.py`
Database utilities for TimescaleDB/PostgreSQL:
- Batch OHLCV uploads with conflict resolution
- Automatic table and hypertable creation
- Connection pooling and error handling
- Efficient bulk inserts with psycopg2

## 🔧 Configuration

### Database Setup

Ensure you have PostgreSQL/TimescaleDB running and create a database:

```sql
CREATE DATABASE trading_data;
\c trading_data
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

Update your database connection settings in your code or environment variables.

### API Configuration

The Kraken API rate limiter uses these defaults:
- **Window**: 10 seconds
- **Max requests**: 20 per window
- **Storage**: SQLite database in `Api mangment data/` folder

Adjust in `API_enforcer.py` if needed.

## 📋 Requirements

- Python 3.8+
- pandas
- pandas_ta
- ccxt (Kraken API wrapper)
- psycopg2-binary (PostgreSQL adapter)
- pytz (timezone handling)

See `requirements.txt` for complete list.

## 🏗️ Project Structure

```
trading-tools/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── .gitignore                        # Git ignore rules
├── Tools/
│   ├── __init__.py                   # Package initialization
│   ├── Kraken_ohlcv_acquisitions.py  # Main data acquisition
│   ├── Pandas_ta_Wrapper.py          # Technical indicators
│   └── Infra/
│       ├── __init__.py               # Infrastructure package init
│       ├── API_enforcer.py           # API rate limiting
│       └── database_uploader.py      # Database operations
```

## 🎓 Advanced Usage

### Custom Indicators

Extend the `BaseIndicator` class to create custom indicators:

```python
from Tools.Pandas_ta_Wrapper import BaseIndicator
import pandas as pd

class CustomIndicator(BaseIndicator):
    def lookback(self) -> int:
        """Return number of candles needed for calculation"""
        return self.params.get("length", 14) + 5
    
    def calculate_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Implement your indicator calculation logic"""
        length = self.params.get("length", 14)
        # Your calculation here
        result_df = df.copy()
        result_df['custom_indicator'] = ...  # Your logic
        return result_df
```

### Multi-Source Data Strategy

The acquisition module uses this intelligent fallback strategy:

1. **Check database first** (fastest)
2. For recent data (<200 periods): **API → ZIP**
3. For older data (>200 periods): **ZIP → API**

This optimizes for:
- Speed (database cache)
- API rate limits (prefer ZIP for bulk historical data)
- Freshness (API for recent data)

## 🤝 Contributing

Contributions are welcome! This is a personal project but feel free to:
- Report bugs via Issues
- Suggest enhancements
- Fork and submit Pull Requests

## 📄 License

This project is provided as-is for educational and personal use.

## 🙏 Acknowledgments

- **pandas_ta** - Comprehensive technical analysis library
- **ccxt** - Cryptocurrency exchange API wrapper
- **TimescaleDB** - Time-series database extension for PostgreSQL

## 📞 Contact

For questions or discussions about this project, feel free to open an Issue on GitHub.

---

**Note**: This is a personal trading toolkit. Use at your own risk. Always test thoroughly before using in production trading systems.
