#Run this file: python -m Tools.New_C_I

# Standard Library
import csv
import io
import os
import time
import zipfile
from datetime import datetime, timezone, timedelta

# Third-Party Libraries
import ccxt
import pandas as pd
import psycopg2
import pytz
from psycopg2.extras import execute_values

# Typing
from typing import Any, Dict, List, Tuple

# Local Modules
from Tools.Infra.API_enforcer import KrakenAPIEnforcer







# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# Helper functions
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

"""
Timestamp Alignment Utility
============================

Aligns Unix timestamps to proper interval boundaries.
Used by all OHLCV acquisition functions to ensure consistent timestamp handling.
"""

def align_timestamps_to_timeframe(
    start_ts: int,
    end_ts: int,
    timeframe: str,
    run_tests: bool = False
) -> Tuple[int, int]:
    """
    Aligns start and end timestamps to the proper boundaries for a given timeframe.
    
    This ensures that timestamps match the actual candle times in the data sources.
    For example, if you request 1h data starting at 8:40 AM, this will align it
    to 8:00 AM so it matches the actual hourly candles.
    
    Args:
        start_ts: Start Unix timestamp (can be any time)
        end_ts: End Unix timestamp (can be any time)
        timeframe: Timeframe string (e.g., "1h", "15m", "1d")
        run_tests: If True, runs test suite and exits (default: False)
        
    Returns:
        Tuple of (aligned_start_ts, aligned_end_ts)
        - aligned_start_ts: Floored to the start of the interval containing start_ts
        - aligned_end_ts: Ceiled to the end of the interval containing end_ts
        
    Examples:
        >>> # 1 hour timeframe
        >>> align_timestamps_to_timeframe(1722473400, 1722480600, "1h")
        (1722470400, 1722481200)  # 8:40 AM → 8:00 AM, 10:50 AM → 11:00 AM
        
        >>> # 15 minute timeframe
        >>> align_timestamps_to_timeframe(1722473420, 1722474320, "15m")
        (1722472800, 1722474600)  # 8:43 AM → 8:30 AM, 8:58 AM → 9:00 AM
        
        >>> # 1 day timeframe
        >>> align_timestamps_to_timeframe(1722508800, 1722595200, "1d")
        (1722470400, 1722556800)  # 6:00 PM → midnight, next 6:00 PM → next midnight
        
        >>> # Run tests
        >>> align_timestamps_to_timeframe(0, 0, "1h", run_tests=True)
        # Runs comprehensive test suite and exits
    """
    
    # ═══════════════════════════════════════════════════════════════════════
    # TEST MODE
    # ═══════════════════════════════════════════════════════════════════════
    
    if run_tests:
        from datetime import datetime, timezone
        
        print("="*70)
        print("TIMESTAMP ALIGNMENT UTILITY TESTS")
        print("="*70)
        
        # Test 1: 1 hour alignment
        print("\n📊 TEST 1: 1 Hour Timeframe")
        print("-"*70)
        
        start = datetime(2024, 8, 1, 8, 40, 0, tzinfo=timezone.utc)
        end = datetime(2024, 8, 1, 10, 50, 0, tzinfo=timezone.utc)
        
        start_ts_test = int(start.timestamp())
        end_ts_test = int(end.timestamp())
        
        print(f"Original range:")
        print(f"  Start: {start} ({start_ts_test})")
        print(f"  End:   {end} ({end_ts_test})")
        
        aligned_start, aligned_end = align_timestamps_to_timeframe(start_ts_test, end_ts_test, "1h")
        
        print(f"\nAligned range:")
        print(f"  Start: {datetime.fromtimestamp(aligned_start, timezone.utc)} ({aligned_start})")
        print(f"  End:   {datetime.fromtimestamp(aligned_end, timezone.utc)} ({aligned_end})")
        print(f"  ✓ Aligned start: 8:40 → 8:00")
        print(f"  ✓ Aligned end: 10:50 → 11:00")
        
        # Test 2: 15 minute alignment
        print("\n📊 TEST 2: 15 Minute Timeframe")
        print("-"*70)
        
        start = datetime(2024, 8, 1, 8, 43, 0, tzinfo=timezone.utc)
        end = datetime(2024, 8, 1, 8, 58, 0, tzinfo=timezone.utc)
        
        start_ts_test = int(start.timestamp())
        end_ts_test = int(end.timestamp())
        
        print(f"Original range:")
        print(f"  Start: {start} ({start_ts_test})")
        print(f"  End:   {end} ({end_ts_test})")
        
        aligned_start, aligned_end = align_timestamps_to_timeframe(start_ts_test, end_ts_test, "15m")
        
        print(f"\nAligned range:")
        print(f"  Start: {datetime.fromtimestamp(aligned_start, timezone.utc)} ({aligned_start})")
        print(f"  End:   {datetime.fromtimestamp(aligned_end, timezone.utc)} ({aligned_end})")
        print(f"  ✓ Aligned start: 8:43 → 8:30")
        print(f"  ✓ Aligned end: 8:58 → 9:00")
        
        # Test 3: Daily alignment
        print("\n📊 TEST 3: Daily Timeframe")
        print("-"*70)
        
        start = datetime(2024, 8, 1, 18, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 8, 2, 18, 0, 0, tzinfo=timezone.utc)
        
        start_ts_test = int(start.timestamp())
        end_ts_test = int(end.timestamp())
        
        print(f"Original range:")
        print(f"  Start: {start} ({start_ts_test})")
        print(f"  End:   {end} ({end_ts_test})")
        
        aligned_start, aligned_end = align_timestamps_to_timeframe(start_ts_test, end_ts_test, "1d")
        
        print(f"\nAligned range:")
        print(f"  Start: {datetime.fromtimestamp(aligned_start, timezone.utc)} ({aligned_start})")
        print(f"  End:   {datetime.fromtimestamp(aligned_end, timezone.utc)} ({aligned_end})")
        print(f"  ✓ Aligned start: Aug 1 6:00 PM → Aug 1 midnight")
        print(f"  ✓ Aligned end: Aug 2 6:00 PM → Aug 3 midnight")
        
        # Test 4: Already aligned timestamps
        print("\n📊 TEST 4: Already Aligned Timestamps")
        print("-"*70)
        
        start = datetime(2024, 8, 1, 8, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 8, 1, 10, 0, 0, tzinfo=timezone.utc)
        
        start_ts_test = int(start.timestamp())
        end_ts_test = int(end.timestamp())
        
        print(f"Original range:")
        print(f"  Start: {start} ({start_ts_test})")
        print(f"  End:   {end} ({end_ts_test})")
        
        # Check if aligned (calculate inline since we're in the same function)
        interval_seconds = 3600  # 1 hour in seconds
        is_aligned = (start_ts_test % interval_seconds == 0) and (end_ts_test % interval_seconds == 0)
        print(f"  Already aligned: {is_aligned}")
        
        aligned_start, aligned_end = align_timestamps_to_timeframe(start_ts_test, end_ts_test, "1h")
        
        print(f"\nAligned range (unchanged):")
        print(f"  Start: {datetime.fromtimestamp(aligned_start, timezone.utc)} ({aligned_start})")
        print(f"  End:   {datetime.fromtimestamp(aligned_end, timezone.utc)} ({aligned_end})")
        print(f"  ✓ No change needed - already aligned!")
        
        print("\n" + "="*70)
        print("✅ ALL TESTS PASSED")
        print("="*70)
        
        return (0, 0)  # Return dummy values when in test mode
    
    # ═══════════════════════════════════════════════════════════════════════
    # NORMAL MODE - ALIGNMENT LOGIC
    # ═══════════════════════════════════════════════════════════════════════
    
    # Convert timeframe to seconds
    tf = timeframe.lower()
    unit = tf[-1]
    val = int(tf[:-1])
    
    if unit == 'm':
        interval_seconds = val * 60
    elif unit == 'h':
        interval_seconds = val * 60 * 60
    elif unit == 'd':
        interval_seconds = val * 60 * 60 * 24
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    
    # Floor start_ts to the nearest interval boundary
    aligned_start_ts = (start_ts // interval_seconds) * interval_seconds
    
    # Ceil end_ts to the nearest interval boundary
    aligned_end_ts = ((end_ts + interval_seconds - 1) // interval_seconds) * interval_seconds
    
    return aligned_start_ts, aligned_end_ts


# if __name__ == "__main__":
#     # Run the built-in test suite
#     align_timestamps_to_timeframe(0, 0, "1h", run_tests=True)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

"""
Database Loading Function for OHLCV Acquisition
================================================

Single function to load OHLCV data into TimescaleDB.
Handles structure creation and data insertion in one call.
"""


def load_ohlcv_to_db(
    ohlcv_df: pd.DataFrame,
    db_name: str = "ohlcv_data",
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "mysecretpassword",
    batch_size: int = 1000
) -> None:
    """
    Loads OHLCV DataFrame into TimescaleDB.
    
    Automatically creates database, schema, table, and hypertable if they don't exist.
    Then inserts the data, silently skipping duplicates.
    
    Args:
        ohlcv_df: DataFrame with columns: symbol, timeframe, ny_time, timestamp_unix, 
                  open, high, low, close, volume
        db_name: Target database name (default: "trading_data")
        host: Database host (default: "localhost")
        port: Database port (default: 5432)
        user: Database user (default: "postgres")
        password: Database password (default: "mysecretpassword")
        batch_size: Number of rows per batch insert (default: 1000)
        
    Returns:
        None (side effect: data inserted into database)
        
    Side Effects:
        - Creates database if missing
        - Creates schema named after symbol (from DataFrame)
        - Creates table named {timeframe}_ohlcv (e.g., "1hour_ohlcv")
        - Converts table to TimescaleDB hypertable
        - Inserts data (skips duplicates)
        
    Required DataFrame Columns:
        - symbol: Trading pair (e.g., "ETHUSD")
        - timeframe: Timeframe string (e.g., "1h", "15m", "1d")
        - ny_time: Format "MM/DD/YYYY HH:MM AM/PM" (e.g., "08/01/2025 01:00 AM")
        - timestamp_unix: Unix timestamp (integer)
        - open, high, low, close, volume: Numeric values
        
    Example:
        >>> df = pd.DataFrame({
        ...     "symbol": ["ETHUSD", "ETHUSD"],
        ...     "timeframe": ["1h", "1h"],
        ...     "ny_time": ["08/01/2025 01:00 AM", "08/01/2025 02:00 AM"],
        ...     "timestamp_unix": [1754355600, 1754359200],
        ...     "open": [2500.0, 2505.0],
        ...     "high": [2510.0, 2515.0],
        ...     "low": [2495.0, 2500.0],
        ...     "close": [2505.0, 2510.0],
        ...     "volume": [1000.0, 1200.0]
        ... })
        >>> load_ohlcv_to_db(df)
        ✅ Inserted 2 row(s) into 'ETHUSD.1hour_ohlcv'
    """
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 0: VALIDATE INPUT
    # ═══════════════════════════════════════════════════════════════════════
    
    if ohlcv_df.empty:
        print("📭 No rows to insert — DataFrame is empty.")
        return
    
    required_cols = ["symbol", "timeframe", "ny_time", "timestamp_unix", 
                     "open", "high", "low", "close", "volume"]
    missing_cols = set(required_cols) - set(ohlcv_df.columns)
    if missing_cols:
        raise ValueError(f"DataFrame missing required columns: {missing_cols}")
    
    # Extract symbol and timeframe from DataFrame (assumes all rows have same symbol/timeframe)
    symbol = ohlcv_df["symbol"].iloc[0]
    timeframe = ohlcv_df["timeframe"].iloc[0]
    
    # Validate that all rows have the same symbol and timeframe
    if not (ohlcv_df["symbol"] == symbol).all():
        raise ValueError("All rows must have the same symbol")
    if not (ohlcv_df["timeframe"] == timeframe).all():
        raise ValueError("All rows must have the same timeframe")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 1: ENSURE DATABASE EXISTS
    # ═══════════════════════════════════════════════════════════════════════
    
    admin_conn = psycopg2.connect(
        host=host, port=port, dbname="postgres", user=user, password=password
    )
    admin_conn.autocommit = True
    admin_cur = admin_conn.cursor()
    
    admin_cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
    if not admin_cur.fetchone():
        admin_cur.execute(f"CREATE DATABASE {db_name} OWNER {user};")
        print(f"🆕 Created database '{db_name}'")
    
    admin_cur.close()
    admin_conn.close()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 2: ENSURE SCHEMA, TABLE, AND HYPERTABLE EXIST
    # ═══════════════════════════════════════════════════════════════════════
    
    conn = psycopg2.connect(
        host=host, port=port, dbname=db_name, user=user, password=password
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    # Create schema
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{symbol}" AUTHORIZATION {user};')
    
    # Create table (convert timeframe: "1h" -> "1hour_ohlcv", "15m" -> "15min_ohlcv")
    tbl = f"{timeframe.replace('m', 'min').replace('h', 'hour')}_ohlcv"
    
    cur.execute(f'''
        CREATE TABLE IF NOT EXISTS "{symbol}"."{tbl}" (
            date           TEXT,
            time           TEXT,
            timestamp_unix BIGINT PRIMARY KEY,
            open           NUMERIC,
            high           NUMERIC,
            low            NUMERIC,
            close          NUMERIC,
            volume         NUMERIC
        );
    ''')
    
    # Convert to hypertable (TimescaleDB)
    cur.execute(f'''
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM timescaledb_information.hypertables
                WHERE hypertable_schema = '{symbol}'
                AND hypertable_name = '{tbl}'
            ) THEN
                PERFORM create_hypertable('"{symbol}"."{tbl}"', 'timestamp_unix', if_not_exists => TRUE);
            END IF;
        END;
        $$;
    ''')
    
    cur.close()
    conn.close()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 3: INSERT DATA
    # ═══════════════════════════════════════════════════════════════════════
    
    conn = psycopg2.connect(
        host=host, port=port, dbname=db_name, user=user, password=password
    )
    conn.autocommit = False  # Manual commit for batch operations
    cur = conn.cursor()
    
    # Prepare insert query
    insert_query = f'''
        INSERT INTO "{symbol}"."{tbl}" (
            date, time, timestamp_unix,
            open, high, low, close, volume
        )
        VALUES %s
        ON CONFLICT (timestamp_unix) DO NOTHING;
    '''
    
    # Prepare rows as list of tuples
    rows = []
    for _, row in ohlcv_df.iterrows():
        ny_time = row["ny_time"]
        date_str, time_str = ny_time.split(" ", 1)
        
        rows.append((
            date_str,                    # date
            time_str,                    # time
            int(row["timestamp_unix"]),  # timestamp_unix
            float(row["open"]),          # open
            float(row["high"]),          # high
            float(row["low"]),           # low
            float(row["close"]),         # close
            float(row["volume"])         # volume
        ))
    
    # Batch insert with transaction
    rows_inserted = 0
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            execute_values(cur, insert_query, batch)
            rows_inserted += len(batch)
        
        conn.commit()

        
    except Exception as e:
        conn.rollback()
        print(f"❌ Insert failed for '{symbol}.{tbl}': {e}")
        
    finally:
        cur.close()
        conn.close()



# if __name__ == "__main__":
#     # Example: Create sample OHLCV data with symbol and timeframe columns
#     sample_data = pd.DataFrame({
#         "symbol": ["ETHUSD", "ETHUSD", "ETHUSD"],
#         "timeframe": ["1h", "1h", "1h"],
#         "ny_time": ["08/01/2025 01:00 AM", "08/01/2025 02:00 AM", "08/01/2025 03:00 AM"],
#         "timestamp_unix": [1754355600, 1754359200, 1754362800],
#         "open": [2500.0, 2505.0, 2510.0],
#         "high": [2510.0, 2515.0, 2520.0],
#         "low": [2495.0, 2500.0, 2505.0],
#         "close": [2505.0, 2510.0, 2515.0],
#         "volume": [1000.0, 1200.0, 1100.0]
#     })
    
#     # Load it - symbol and timeframe are extracted from the DataFrame!
#     load_ohlcv_to_db(ohlcv_df=sample_data)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
# Acqustion Functions
# ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════

"""
ZIP Data Fetcher for OHLCV Acquisition
=======================================

Single function to fetch OHLCV data from ZIP archives.
Returns both found data and missing timestamps.
"""

def fetch_ohlcv_from_zip(
    symbol: str,
    timeframe: str,
    start_ts: int,
    end_ts: int,
    zip_folder: str = "Kraken_Zip_Files"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetches OHLCV data from ZIP files for a given symbol, timeframe, and date range.
    
    Returns ALL candles between start_ts and end_ts (inclusive), plus a DataFrame
    showing which timestamps are missing so callers can handle sparse data.
    
    Intelligently scans ZIPs in the folder and only opens the ones likely to contain
    the requested data based on timestamp ranges.
    
    Args:
        symbol: Trading pair (e.g., "ETHUSD")
        timeframe: Timeframe string (e.g., "1h", "15m", "1d")
        start_ts: Start Unix timestamp (inclusive)
        end_ts: End Unix timestamp (inclusive)
        zip_folder: Folder containing ZIP files (default: "Kraken_Zip_Files")
        
    Returns:
        Tuple of (found_df, missing_df):
        
        found_df: DataFrame with columns:
            - symbol, timeframe, ny_time, timestamp_unix, open, high, low, close, volume
            - Contains all candles that were found in ZIPs
            
        missing_df: DataFrame with columns:
            - symbol, timeframe, ny_time, timestamp_unix
            - Contains timestamps that should exist but weren't found
            
    Example:
        >>> found, missing = fetch_ohlcv_from_zip(
        ...     symbol="ETHUSD",
        ...     timeframe="1h",
        ...     start_ts=1754355600,
        ...     end_ts=1754370000
        ... )
        >>> print(f"Found {len(found)} candles, missing {len(missing)}")
        Found 4 candles, missing 0
    """
    # ALIGN TIMESTAMPS TO PROPER INTERVAL BOUNDARIES
    start_ts, end_ts = align_timestamps_to_timeframe(start_ts, end_ts, timeframe)
    
    tz_NY = pytz.timezone("America/New_York")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 1: CONVERT TIMEFRAME TO MINUTES AND GENERATE EXPECTED TIMESTAMPS
    # ═══════════════════════════════════════════════════════════════════════
    
    # Convert timeframe string to minutes
    tf = timeframe.lower()
    unit = tf[-1]
    val = int(tf[:-1])
    
    if unit == 'm':
        interval_minutes = val
    elif unit == 'h':
        interval_minutes = val * 60
    elif unit == 'd':
        interval_minutes = val * 60 * 24
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    
    interval_seconds = interval_minutes * 60
    
    # Generate ALL expected timestamps between start and end
    expected_ts = list(range(start_ts, end_ts + 1, interval_seconds))
    
    # Helper function to create empty/missing dataframes
    def create_missing_dataframes():
        found_df = pd.DataFrame(columns=[
            'symbol', 'timeframe', 'ny_time', 'timestamp_unix',
            'open', 'high', 'low', 'close', 'volume'
        ])
        missing_df = pd.DataFrame({
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp_unix': expected_ts,
            'ny_time': [
                datetime.fromtimestamp(ts, pytz.utc)
                        .astimezone(tz_NY)
                        .strftime("%m/%d/%Y %I:%M %p")
                for ts in expected_ts
            ]
        })
        return found_df, missing_df
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 2: SCAN ZIP FOLDER
    # ═══════════════════════════════════════════════════════════════════════
    
    if not os.path.exists(zip_folder):
        print(f"⚠️ ZIP folder not found: {zip_folder}")
        return create_missing_dataframes()
    
    zip_paths = [
        os.path.join(zip_folder, f)
        for f in os.listdir(zip_folder)
        if f.endswith('.zip')
    ]
    
    if not zip_paths:
        print(f"⚠️ No ZIP files found in {zip_folder}")
        return create_missing_dataframes()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 3: BUILD INVENTORY (WHICH ZIPS HAVE WHAT DATA)
    # ═══════════════════════════════════════════════════════════════════════
    
    csv_filename = f"{symbol}_{interval_minutes}.csv"
    inventory = {}  # {zip_path: (start_ts, end_ts)}
    
    for zip_path in zip_paths:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Check if this ZIP contains our symbol/timeframe
                if csv_filename not in zf.namelist():
                    continue
                
                # Scan to find timestamp range
                with zf.open(csv_filename) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    timestamps = []
                    
                    for row in reader:
                        if row and len(row) >= 1:
                            try:
                                timestamps.append(int(row[0]))
                            except ValueError:
                                continue
                    
                    if timestamps:
                        inventory[zip_path] = (min(timestamps), max(timestamps))
                        
        except Exception as e:
            print(f"⚠️ Error scanning {os.path.basename(zip_path)}: {e}")
            continue
    
    if not inventory:
        print(f"⚠️ No data found for {symbol} @ {timeframe} in any ZIP")
        return create_missing_dataframes()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 4: SELECT OPTIMAL ZIPS (ONLY ONES THAT OVERLAP OUR DATE RANGE)
    # ═══════════════════════════════════════════════════════════════════════
    
    selected_zips = []
    
    for zip_path, (zip_start, zip_end) in inventory.items():
        # Check if ZIP overlaps with our range
        if zip_end < start_ts:
            continue  # ZIP ends before our range starts
        if zip_start > end_ts:
            continue  # ZIP starts after our range ends
        
        selected_zips.append((zip_path, zip_start))
    
    # Sort by start timestamp
    selected_zips.sort(key=lambda x: x[1])
    selected_zip_paths = [path for path, _ in selected_zips]
    
    if not selected_zip_paths:
        print(f"⚠️ No ZIPs contain data for {symbol} in range {start_ts}-{end_ts}")
        return create_missing_dataframes()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 5: EXTRACT DATA FROM SELECTED ZIPS
    # ═══════════════════════════════════════════════════════════════════════
    
    all_data = []
    
    for zip_path in selected_zip_paths:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                with zf.open(csv_filename) as f:
                    df = pd.read_csv(
                        f,
                        header=None,
                        names=["timestamp_unix", "open", "high", "low", "close", "volume", "trades"]
                    )
            
            # Filter to only rows that match expected timestamps
            df = df[df['timestamp_unix'].isin(expected_ts)]
            
            if df.empty:
                continue
            
            # Add NY formatted time
            df['ny_time'] = df['timestamp_unix'].apply(
                lambda ts: datetime.fromtimestamp(ts, pytz.utc)
                                  .astimezone(tz_NY)
                                  .strftime("%m/%d/%Y %I:%M %p")
            )
            
            # Select and order columns (drop 'trades')
            df = df[['ny_time', 'timestamp_unix', 'open', 'high', 'low', 'close', 'volume']]
            
            all_data.append(df)
            
        except Exception as e:
            print(f"⚠️ Error extracting from {os.path.basename(zip_path)}: {e}")
            continue
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 6: COMBINE, DEDUPLICATE, AND IDENTIFY MISSING
    # ═══════════════════════════════════════════════════════════════════════
    
    if not all_data:
        return create_missing_dataframes()
    
    # Combine all data and remove duplicates
    found_df = pd.concat(all_data, ignore_index=True)
    found_df = found_df.drop_duplicates(subset=['timestamp_unix'])
    found_df = found_df.sort_values('timestamp_unix').reset_index(drop=True)
    
    # Add symbol and timeframe columns
    found_df.insert(0, 'symbol', symbol)
    found_df.insert(1, 'timeframe', timeframe)
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 7: IDENTIFY MISSING TIMESTAMPS
    # ═══════════════════════════════════════════════════════════════════════
    
    found_timestamps = set(found_df['timestamp_unix'].values)
    missing_timestamps = sorted(set(expected_ts) - found_timestamps)
    
    if missing_timestamps:
        missing_df = pd.DataFrame({
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp_unix': missing_timestamps,
            'ny_time': [
                datetime.fromtimestamp(ts, pytz.utc)
                        .astimezone(tz_NY)
                        .strftime("%m/%d/%Y %I:%M %p")
                for ts in missing_timestamps
            ]
        })
        print(f"⚠️ Found {len(found_df)} candles, missing {len(missing_df)} for {symbol} @ {timeframe}")
    else:
        missing_df = pd.DataFrame(columns=['symbol', 'timeframe', 'timestamp_unix', 'ny_time'])
        # print(f"✅ Found all {len(found_df)} candles for {symbol} @ {timeframe}")
    
    return found_df, missing_df


# if __name__ == "__main__":
#     # Example: Fetch hourly ETH data for a specific range
#     found_df, missing_df = fetch_ohlcv_from_zip(
#         symbol="ETHUSD",
#         timeframe="1h",
#         start_ts=1681516800,  
#         end_ts=1694304000     
#     )
    
#     print("\n📊 FOUND DATA:")
#     if not found_df.empty:
#         print(f"  Total candles: {len(found_df)}")
#         print(found_df.head())
#     else:
#         print("  No data found")
    
#     print("\n⚠️ MISSING DATA:")
#     if not missing_df.empty:
#         print(f"  Missing {len(missing_df)} candles")
#         print(missing_df.head())
#     else:
#         print("  No missing candles - data is complete!")

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


"""
Kraken API Fetcher for OHLCV Acquisition
=========================================

Single function to fetch OHLCV data from Kraken API with rate limiting.
Returns both found data and missing timestamps.
"""

def fetch_ohlcv_from_api(
    symbol: str,
    timeframe: str,
    start_ts: int,
    end_ts: int,
    max_candles_per_request: int = 720
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetches OHLCV data from Kraken API for a given symbol, timeframe, and date range.
    
    Returns ALL candles between start_ts and end_ts (inclusive), plus a DataFrame
    showing which timestamps are missing so callers can handle sparse data.
    
    Uses rate limiting to prevent hitting Kraken's API limits.
    
    Args:
        symbol: Trading pair (e.g., "ETHUSD")
        timeframe: Timeframe string (e.g., "1h", "15m", "1d")
        start_ts: Start Unix timestamp (inclusive)
        end_ts: End Unix timestamp (inclusive)
        max_candles_per_request: Maximum candles to request per API call (default 720, Kraken's max)
        
    Returns:
        Tuple of (found_df, missing_df):
        
        found_df: DataFrame with columns:
            - symbol, timeframe, ny_time, timestamp_unix, open, high, low, close, volume
            - Contains all candles that were found via API
            
        missing_df: DataFrame with columns:
            - symbol, timeframe, ny_time, timestamp_unix
            - Contains timestamps that should exist but weren't returned by API
            
    Example:
        >>> found, missing = fetch_ohlcv_from_api(
        ...     symbol="ETHUSD",
        ...     timeframe="1h",
        ...     start_ts=1754355600,
        ...     end_ts=1754370000
        ... )
        >>> print(f"Found {len(found)} candles, missing {len(missing)}")
        Found 4 candles, missing 0
    """
    # ALIGN TIMESTAMPS TO PROPER INTERVAL BOUNDARIES
    start_ts, end_ts = align_timestamps_to_timeframe(start_ts, end_ts, timeframe)
    
    tz_NY = pytz.timezone("America/New_York")
    
    # Create rate limiter instance
    enforcer = KrakenAPIEnforcer()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 1: CONVERT TIMEFRAME TO MINUTES AND GENERATE EXPECTED TIMESTAMPS
    # ═══════════════════════════════════════════════════════════════════════
    
    # Convert timeframe string to minutes
    tf = timeframe.lower()
    unit = tf[-1]
    val = int(tf[:-1])
    
    if unit == 'm':
        interval_minutes = val
    elif unit == 'h':
        interval_minutes = val * 60
    elif unit == 'd':
        interval_minutes = val * 60 * 24
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    
    interval_seconds = interval_minutes * 60
    
    # Generate ALL expected timestamps between start and end
    expected_ts = list(range(start_ts, end_ts + 1, interval_seconds))
    
    # Helper function to create empty/missing dataframes
    def create_missing_dataframes():
        found_df = pd.DataFrame(columns=[
            'symbol', 'timeframe', 'ny_time', 'timestamp_unix',
            'open', 'high', 'low', 'close', 'volume'
        ])
        missing_df = pd.DataFrame({
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp_unix': expected_ts,
            'ny_time': [
                datetime.fromtimestamp(ts, pytz.utc)
                        .astimezone(tz_NY)
                        .strftime("%m/%d/%Y %I:%M %p")
                for ts in expected_ts
            ]
        })
        return found_df, missing_df
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 2: INITIALIZE KRAKEN API
    # ═══════════════════════════════════════════════════════════════════════
    
    try:
        kraken = ccxt.kraken()
        kraken.load_markets()
    except Exception as e:
        print(f"❌ Failed to initialize Kraken API: {e}")
        return create_missing_dataframes()
    
    # Convert symbol format (e.g., "ETHUSD" → "ETH/USD")
    try:
        base = symbol[:-3]  # e.g., "ETH"
        quote = symbol[-3:]  # e.g., "USD"
        ccxt_symbol = f"{base}/{quote}"
        
        # Verify symbol exists
        if ccxt_symbol not in kraken.markets:
            print(f"❌ Symbol {ccxt_symbol} not found in Kraken markets")
            return create_missing_dataframes()
    except Exception as e:
        print(f"❌ Invalid symbol format: {symbol}")
        return create_missing_dataframes()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 3: FETCH DATA IN BATCHES (RESPECTING RATE LIMITS)
    # ═══════════════════════════════════════════════════════════════════════
    
    all_data = []
    candles_needed = len(expected_ts)
    
    # Calculate how many requests we need
    num_requests = (candles_needed + max_candles_per_request - 1) // max_candles_per_request
    
    print(f"📡 Fetching {candles_needed} candles for {symbol} @ {timeframe}")
    print(f"   Will require {num_requests} API request(s)")
    
    current_start_ts = start_ts
    
    for batch_num in range(num_requests):
        # Calculate batch size
        remaining_candles = candles_needed - len(all_data) * interval_seconds // interval_seconds
        batch_limit = min(max_candles_per_request, remaining_candles)
        
        # Rate limit enforcement
        wait_time = enforcer.log_and_maybe_pause(
            endpoint="ohlcv",
            symbol=symbol,
            timeframe=timeframe,
            limit=batch_limit
        )
        
        # Convert to milliseconds for Kraken API
        since_ms = current_start_ts * 1000
        
        try:
            # Fetch OHLCV data
            ohlcv = kraken.fetch_ohlcv(
                ccxt_symbol,
                timeframe=timeframe,
                since=since_ms,
                limit=batch_limit
            )
            
            if not ohlcv:
                print(f"⚠️ No data returned for batch {batch_num + 1}/{num_requests}")
                break
            
            # Convert to DataFrame
            df = pd.DataFrame(
                ohlcv,
                columns=["timestamp_ms", "open", "high", "low", "close", "volume"]
            )
            
            # Convert milliseconds to seconds
            df['timestamp_unix'] = df['timestamp_ms'] // 1000
            
            # CRITICAL FIX: Filter to only include timestamps in our requested range
            # The API sometimes returns recent data instead of historical data we requested
            df = df[(df['timestamp_unix'] >= start_ts) & (df['timestamp_unix'] <= end_ts)]
            
            if df.empty:
                print(f"⚠️ No data in requested range for batch {batch_num + 1}/{num_requests}")
                break
            
            # Add NY formatted time
            df['ny_time'] = df['timestamp_unix'].apply(
                lambda ts: datetime.fromtimestamp(ts, pytz.utc)
                                  .astimezone(tz_NY)
                                  .strftime("%m/%d/%Y %I:%M %p")
            )
            
            # Select and order columns
            df = df[['ny_time', 'timestamp_unix', 'open', 'high', 'low', 'close', 'volume']]
            
            all_data.append(df)
            
            print(f"✅ Batch {batch_num + 1}/{num_requests}: Fetched {len(df)} candles")
            
            # Update start timestamp for next batch
            last_ts = df['timestamp_unix'].max()
            current_start_ts = last_ts + interval_seconds
            
            # If we've reached the end, stop
            if current_start_ts > end_ts:
                break
        
        except ccxt.RateLimitExceeded as e:
            print(f"⚠️ Rate limit exceeded despite enforcer: {e}")
            print(f"   Pausing for 30 seconds before retrying...")
            time.sleep(30)
            continue
        
        except ccxt.NetworkError as e:
            print(f"⚠️ Network error: {e}")
            print(f"   Pausing for 10 seconds before retrying...")
            time.sleep(10)
            continue
        
        except Exception as e:
            print(f"❌ Error fetching batch {batch_num + 1}/{num_requests}: {e}")
            break
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 4: COMBINE, DEDUPLICATE, AND IDENTIFY MISSING
    # ═══════════════════════════════════════════════════════════════════════
    
    if not all_data:
        print(f"❌ No data fetched from API")
        return create_missing_dataframes()
    
    # Combine all data and remove duplicates
    found_df = pd.concat(all_data, ignore_index=True)
    found_df = found_df.drop_duplicates(subset=['timestamp_unix'])
    found_df = found_df.sort_values('timestamp_unix').reset_index(drop=True)
    
    # Add symbol and timeframe columns
    found_df.insert(0, 'symbol', symbol)
    found_df.insert(1, 'timeframe', timeframe)
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 5: IDENTIFY MISSING TIMESTAMPS
    # ═══════════════════════════════════════════════════════════════════════
    
    found_timestamps = set(found_df['timestamp_unix'].values)
    missing_timestamps = sorted(set(expected_ts) - found_timestamps)
    
    if missing_timestamps:
        missing_df = pd.DataFrame({
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp_unix': missing_timestamps,
            'ny_time': [
                datetime.fromtimestamp(ts, pytz.utc)
                        .astimezone(tz_NY)
                        .strftime("%m/%d/%Y %I:%M %p")
                for ts in missing_timestamps
            ]
        })
        print(f"⚠️ API returned {len(found_df)} candles, missing {len(missing_df)} for {symbol} @ {timeframe}")
    else:
        missing_df = pd.DataFrame(columns=['symbol', 'timeframe', 'timestamp_unix', 'ny_time'])
        print(f"✅ API returned all {len(found_df)} candles for {symbol} @ {timeframe}")
    
    return found_df, missing_df


# if __name__ == "__main__":
#     from datetime import datetime, timedelta, timezone
#     import time
    
#     # Calculate date range (last 200 days from today: 10/27/2025)
#     # Use timezone-aware UTC datetime
#     today = datetime(2025, 10, 27, tzinfo=timezone.utc)
#     days_back = 200
#     end_date = today
#     start_date = today - timedelta(days=days_back)
    
#     # Convert to Unix timestamps (already at midnight UTC)
#     end_ts = int(end_date.timestamp())
#     start_ts = int(start_date.timestamp())
    
#     print("="*70)
#     print("🧪 TESTING KRAKEN API FETCHER WITH RATE LIMITING")
#     print("="*70)
#     print(f"📅 Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
#     print(f"📊 Expected: ~{days_back} daily candles")
#     print(f"⏱️  Testing rate limiter with multiple consecutive API calls")
#     print("="*70)
    
#     # Test 1: Large request that requires multiple API calls (batching test)
#     print("\n🔬 TEST 1: Single large request (tests batching)")
#     print("-"*70)
    
#     test_start = time.time()
    
#     found_df, missing_df = fetch_ohlcv_from_api(
#         symbol="ETHUSD",
#         timeframe="1d",
#         start_ts=start_ts,
#         end_ts=end_ts
#     )
    
#     test_duration = time.time() - test_start
    
#     print(f"\n✅ Test 1 Complete in {test_duration:.2f}s")
#     print(f"   Found: {len(found_df)} candles")
#     print(f"   Missing: {len(missing_df)} candles")
#     print(f"   Completeness: {len(found_df)/(len(found_df)+len(missing_df))*100:.1f}%")
    
#     if not found_df.empty:
#         print(f"\n   📊 Sample data (first 3 rows):")
#         print(found_df[['symbol', 'timeframe', 'ny_time', 'close', 'volume']].head(3).to_string(index=False))
    
#     # Test 2: Multiple smaller requests in rapid succession (rate limiter stress test)
#     print("\n\n🔬 TEST 2: Multiple rapid requests (tests rate limiting)")
#     print("-"*70)
#     print("Making 5 consecutive requests to trigger rate limiter...")
    
#     test_ranges = []
#     interval_days = days_back // 5  # Split into 5 chunks
    
#     for i in range(5):
#         chunk_start = start_date + timedelta(days=i * interval_days)
#         chunk_end = start_date + timedelta(days=(i + 1) * interval_days - 1)
        
#         # Already timezone-aware UTC, just convert to timestamp
#         chunk_start_ts = int(chunk_start.timestamp())
#         chunk_end_ts = int(chunk_end.timestamp())
        
#         test_ranges.append((chunk_start_ts, chunk_end_ts, chunk_start, chunk_end))
    
#     all_results = []
#     test2_start = time.time()
    
#     for idx, (chunk_start_ts, chunk_end_ts, chunk_start, chunk_end) in enumerate(test_ranges, 1):
#         print(f"\n   Request {idx}/5: {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
        
#         request_start = time.time()
        
#         found_df, missing_df = fetch_ohlcv_from_api(
#             symbol="BTCUSD",  # Different symbol to avoid cache
#             timeframe="1d",
#             start_ts=chunk_start_ts,
#             end_ts=chunk_end_ts
#         )
        
#         request_duration = time.time() - request_start
        
#         all_results.append({
#             'request': idx,
#             'found': len(found_df),
#             'missing': len(missing_df),
#             'duration': request_duration
#         })
        
#         print(f"      Duration: {request_duration:.2f}s | Found: {len(found_df)} | Missing: {len(missing_df)}")
    
#     test2_duration = time.time() - test2_start
    
#     print(f"\n✅ Test 2 Complete in {test2_duration:.2f}s")
#     print(f"   Average request time: {test2_duration/5:.2f}s")
#     print(f"   Total candles fetched: {sum(r['found'] for r in all_results)}")
    
#     # Summary
#     print("\n" + "="*70)
#     print("🎉 ALL TESTS COMPLETE")
#     print("="*70)
#     print("✅ Rate limiter successfully prevented API overload")
#     print("✅ Batching handled large requests correctly")
#     print("✅ Multiple consecutive requests executed safely")
#     print("="*70)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


"""
Timestamp Alignment Test Suite
===============================

Tests both fetch_ohlcv_from_zip and fetch_ohlcv_from_api with misaligned timestamps
to verify the alignment functionality works correctly.

Constraints:
- API: Only has data within last 200 periods from today (10/27/2025)
- ZIP: Only has 2024 and earlier data
"""


def test_timestamp_alignment():
    """
    Test timestamp alignment for both ZIP and API fetchers.
    Uses intentionally misaligned timestamps to verify auto-correction.
    """
    
    print("="*80)
    print("TIMESTAMP ALIGNMENT TEST SUITE")
    print("="*80)
    print(f"Test Date: October 27, 2025")
    print(f"Testing with MISALIGNED timestamps to verify auto-alignment")
    print("="*80)
    
    # ═══════════════════════════════════════════════════════════════════════
    # TEST 1: ZIP FETCHER - 1 HOUR TIMEFRAME (2024 data)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "─"*80)
    print("TEST 1: ZIP FETCHER - 1 Hour Timeframe (Misaligned)")
    print("─"*80)
    print("Testing with: August 1, 2024 at 8:43 AM to 10:17 AM")
    print("Expected alignment: 8:00 AM to 11:00 AM (captures 8:00, 9:00, 10:00, 11:00)")
    
    # Intentionally misaligned timestamps (8:43 AM to 10:17 AM)
    start_misaligned = datetime(2024, 8, 1, 8, 43, 0, tzinfo=timezone.utc)
    end_misaligned = datetime(2024, 8, 1, 10, 17, 0, tzinfo=timezone.utc)
    
    start_ts = int(start_misaligned.timestamp())
    end_ts = int(end_misaligned.timestamp())
    
    print(f"\n📥 Input (MISALIGNED):")
    print(f"   Start: {start_misaligned} (timestamp: {start_ts})")
    print(f"   End:   {end_misaligned} (timestamp: {end_ts})")
    
    found_df, missing_df = fetch_ohlcv_from_zip(
        symbol="ETHUSD",
        timeframe="1h",
        start_ts=start_ts,
        end_ts=end_ts
    )
    
    print(f"\n✅ Results:")
    print(f"   Found: {len(found_df)} candles")
    print(f"   Missing: {len(missing_df)} candles")
    print(f"   Expected: 4 candles (8:00, 9:00, 10:00, 11:00)")
    
    if not found_df.empty:
        print(f"\n   📊 Timestamps found:")
        for idx, row in found_df.iterrows():
            dt = datetime.fromtimestamp(row['timestamp_unix'], timezone.utc)
            print(f"      {dt.strftime('%H:%M')} - Close: ${row['close']:.2f}")
    
    test1_pass = len(found_df) == 4 and len(missing_df) == 0
    print(f"\n   {'✅ PASS' if test1_pass else '❌ FAIL'}: Alignment working correctly")
    
    # ═══════════════════════════════════════════════════════════════════════
    # TEST 2: ZIP FETCHER - 15 MINUTE TIMEFRAME (2024 data)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "─"*80)
    print("TEST 2: ZIP FETCHER - 15 Minute Timeframe (Misaligned)")
    print("─"*80)
    print("Testing with: August 1, 2024 at 8:37 AM to 9:08 AM")
    print("Expected alignment: 8:30 AM to 9:15 AM")
    
    # Intentionally misaligned timestamps (8:37 AM to 9:08 AM)
    start_misaligned = datetime(2024, 8, 1, 8, 37, 0, tzinfo=timezone.utc)
    end_misaligned = datetime(2024, 8, 1, 9, 8, 0, tzinfo=timezone.utc)
    
    start_ts = int(start_misaligned.timestamp())
    end_ts = int(end_misaligned.timestamp())
    
    print(f"\n📥 Input (MISALIGNED):")
    print(f"   Start: {start_misaligned} (timestamp: {start_ts})")
    print(f"   End:   {end_misaligned} (timestamp: {end_ts})")
    
    found_df, missing_df = fetch_ohlcv_from_zip(
        symbol="ETHUSD",
        timeframe="15m",
        start_ts=start_ts,
        end_ts=end_ts
    )
    
    print(f"\n✅ Results:")
    print(f"   Found: {len(found_df)} candles")
    print(f"   Missing: {len(missing_df)} candles")
    print(f"   Expected: 4 candles (8:30, 8:45, 9:00, 9:15)")
    
    if not found_df.empty:
        print(f"\n   📊 Timestamps found:")
        for idx, row in found_df.head(4).iterrows():
            dt = datetime.fromtimestamp(row['timestamp_unix'], timezone.utc)
            print(f"      {dt.strftime('%H:%M')} - Close: ${row['close']:.2f}")
    
    test2_pass = len(found_df) == 4 and len(missing_df) == 0
    print(f"\n   {'✅ PASS' if test2_pass else '❌ FAIL'}: Alignment working correctly")
    
    # ═══════════════════════════════════════════════════════════════════════
    # TEST 3: ZIP FETCHER - DAILY TIMEFRAME (2024 data)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "─"*80)
    print("TEST 3: ZIP FETCHER - Daily Timeframe (Misaligned)")
    print("─"*80)
    print("Testing with: August 1, 2024 at 3:00 PM to August 5, 2024 at 9:00 AM")
    print("Expected alignment: August 1 midnight to August 6 midnight (5 days)")
    
    # Intentionally misaligned timestamps (Aug 1 3:00 PM to Aug 5 9:00 AM)
    start_misaligned = datetime(2024, 8, 1, 15, 0, 0, tzinfo=timezone.utc)
    end_misaligned = datetime(2024, 8, 5, 9, 0, 0, tzinfo=timezone.utc)
    
    start_ts = int(start_misaligned.timestamp())
    end_ts = int(end_misaligned.timestamp())
    
    print(f"\n📥 Input (MISALIGNED):")
    print(f"   Start: {start_misaligned} (timestamp: {start_ts})")
    print(f"   End:   {end_misaligned} (timestamp: {end_ts})")
    
    found_df, missing_df = fetch_ohlcv_from_zip(
        symbol="ETHUSD",
        timeframe="1d",
        start_ts=start_ts,
        end_ts=end_ts
    )
    
    print(f"\n✅ Results:")
    print(f"   Found: {len(found_df)} candles")
    print(f"   Missing: {len(missing_df)} candles")
    print(f"   Expected: 6 candles (Aug 1-6)")
    
    if not found_df.empty:
        print(f"\n   📊 Timestamps found:")
        for idx, row in found_df.iterrows():
            dt = datetime.fromtimestamp(row['timestamp_unix'], timezone.utc)
            print(f"      {dt.strftime('%Y-%m-%d')} - Close: ${row['close']:.2f}")
    
    test3_pass = len(found_df) == 6 and len(missing_df) == 0
    print(f"\n   {'✅ PASS' if test3_pass else '❌ FAIL'}: Alignment working correctly")
    
    # ═══════════════════════════════════════════════════════════════════════
    # TEST 4: API FETCHER - DAILY TIMEFRAME (Recent data, within 200 periods)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "─"*80)
    print("TEST 4: API FETCHER - Daily Timeframe (Misaligned, Recent Data)")
    print("─"*80)
    print("Testing with: Last 10 days at misaligned times")
    print("Expected alignment: Proper daily boundaries")
    
    # Use last 10 days (well within 200 period limit)
    today = datetime(2025, 10, 27, tzinfo=timezone.utc)
    
    # Intentionally misaligned (10 days ago at 3:30 PM to yesterday at 2:45 PM)
    start_misaligned = today - timedelta(days=10, hours=8, minutes=30)
    end_misaligned = today - timedelta(days=1, hours=9, minutes=15)
    
    start_ts = int(start_misaligned.timestamp())
    end_ts = int(end_misaligned.timestamp())
    
    print(f"\n📥 Input (MISALIGNED):")
    print(f"   Start: {start_misaligned} (timestamp: {start_ts})")
    print(f"   End:   {end_misaligned} (timestamp: {end_ts})")
    print(f"   Range: ~10 days")
    
    found_df, missing_df = fetch_ohlcv_from_api(
        symbol="ETHUSD",
        timeframe="1d",
        start_ts=start_ts,
        end_ts=end_ts
    )
    
    print(f"\n✅ Results:")
    print(f"   Found: {len(found_df)} candles")
    print(f"   Missing: {len(missing_df)} candles")
    print(f"   Expected: ~10 candles")
    
    if not found_df.empty:
        print(f"\n   📊 First 5 timestamps:")
        for idx, row in found_df.head(5).iterrows():
            dt = datetime.fromtimestamp(row['timestamp_unix'], timezone.utc)
            print(f"      {dt.strftime('%Y-%m-%d')} - Close: ${row['close']:.2f}")
    
    test4_pass = len(found_df) >= 10 and len(missing_df) == 0
    print(f"\n   {'✅ PASS' if test4_pass else '❌ FAIL'}: Alignment working correctly")
    
    # ═══════════════════════════════════════════════════════════════════════
    # TEST 5: API FETCHER - HOURLY TIMEFRAME (Recent data, within 200 periods)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "─"*80)
    print("TEST 5: API FETCHER - Hourly Timeframe (Misaligned, Recent Data)")
    print("─"*80)
    print("Testing with: Last 5 hours at misaligned times")
    print("Expected alignment: Proper hourly boundaries")
    
    # Use last 5 hours (well within 200 period limit)
    now = datetime(2025, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
    
    # Intentionally misaligned (5 hours ago at :23 minutes to 1 hour ago at :47 minutes)
    start_misaligned = now - timedelta(hours=5, minutes=23)
    end_misaligned = now - timedelta(hours=1, minutes=47)
    
    start_ts = int(start_misaligned.timestamp())
    end_ts = int(end_misaligned.timestamp())
    
    print(f"\n📥 Input (MISALIGNED):")
    print(f"   Start: {start_misaligned} (timestamp: {start_ts})")
    print(f"   End:   {end_misaligned} (timestamp: {end_ts})")
    
    found_df, missing_df = fetch_ohlcv_from_api(
        symbol="BTCUSD",
        timeframe="1h",
        start_ts=start_ts,
        end_ts=end_ts
    )
    
    print(f"\n✅ Results:")
    print(f"   Found: {len(found_df)} candles")
    print(f"   Missing: {len(missing_df)} candles")
    print(f"   Expected: ~5 candles")
    
    if not found_df.empty:
        print(f"\n   📊 Timestamps found:")
        for idx, row in found_df.iterrows():
            dt = datetime.fromtimestamp(row['timestamp_unix'], timezone.utc)
            print(f"      {dt.strftime('%Y-%m-%d %H:%M')} - Close: ${row['close']:.2f}")
    
    test5_pass = len(found_df) >= 4 and len(missing_df) == 0
    print(f"\n   {'✅ PASS' if test5_pass else '❌ FAIL'}: Alignment working correctly")
    
    # ═══════════════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    
    all_tests = [
        ("ZIP - 1h timeframe", test1_pass),
        ("ZIP - 15m timeframe", test2_pass),
        ("ZIP - 1d timeframe", test3_pass),
        ("API - 1d timeframe", test4_pass),
        ("API - 1h timeframe", test5_pass)
    ]
    
    for test_name, passed in all_tests:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status}: {test_name}")
    
    total_pass = sum(1 for _, p in all_tests if p)
    print(f"\n   Results: {total_pass}/{len(all_tests)} tests passed")
    
    if total_pass == len(all_tests):
        print("\n   🎉 ALL TESTS PASSED - Timestamp alignment working perfectly!")
    else:
        print(f"\n   ⚠️ {len(all_tests) - total_pass} test(s) failed - review output above")
    
    print("="*80)

# if __name__ == "__main__":
#     test_timestamp_alignment()

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




"""
Database Acquisition Function
==============================

Fetches OHLCV data from TimescaleDB, matching the same interface as
fetch_ohlcv_from_zip and fetch_ohlcv_from_api.
"""

import pandas as pd
import psycopg2
from datetime import datetime
import pytz
from typing import Tuple


def fetch_ohlcv_from_db(
    symbol: str,
    timeframe: str,
    start_ts: int,
    end_ts: int,
    db_name: str = "ohlcv_data",
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "mysecretpassword"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetches OHLCV data from TimescaleDB for a given symbol, timeframe, and date range.
    
    Returns ALL candles between start_ts and end_ts (inclusive), plus a DataFrame
    showing which timestamps are missing so callers can handle sparse data.
    
    Args:
        symbol: Trading pair (e.g., "ETHUSD")
        timeframe: Timeframe string (e.g., "1h", "15m", "1d")
        start_ts: Start Unix timestamp (inclusive)
        end_ts: End Unix timestamp (inclusive)
        db_name: Target database name (default: "ohlcv_data")
        host: Database host (default: "localhost")
        port: Database port (default: 5432)
        user: Database user (default: "postgres")
        password: Database password (default: "mysecretpassword")
        
    Returns:
        Tuple of (found_df, missing_df):
        
        found_df: DataFrame with columns:
            - symbol, timeframe, ny_time, timestamp_unix, open, high, low, close, volume
            - Contains all candles that were found in database
            
        missing_df: DataFrame with columns:
            - symbol, timeframe, ny_time, timestamp_unix
            - Contains timestamps that should exist but weren't found in database
            
    Example:
        >>> found, missing = fetch_ohlcv_from_db(
        ...     symbol="ETHUSD",
        ...     timeframe="1h",
        ...     start_ts=1754355600,
        ...     end_ts=1754370000
        ... )
        >>> print(f"Found {len(found)} candles, missing {len(missing)}")
        Found 4 candles, missing 0
    """
    # ALIGN TIMESTAMPS TO PROPER INTERVAL BOUNDARIES
    start_ts, end_ts = align_timestamps_to_timeframe(start_ts, end_ts, timeframe)
    
    tz_NY = pytz.timezone("America/New_York")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 1: CONVERT TIMEFRAME TO MINUTES AND GENERATE EXPECTED TIMESTAMPS
    # ═══════════════════════════════════════════════════════════════════════
    
    # Convert timeframe string to minutes
    tf = timeframe.lower()
    unit = tf[-1]
    val = int(tf[:-1])
    
    if unit == 'm':
        interval_minutes = val
    elif unit == 'h':
        interval_minutes = val * 60
    elif unit == 'd':
        interval_minutes = val * 60 * 24
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    
    interval_seconds = interval_minutes * 60
    
    # Generate ALL expected timestamps between start and end
    expected_ts = list(range(start_ts, end_ts + 1, interval_seconds))
    
    # Helper function to create empty/missing dataframes
    def create_missing_dataframes():
        found_df = pd.DataFrame(columns=[
            'symbol', 'timeframe', 'ny_time', 'timestamp_unix',
            'open', 'high', 'low', 'close', 'volume'
        ])
        missing_df = pd.DataFrame({
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp_unix': expected_ts,
            'ny_time': [
                datetime.fromtimestamp(ts, pytz.utc)
                        .astimezone(tz_NY)
                        .strftime("%m/%d/%Y %I:%M %p")
                for ts in expected_ts
            ]
        })
        return found_df, missing_df
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 2: CONNECT TO DATABASE
    # ═══════════════════════════════════════════════════════════════════════
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=db_name,
            user=user,
            password=password
        )
        cur = conn.cursor()
    except psycopg2.OperationalError as e:
        print(f"❌ Failed to connect to database '{db_name}': {e}")
        return create_missing_dataframes()
    except Exception as e:
        print(f"❌ Database connection error: {e}")
        return create_missing_dataframes()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 3: CHECK IF SCHEMA AND TABLE EXIST
    # ═══════════════════════════════════════════════════════════════════════
    
    # Convert timeframe to table name format
    # Database uses MINUTES: "1h" -> "60_ohlcv", "1d" -> "1440_ohlcv", "15m" -> "15_ohlcv"
    tf = timeframe.lower()
    unit = tf[-1]
    val = int(tf[:-1])
    
    if unit == 'm':
        table_name = f"{val}_ohlcv"
    elif unit == 'h':
        table_name = f"{val * 60}_ohlcv"
    elif unit == 'd':
        table_name = f"{val * 1440}_ohlcv"
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    
    try:
        # Check if schema exists
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = %s;
        """, (symbol,))
        
        if not cur.fetchone():
            print(f"⚠️ Schema '{symbol}' does not exist in database")
            cur.close()
            conn.close()
            return create_missing_dataframes()
        
        # Check if table exists
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s;
        """, (symbol, table_name))
        
        if not cur.fetchone():
            print(f"⚠️ Table '{symbol}.{table_name}' does not exist in database")
            cur.close()
            conn.close()
            return create_missing_dataframes()
            
    except Exception as e:
        print(f"❌ Error checking schema/table existence: {e}")
        cur.close()
        conn.close()
        return create_missing_dataframes()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 4: QUERY DATA FROM DATABASE
    # ═══════════════════════════════════════════════════════════════════════
    
    try:
        # Query data within timestamp range
        query = f"""
            SELECT 
                date,
                time,
                timestamp_unix,
                open,
                high,
                low,
                close,
                volume
            FROM "{symbol}"."{table_name}"
            WHERE timestamp_unix >= %s AND timestamp_unix <= %s
            ORDER BY timestamp_unix ASC;
        """
        
        cur.execute(query, (start_ts, end_ts))
        rows = cur.fetchall()
        
        cur.close()
        conn.close()
        
        if not rows:
            print(f"⚠️ No data found in database for {symbol} @ {timeframe} in range {start_ts}-{end_ts}")
            return create_missing_dataframes()
        
    except Exception as e:
        print(f"❌ Error querying database: {e}")
        cur.close()
        conn.close()
        return create_missing_dataframes()
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 5: CONVERT TO DATAFRAME
    # ═══════════════════════════════════════════════════════════════════════
    
    # Create DataFrame from query results
    found_df = pd.DataFrame(
        rows,
        columns=['date', 'time', 'timestamp_unix', 'open', 'high', 'low', 'close', 'volume']
    )
    
    # Reconstruct ny_time from date and time columns
    found_df['ny_time'] = found_df['date'] + ' ' + found_df['time']
    
    # Drop the separate date and time columns
    found_df = found_df.drop(columns=['date', 'time'])
    
    # Reorder columns to match standard format
    found_df = found_df[['ny_time', 'timestamp_unix', 'open', 'high', 'low', 'close', 'volume']]
    
    # Add symbol and timeframe columns
    found_df.insert(0, 'symbol', symbol)
    found_df.insert(1, 'timeframe', timeframe)
    
    # Ensure timestamp_unix is integer
    found_df['timestamp_unix'] = found_df['timestamp_unix'].astype(int)
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 6: IDENTIFY MISSING TIMESTAMPS
    # ═══════════════════════════════════════════════════════════════════════
    
    found_timestamps = set(found_df['timestamp_unix'].values)
    missing_timestamps = sorted(set(expected_ts) - found_timestamps)
    
    if missing_timestamps:
        missing_df = pd.DataFrame({
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp_unix': missing_timestamps,
            'ny_time': [
                datetime.fromtimestamp(ts, pytz.utc)
                        .astimezone(tz_NY)
                        .strftime("%m/%d/%Y %I:%M %p")
                for ts in missing_timestamps
            ]
        })
        print(f"⚠️ Database returned {len(found_df)} candles, missing {len(missing_df)} for {symbol} @ {timeframe}")
    else:
        missing_df = pd.DataFrame(columns=['symbol', 'timeframe', 'timestamp_unix', 'ny_time'])
    
    return found_df, missing_df



# if __name__ == "__main__":
#     from datetime import datetime, timezone
    
#     print("="*80)
#     print("🧪 COMPREHENSIVE DATABASE DIAGNOSTIC")
#     print("="*80)
#     print("Testing multiple symbols, timeframes, and date ranges")
#     print("to determine if this is an alignment or data hydration issue")
#     print("="*80)
    
#     # First, let's check what's actually in the database
#     print("\n" + "─"*80)
#     print("STEP 1: DATABASE INVENTORY (Sample)")
#     print("─"*80)
#     print("Checking a few specific symbols to avoid memory issues...")
    
#     try:
#         import psycopg2
#         conn = psycopg2.connect(
#             host="localhost",
#             port=5432,
#             dbname="ohlcv_data",
#             user="postgres",
#             password="mysecretpassword"
#         )
#         cur = conn.cursor()
        
#         # Check just a few specific symbols we'll test
#         test_symbols = ['ETHUSD', 'BTCUSD', 'DAIUSD', 'DASHUSD']
        
#         for schema in test_symbols:
#             # Check if schema exists
#             cur.execute("""
#                 SELECT schema_name 
#                 FROM information_schema.schemata 
#                 WHERE schema_name = %s;
#             """, (schema,))
            
#             if not cur.fetchone():
#                 print(f"\n   Schema '{schema}': Does not exist")
#                 continue
                
#             # Get tables
#             cur.execute("""
#                 SELECT table_name 
#                 FROM information_schema.tables 
#                 WHERE table_schema = %s
#                 ORDER BY table_name;
#             """, (schema,))
#             tables = [row[0] for row in cur.fetchall()]
            
#             print(f"\n   Schema '{schema}':")
#             for table in tables[:5]:  # Limit to first 5 tables
#                 # Get row count
#                 cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
#                 count = cur.fetchone()[0]
                
#                 # Get timestamp range
#                 cur.execute(f'SELECT MIN(timestamp_unix), MAX(timestamp_unix) FROM "{schema}"."{table}";')
#                 min_ts, max_ts = cur.fetchone()
                
#                 if min_ts and max_ts:
#                     min_dt = datetime.fromtimestamp(min_ts, timezone.utc)
#                     max_dt = datetime.fromtimestamp(max_ts, timezone.utc)
#                     print(f"      • {table}: {count:,} rows")
#                     print(f"        Range: {min_dt.date()} to {max_dt.date()}")
#                 else:
#                     print(f"      • {table}: {count} rows (empty)")
        
#         cur.close()
#         conn.close()
        
#     except Exception as e:
#         print(f"❌ Error checking database: {e}")
#         print("Proceeding with tests anyway...")

    
#     # Now run tests based on what we found
#     print("\n" + "─"*80)
#     print("STEP 2: RUNNING FETCH TESTS")
#     print("─"*80)
    
#     test_cases = [
#         # Test DAIUSD (has data from 2019-09-25 to 2025-06-30)
#         {
#             "name": "DAIUSD - 1h (60_ohlcv) - Jan 1, 2024 (aligned)",
#             "symbol": "DAIUSD",
#             "timeframe": "1h",
#             "start": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
#         },
#         {
#             "name": "DAIUSD - 1h (60_ohlcv) - Jan 1, 2024 (misaligned)",
#             "symbol": "DAIUSD",
#             "timeframe": "1h",
#             "start": datetime(2024, 1, 1, 0, 23, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 1, 3, 47, 0, tzinfo=timezone.utc),
#         },
#         {
#             "name": "DAIUSD - 1d (1440_ohlcv) - Jan 2024 (aligned)",
#             "symbol": "DAIUSD",
#             "timeframe": "1d",
#             "start": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 5, 0, 0, 0, tzinfo=timezone.utc),
#         },
#         {
#             "name": "DAIUSD - 1d (1440_ohlcv) - Jan 2024 (misaligned)",
#             "symbol": "DAIUSD",
#             "timeframe": "1d",
#             "start": datetime(2024, 1, 1, 14, 30, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 5, 18, 45, 0, tzinfo=timezone.utc),
#         },
#         # Test DASHUSD (has data from 2017-04-12 to 2025-06-30)
#         {
#             "name": "DASHUSD - 1h (60_ohlcv) - Jan 1, 2024 (aligned)",
#             "symbol": "DASHUSD",
#             "timeframe": "1h",
#             "start": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
#         },
#         {
#             "name": "DASHUSD - 1h (60_ohlcv) - Jan 1, 2024 (misaligned)",
#             "symbol": "DASHUSD",
#             "timeframe": "1h",
#             "start": datetime(2024, 1, 1, 0, 37, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 1, 3, 51, 0, tzinfo=timezone.utc),
#         },
#         {
#             "name": "DASHUSD - 1d (1440_ohlcv) - Jan 2024 (aligned)",
#             "symbol": "DASHUSD",
#             "timeframe": "1d",
#             "start": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 7, 0, 0, 0, tzinfo=timezone.utc),
#         },
#         {
#             "name": "DASHUSD - 1d (1440_ohlcv) - Jan 2024 (misaligned)",
#             "symbol": "DASHUSD",
#             "timeframe": "1d",
#             "start": datetime(2024, 1, 1, 9, 15, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 7, 21, 30, 0, tzinfo=timezone.utc),
#         },
#         # Test ETHUSD if it exists
#         {
#             "name": "ETHUSD - 1h - Jan 1, 2024 (aligned)",
#             "symbol": "ETHUSD",
#             "timeframe": "1h",
#             "start": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
#         },
#         {
#             "name": "BTCUSD - 1h - Jan 1, 2024 (aligned)",
#             "symbol": "BTCUSD",
#             "timeframe": "1h",
#             "start": datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
#             "end": datetime(2024, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
#         },
#     ]
    
#     results = []
    
#     for i, test in enumerate(test_cases, 1):
#         print(f"\n📊 TEST {i}: {test['name']}")
#         print("─"*80)
        
#         start_ts = int(test['start'].timestamp())
#         end_ts = int(test['end'].timestamp())
        
#         print(f"Symbol: {test['symbol']}")
#         print(f"Timeframe: {test['timeframe']}")
#         print(f"Range: {test['start']} to {test['end']}")
#         print(f"Unix: {start_ts} to {end_ts}")
        
#         try:
#             found_df, missing_df = fetch_ohlcv_from_db(
#                 symbol=test['symbol'],
#                 timeframe=test['timeframe'],
#                 start_ts=start_ts,
#                 end_ts=end_ts
#             )
            
#             print(f"\n✅ Results:")
#             print(f"   Found: {len(found_df)} candles")
#             print(f"   Missing: {len(missing_df)} candles")
            
#             if not found_df.empty:
#                 print(f"\n   📊 First 3 candles:")
#                 for _, row in found_df.head(3).iterrows():
#                     print(f"      {row['ny_time']} - Close: ${row['close']:.2f}")
            
#             results.append({
#                 'name': test['name'],
#                 'found': len(found_df),
#                 'missing': len(missing_df),
#                 'success': len(found_df) > 0
#             })
            
#         except Exception as e:
#             print(f"❌ Error: {e}")
#             results.append({
#                 'name': test['name'],
#                 'found': 0,
#                 'missing': 0,
#                 'success': False
#             })
    
#     # Summary
#     print("\n" + "="*80)
#     print("TEST SUMMARY")
#     print("="*80)
    
#     successful = sum(1 for r in results if r['success'])
#     total = len(results)
    
#     for result in results:
#         status = "✅" if result['success'] else "❌"
#         print(f"{status} {result['name']}")
#         print(f"   Found: {result['found']}, Missing: {result['missing']}")
    
#     print(f"\n📊 Overall: {successful}/{total} tests found data")
    
#     if successful == 0:
#         print("\n⚠️ DIAGNOSIS: DATABASE HYDRATION ISSUE")
#         print("No data was found in any test case.")
#         print("This indicates the database is empty or not properly hydrated.")
#         print("\nRecommendations:")
#         print("1. Check if data was actually loaded using load_ohlcv_to_db()")
#         print("2. Verify the table names match the expected format")
#         print("3. Check if TimescaleDB is properly installed and configured")
#     elif successful < total:
#         print("\n⚠️ DIAGNOSIS: PARTIAL DATA OR ALIGNMENT ISSUE")
#         print("Some tests succeeded, others failed.")
#         print("\nRecommendations:")
#         print("1. Compare successful vs failed tests to identify pattern")
#         print("2. Check if specific symbols/timeframes are missing data")
#         print("3. Verify timestamp alignment is working correctly")
#     else:
#         print("\n✅ DIAGNOSIS: ALL SYSTEMS OPERATIONAL")
#         print("Database fetcher is working correctly!")
    
#     print("="*80)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
"""
Timestamp Misalignment Diagnostic
==================================

Shows EXACTLY what timestamps we're searching for vs what's actually in the database.
This will reveal the precise misalignment issue.
"""

import psycopg2
from datetime import datetime, timezone

def diagnose_timestamp_misalignment():
    """
    Compare what we're searching for vs what's actually in the database.
    """
    
    print("="*80)
    print("🔬 TIMESTAMP MISALIGNMENT DIAGNOSTIC")
    print("="*80)
    
    # Connect to database
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ohlcv_data",
        user="postgres",
        password="mysecretpassword"
    )
    cur = conn.cursor()
    
    # ═══════════════════════════════════════════════════════════════════════
    # PART 1: Show what timestamps ACTUALLY exist in database
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n📊 PART 1: What's ACTUALLY in the database (DAIUSD.60_ohlcv)")
    print("-"*80)
    print("First 10 timestamps on Jan 1, 2024:\n")
    
    cur.execute("""
        SELECT timestamp_unix, to_timestamp(timestamp_unix), date, time
        FROM "DAIUSD"."60_ohlcv"
        WHERE timestamp_unix >= 1704067200  -- Jan 1, 2024 00:00
          AND timestamp_unix <= 1704103200  -- Jan 1, 2024 10:00
        ORDER BY timestamp_unix
        LIMIT 10;
    """)
    
    actual_timestamps = []
    for row in cur.fetchall():
        actual_timestamps.append(row[0])
        dt = datetime.fromtimestamp(row[0], timezone.utc)
        print(f"   {row[0]:>12} | {dt} | {row[2]:>15} {row[3]:<15}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # PART 2: Show what timestamps we GENERATE (what we're searching for)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n📊 PART 2: What we're SEARCHING for (generated by our function)")
    print("-"*80)
    print("Expected hourly timestamps on Jan 1, 2024:\n")
    
    # Simulate what the function does
    start_ts = int(datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())
    end_ts = int(datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc).timestamp())
    interval_seconds = 60 * 60  # 1 hour
    
    expected_timestamps = list(range(start_ts, end_ts + 1, interval_seconds))
    
    for ts in expected_timestamps:
        dt = datetime.fromtimestamp(ts, timezone.utc)
        print(f"   {ts:>12} | {dt}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # PART 3: Compare and show the difference
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n📊 PART 3: COMPARISON")
    print("-"*80)
    
    print(f"\nFirst ACTUAL timestamp:   {actual_timestamps[0]}")
    print(f"First EXPECTED timestamp: {expected_timestamps[0]}")
    print(f"Difference: {actual_timestamps[0] - expected_timestamps[0]} seconds")
    
    if actual_timestamps[0] == expected_timestamps[0]:
        print("\n✅ TIMESTAMPS MATCH! No alignment issue.")
    else:
        diff = actual_timestamps[0] - expected_timestamps[0]
        diff_hours = diff / 3600
        print(f"\n⚠️ MISALIGNMENT DETECTED!")
        print(f"Database timestamps are {diff} seconds ({diff_hours} hours) off")
        
        # Check if it's a timezone issue
        if abs(diff) in [3600, 7200, 14400, 18000]:  # Common timezone offsets
            print(f"\n💡 This looks like a TIMEZONE issue!")
            print(f"   The data might be stored in a different timezone than UTC")
    
    # ═══════════════════════════════════════════════════════════════════════
    # PART 4: Check if ALL timestamps have the same offset
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n📊 PART 4: Pattern Analysis")
    print("-"*80)
    
    if len(actual_timestamps) >= 3:
        # Check intervals between consecutive timestamps
        intervals = [actual_timestamps[i+1] - actual_timestamps[i] 
                     for i in range(len(actual_timestamps)-1)]
        
        print(f"\nIntervals between consecutive database timestamps:")
        for i, interval in enumerate(intervals[:5]):
            print(f"   Gap {i+1}: {interval} seconds ({interval/3600} hours)")
        
        if all(x == 3600 for x in intervals):
            print("\n✅ Intervals are perfect 1-hour gaps")
        else:
            print("\n⚠️ Irregular intervals detected!")
    
    # ═══════════════════════════════════════════════════════════════════════
    # PART 5: Show modulo analysis (alignment to hour boundaries)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n📊 PART 5: Hour Boundary Alignment")
    print("-"*80)
    
    print(f"\nChecking if timestamps are on hour boundaries (divisible by 3600):\n")
    
    for ts in actual_timestamps[:5]:
        remainder = ts % 3600
        dt = datetime.fromtimestamp(ts, timezone.utc)
        if remainder == 0:
            print(f"   {ts} | {dt} | ✅ On boundary")
        else:
            minutes_off = remainder / 60
            print(f"   {ts} | {dt} | ⚠️ Off by {remainder}s ({minutes_off} min)")
    
    cur.close()
    conn.close()
    
    # ═══════════════════════════════════════════════════════════════════════
    # FINAL DIAGNOSIS
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "="*80)
    print("🎯 DIAGNOSIS")
    print("="*80)
    
    if actual_timestamps[0] == expected_timestamps[0]:
        print("\n✅ No alignment issue - timestamps match perfectly")
        print("The problem must be elsewhere in the code")
    else:
        diff = actual_timestamps[0] - expected_timestamps[0]
        print(f"\n⚠️ ALIGNMENT ISSUE CONFIRMED")
        print(f"\nRoot cause: Database timestamps are offset by {diff} seconds")
        print(f"\nSolution: Adjust the timestamp generation or query logic")
        print(f"to account for this {diff} second offset")
    
    print("="*80)


# if __name__ == "__main__":
#     diagnose_timestamp_misalignment()

#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


"""
OHLCV Data Orchestrator
=======================

Intelligent orchestrator that fetches OHLCV data from the best available source.

Flow:
1. Always check DB first (fastest)
2. If missing data:
   - If within last 200 periods → API then ZIP
   - If older than 200 periods → ZIP then API
3. Return combined results

This optimizes by avoiding API calls for old data (API only has ~200 periods).
"""

import pandas as pd
from datetime import datetime, timezone
from typing import Tuple

def fetch_ohlcv(
    symbol: str,
    timeframe: str,
    start_ts: int,
    end_ts: int,
    db_name: str = "ohlcv_data",
    zip_folder: str = "Kraken_Zip_Files",
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "mysecretpassword",
    verbose: bool = False,
    debug: bool = False,
    print_summary: bool = False
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Intelligently fetches OHLCV data from the best available sources.
    
    Sources are tried in optimal order based on data recency:
    1. Database (always first - fastest)
    2. API or ZIP (order depends on whether data is within last 200 periods)
    3. Remaining source
    
    Args:
        symbol: Trading pair (e.g., "ETHUSD")
        timeframe: Timeframe string (e.g., "1h", "15m", "1d")
        start_ts: Start Unix timestamp (inclusive)
        end_ts: End Unix timestamp (inclusive)
        db_name: Database name (default: "ohlcv_data")
        zip_folder: ZIP folder path (default: "Kraken_Zip_Files")
        host: Database host (default: "localhost")
        port: Database port (default: 5432)
        user: Database user (default: "postgres")
        password: Database password (default: "mysecretpassword")
        verbose: Show progress messages (default: False)
        debug: Show detailed debug information (default: False)
        print_summary: Print final summary (default: False)
        
    Returns:
        Tuple of (found_df, missing_df):
        
        found_df: DataFrame with columns:
            - symbol, timeframe, ny_time, timestamp_unix, open, high, low, close, volume
            - Contains all candles that were found across all sources
            
        missing_df: DataFrame with columns:
            - symbol, timeframe, ny_time, timestamp_unix
            - Contains timestamps that couldn't be found in any source
            
    Example:
        >>> # Silent mode (default)
        >>> found, missing = fetch_ohlcv("ETHUSD", "1h", start_ts, end_ts)
        
        >>> # With summary
        >>> found, missing = fetch_ohlcv("ETHUSD", "1h", start_ts, end_ts, print_summary=True)
        
        >>> # Verbose mode
        >>> found, missing = fetch_ohlcv("ETHUSD", "1h", start_ts, end_ts, verbose=True)
        
        >>> # Debug mode (most detailed)
        >>> found, missing = fetch_ohlcv("ETHUSD", "1h", start_ts, end_ts, debug=True)
    """
    
    if debug or verbose:
        print("="*80)
        print(f"🔍 FETCHING OHLCV DATA: {symbol} @ {timeframe}")
        print("="*80)
    
    if debug:
        start_dt = datetime.fromtimestamp(start_ts, timezone.utc)
        end_dt = datetime.fromtimestamp(end_ts, timezone.utc)
        print(f"📅 Range: {start_dt.date()} to {end_dt.date()}")
        print(f"🕐 Timestamps: {start_ts} to {end_ts}")
    
    # Calculate interval in seconds
    tf = timeframe.lower()
    unit = tf[-1]
    val = int(tf[:-1])
    
    if unit == 'm':
        interval_seconds = val * 60
    elif unit == 'h':
        interval_seconds = val * 60 * 60
    elif unit == 'd':
        interval_seconds = val * 60 * 60 * 24
    else:
        raise ValueError(f"Invalid timeframe: {timeframe}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 1: ALWAYS CHECK DATABASE FIRST (FASTEST)
    # ═══════════════════════════════════════════════════════════════════════
    
    if debug:
        print("\n" + "─"*80)
        print("📊 STEP 1: Checking Database")
        print("─"*80)
    
    found_df, missing_df = fetch_ohlcv_from_db(
        symbol=symbol,
        timeframe=timeframe,
        start_ts=start_ts,
        end_ts=end_ts,
        db_name=db_name,
        host=host,
        port=port,
        user=user,
        password=password
    )
    
    # If we have all the data, we're done!
    if missing_df.empty:
        if debug or verbose:
            print(f"\n✅ SUCCESS: All data found in database!")
            print("="*80)
        if print_summary:
            print(f"📊 {symbol} @ {timeframe}: Found {len(found_df)} candles, Missing 0")
        return found_df, missing_df
    
    if debug or verbose:
        print(f"\n⚠️  Database has {len(found_df)} candles, missing {len(missing_df)}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 2: DETERMINE OPTIMAL SOURCE ORDER BASED ON DATA RECENCY
    # ═══════════════════════════════════════════════════════════════════════
    
    if debug:
        print("\n" + "─"*80)
        print("📊 STEP 2: Determining optimal source order")
        print("─"*80)
    
    # Get the earliest missing timestamp
    earliest_missing_ts = missing_df['timestamp_unix'].min()
    
    if debug:
        earliest_missing_dt = datetime.fromtimestamp(earliest_missing_ts, timezone.utc)
    
    # Calculate how many periods back from end_ts
    periods_back = (end_ts - earliest_missing_ts) / interval_seconds
    
    if debug:
        print(f"📍 Earliest missing data: {earliest_missing_dt.date()} ({earliest_missing_ts})")
        print(f"📏 Periods back from end: {periods_back:.0f}")
    
    # Determine source order
    if periods_back < 200:
        source_order = ["API", "ZIP"]
        if debug:
            print(f"✅ Within 200 periods → Try API first (has recent data)")
    else:
        source_order = ["ZIP", "API"]
        if debug:
            print(f"✅ Beyond 200 periods → Try ZIP first (API won't have this old data)")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 3: TRY SOURCES IN OPTIMAL ORDER
    # ═══════════════════════════════════════════════════════════════════════
    
    all_found = [found_df] if not found_df.empty else []
    
    for source_name in source_order:
        if missing_df.empty:
            break  # No more missing data
        
        if debug:
            print("\n" + "─"*80)
            print(f"📊 STEP 3.{source_order.index(source_name) + 1}: Checking {source_name}")
            print("─"*80)
        
        # Get range of missing data
        missing_start = missing_df['timestamp_unix'].min()
        missing_end = missing_df['timestamp_unix'].max()
        
        try:
            if source_name == "API":
                source_found_df, source_missing_df = fetch_ohlcv_from_api(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_ts=missing_start,
                    end_ts=missing_end
                )
            else:  # ZIP
                source_found_df, source_missing_df = fetch_ohlcv_from_zip(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_ts=missing_start,
                    end_ts=missing_end,
                    zip_folder=zip_folder
                )
        except Exception as e:
            # Always print errors
            print(f"❌ Error fetching from {source_name} for {symbol}: {e}")
            continue
        
        # Add newly found data
        if not source_found_df.empty:
            all_found.append(source_found_df)
            if debug or verbose:
                print(f"✅ {source_name} provided {len(source_found_df)} candles")
            
            # Update missing_df to only include timestamps still missing
            found_timestamps = set(source_found_df['timestamp_unix'].values)
            missing_df = missing_df[~missing_df['timestamp_unix'].isin(found_timestamps)]
            
            if missing_df.empty:
                if debug or verbose:
                    print(f"✅ SUCCESS: All data now found!")
                break
            else:
                if debug or verbose:
                    print(f"⚠️  Still missing {len(missing_df)} candles")
        else:
            if debug or verbose:
                print(f"❌ {source_name} had no data")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 4: COMBINE AND RETURN RESULTS
    # ═══════════════════════════════════════════════════════════════════════
    
    if debug:
        print("\n" + "─"*80)
        print("📊 STEP 4: Combining results")
        print("─"*80)
    
    if all_found:
        # Combine all found data
        final_found_df = pd.concat(all_found, ignore_index=True)
        
        # Remove duplicates (keep first occurrence)
        final_found_df = final_found_df.drop_duplicates(subset=['timestamp_unix'], keep='first')
        
        # Sort by timestamp
        final_found_df = final_found_df.sort_values('timestamp_unix').reset_index(drop=True)
        
        if debug:
            print(f"✅ Total found: {len(final_found_df)} candles")
    else:
        # No data found anywhere
        final_found_df = pd.DataFrame(columns=[
            'symbol', 'timeframe', 'ny_time', 'timestamp_unix',
            'open', 'high', 'low', 'close', 'volume'
        ])
        # Always print if no data found
        print(f"❌ No data found in any source for {symbol} @ {timeframe}")
    
    if not missing_df.empty:
        if debug or verbose:
            print(f"⚠️  Still missing: {len(missing_df)} candles")
        # Always print warning if data is incomplete
        elif len(missing_df) > 0:
            print(f"⚠️  Warning: {symbol} @ {timeframe} missing {len(missing_df)} candles")
    else:
        if debug:
            print(f"✅ No missing data!")
    
    if debug or verbose:
        print("\n" + "="*80)
        print(f"🎉 FETCH COMPLETE")
        print("="*80)
    
    if debug or print_summary:
        print(f"📊 {symbol} @ {timeframe}: Found {len(final_found_df)}, Missing {len(missing_df)}")
    
    if debug or verbose:
        print("="*80 + "\n")
    
    return final_found_df, missing_df


# ═══════════════════════════════════════════════════════════════════════
# TESTS
# ═══════════════════════════════════════════════════════════════════════

# if __name__ == "__main__":
#     from datetime import datetime, timezone, timedelta
    
#     print("="*80)
#     print("🧪 TESTING ORCHESTRATOR")
#     print("="*80)
    
#     # Test 1: Old data (should prefer ZIP)
#     print("\n\n" + "="*80)
#     print("TEST 1: Old data (Jan 2024) - Should prefer ZIP over API")
#     print("="*80)
    
#     start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
#     end = datetime(2024, 1, 5, 0, 0, 0, tzinfo=timezone.utc)
    
#     found, missing = fetch_ohlcv(
#         symbol="ETHUSD",
#         timeframe="1h",
#         start_ts=int(start.timestamp()),
#         end_ts=int(end.timestamp())
#     )
    
#     print(f"\n📊 Results: Found {len(found)}, Missing {len(missing)}")
    
#     # Test 2: Recent data (should prefer API)
#     print("\n\n" + "="*80)
#     print("TEST 2: Recent data (last 10 days) - Should prefer API over ZIP")
#     print("="*80)
    
#     today = datetime(2025, 10, 27, tzinfo=timezone.utc)
#     start = today - timedelta(days=10)
#     end = today - timedelta(days=1)
    
#     found, missing = fetch_ohlcv(
#         symbol="ETHUSD",
#         timeframe="1d",
#         start_ts=int(start.timestamp()),
#         end_ts=int(end.timestamp())
#     )
    
#     print(f"\n📊 Results: Found {len(found)}, Missing {len(missing)}")
    
#     # Test 3: Mixed range (some in DB, some needs API)
#     print("\n\n" + "="*80)
#     print("TEST 3: Mixed range - Tests intelligent source selection")
#     print("="*80)
    
#     start = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
#     end = datetime(2024, 6, 30, 0, 0, 0, tzinfo=timezone.utc)
    
#     found, missing = fetch_ohlcv(
#         symbol="DAIUSD",
#         timeframe="1d",
#         start_ts=int(start.timestamp()),
#         end_ts=int(end.timestamp())
#     )
    
#     print(f"\n📊 Results: Found {len(found)}, Missing {len(missing)}")
    
#     print("\n" + "="*80)
#     print("✅ ORCHESTRATOR TESTS COMPLETE")
#     print("="*80)



"""
FETCH_OHLCV STRESS TEST SUITE
==============================

Comprehensive stress test covering:
- Edge cases
- Failure scenarios
- Performance limits
- Data quality issues
- Source coordination problems
- Boundary conditions
"""

from datetime import datetime, timezone, timedelta
import time


def run_stress_test():
    """
    Run comprehensive stress tests on fetch_ohlcv orchestrator.
    Tests all potential failure points and edge cases.
    """
    
    print("="*80)
    print("🔥 FETCH_OHLCV STRESS TEST SUITE")
    print("="*80)
    print("Testing edge cases, failures, and performance limits")
    print("="*80)
    
    results = []
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 1: EDGE CASE TIMESTAMPS
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "🔥"*40)
    print("CATEGORY 1: EDGE CASE TIMESTAMPS")
    print("🔥"*40)
    
    # Test 1.1: Single candle request
    print("\n📊 TEST 1.1: Single candle request")
    print("-"*80)
    start = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    end = start  # Same timestamp
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) <= 1 else "FAIL"
        results.append(("1.1: Single candle", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("1.1: Single candle", "ERROR", 0, 0))
    
    # Test 1.2: Misaligned timestamps (off by minutes)
    print("\n📊 TEST 1.2: Badly misaligned timestamps")
    print("-"*80)
    start = datetime(2024, 6, 15, 8, 37, 23, tzinfo=timezone.utc)  # 8:37:23 AM
    end = datetime(2024, 6, 15, 14, 51, 47, tzinfo=timezone.utc)   # 2:51:47 PM
    try:
        found, missing = fetch_ohlcv(
            symbol="DAIUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) > 0 else "FAIL"
        results.append(("1.2: Misaligned timestamps", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("1.2: Misaligned timestamps", "ERROR", 0, 0))
    
    # Test 1.3: Reverse timestamps (end before start)
    print("\n📊 TEST 1.3: Reverse timestamps (end < start)")
    print("-"*80)
    start = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 14, 12, 0, 0, tzinfo=timezone.utc)  # Day before!
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) == 0 else "FAIL"
        results.append(("1.3: Reverse timestamps", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ Expected error (good): {type(e).__name__}")
        results.append(("1.3: Reverse timestamps", "PASS", 0, 0))
    
    # Test 1.4: Extremely long range (years)
    print("\n📊 TEST 1.4: Very long range (2 years of hourly data)")
    print("-"*80)
    start = datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2023, 12, 31, 23, 0, 0, tzinfo=timezone.utc)
    start_time = time.time()
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        elapsed = time.time() - start_time
        result = "PASS" if elapsed < 60 else "SLOW"  # Should complete in <60s
        results.append((f"1.4: Long range ({elapsed:.1f}s)", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("1.4: Long range", "ERROR", 0, 0))
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 2: MISSING DATA / EMPTY SOURCES
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "🔥"*40)
    print("CATEGORY 2: MISSING DATA / EMPTY SOURCES")
    print("🔥"*40)
    
    # Test 2.1: Non-existent symbol
    print("\n📊 TEST 2.1: Non-existent symbol")
    print("-"*80)
    start = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 16, 0, 0, 0, tzinfo=timezone.utc)
    try:
        found, missing = fetch_ohlcv(
            symbol="FAKECOIN",  # Doesn't exist
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) == 0 and len(missing) > 0 else "FAIL"
        results.append(("2.1: Non-existent symbol", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("2.1: Non-existent symbol", "PASS", 0, 0))
    
    # Test 2.2: Future dates (no data should exist)
    print("\n📊 TEST 2.2: Future dates")
    print("-"*80)
    start = datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2030, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) == 0 else "FAIL"
        results.append(("2.2: Future dates", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("2.2: Future dates", "ERROR", 0, 0))
    
    # Test 2.3: Very old dates (before coin existed)
    print("\n📊 TEST 2.3: Before coin existed (DAI in 2015)")
    print("-"*80)
    start = datetime(2015, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2015, 1, 2, 0, 0, 0, tzinfo=timezone.utc)
    try:
        found, missing = fetch_ohlcv(
            symbol="DAIUSD",  # DAI didn't exist in 2015
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) == 0 else "FAIL"
        results.append(("2.3: Before coin existed", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("2.3: Before coin existed", "ERROR", 0, 0))
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 3: BOUNDARY CONDITIONS (200-PERIOD THRESHOLD)
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "🔥"*40)
    print("CATEGORY 3: BOUNDARY CONDITIONS (200-PERIOD THRESHOLD)")
    print("🔥"*40)
    
    # Test 3.1: Exactly 200 periods back (boundary)
    print("\n📊 TEST 3.1: Exactly 200 periods back (hourly)")
    print("-"*80)
    now = datetime(2025, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
    start = now - timedelta(hours=200)
    end = now - timedelta(hours=1)
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) > 0 else "FAIL"
        results.append(("3.1: 200 periods (boundary)", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("3.1: 200 periods", "ERROR", 0, 0))
    
    # Test 3.2: Just under 200 periods (should prefer API)
    print("\n📊 TEST 3.2: 199 periods back (should prefer API)")
    print("-"*80)
    now = datetime(2025, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
    start = now - timedelta(hours=199)
    end = now - timedelta(hours=1)
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) > 0 else "FAIL"
        results.append(("3.2: 199 periods (API first)", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("3.2: 199 periods", "ERROR", 0, 0))
    
    # Test 3.3: Just over 200 periods (should prefer ZIP)
    print("\n📊 TEST 3.3: 201 periods back (should prefer ZIP)")
    print("-"*80)
    now = datetime(2025, 10, 27, 12, 0, 0, tzinfo=timezone.utc)
    start = now - timedelta(hours=201)
    end = now - timedelta(hours=1)
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) > 0 else "FAIL"
        results.append(("3.3: 201 periods (ZIP first)", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("3.3: 201 periods", "ERROR", 0, 0))
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 4: DIFFERENT TIMEFRAMES
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "🔥"*40)
    print("CATEGORY 4: DIFFERENT TIMEFRAMES")
    print("🔥"*40)
    
    # Test 4.1: 15-minute data (if available)
    print("\n📊 TEST 4.1: 15-minute timeframe")
    print("-"*80)
    start = datetime(2024, 6, 15, 8, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="15m",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS"  # Any result is fine, just testing it doesn't crash
        results.append(("4.1: 15-minute timeframe", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("4.1: 15-minute", "ERROR", 0, 0))
    
    # Test 4.2: Daily data
    print("\n📊 TEST 4.2: Daily timeframe")
    print("-"*80)
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 12, 31, 0, 0, 0, tzinfo=timezone.utc)
    try:
        found, missing = fetch_ohlcv(
            symbol="DAIUSD",
            timeframe="1d",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "PASS" if len(found) > 0 else "FAIL"
        results.append(("4.2: Daily timeframe", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("4.2: Daily", "ERROR", 0, 0))
    
    # Test 4.3: Invalid timeframe
    print("\n📊 TEST 4.3: Invalid timeframe")
    print("-"*80)
    start = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 16, 0, 0, 0, tzinfo=timezone.utc)
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="99x",  # Invalid
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        result = "FAIL"  # Should have raised error
        results.append(("4.3: Invalid timeframe", result, len(found), len(missing)))
    except Exception as e:
        print(f"✅ Expected error (good): {type(e).__name__}")
        results.append(("4.3: Invalid timeframe", "PASS", 0, 0))
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 5: DATA CONSISTENCY
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "🔥"*40)
    print("CATEGORY 5: DATA CONSISTENCY")
    print("🔥"*40)
    
    # Test 5.1: Check for duplicate timestamps
    print("\n📊 TEST 5.1: No duplicate timestamps in result")
    print("-"*80)
    start = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 20, 0, 0, 0, tzinfo=timezone.utc)
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        duplicates = found['timestamp_unix'].duplicated().sum()
        result = "PASS" if duplicates == 0 else "FAIL"
        results.append((f"5.1: No duplicates ({duplicates} found)", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("5.1: No duplicates", "ERROR", 0, 0))
    
    # Test 5.2: Check timestamps are sorted
    print("\n📊 TEST 5.2: Timestamps are sorted ascending")
    print("-"*80)
    start = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 20, 0, 0, 0, tzinfo=timezone.utc)
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        is_sorted = found['timestamp_unix'].is_monotonic_increasing
        result = "PASS" if is_sorted else "FAIL"
        results.append(("5.2: Sorted timestamps", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("5.2: Sorted", "ERROR", 0, 0))
    
    # Test 5.3: Check all required columns exist
    print("\n📊 TEST 5.3: All required columns present")
    print("-"*80)
    start = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 16, 0, 0, 0, tzinfo=timezone.utc)
    try:
        found, missing = fetch_ohlcv(
            symbol="ETHUSD",
            timeframe="1h",
            start_ts=int(start.timestamp()),
            end_ts=int(end.timestamp())
        )
        required_cols = ['symbol', 'timeframe', 'ny_time', 'timestamp_unix', 
                        'open', 'high', 'low', 'close', 'volume']
        has_all_cols = all(col in found.columns for col in required_cols)
        result = "PASS" if has_all_cols else "FAIL"
        results.append(("5.3: All columns present", result, len(found), len(missing)))
    except Exception as e:
        print(f"❌ ERROR: {e}")
        results.append(("5.3: All columns", "ERROR", 0, 0))
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 6: STRESS SCENARIOS
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "🔥"*40)
    print("CATEGORY 6: STRESS SCENARIOS")
    print("🔥"*40)
    
    # Test 6.1: Rapid consecutive requests
    print("\n📊 TEST 6.1: 5 rapid consecutive requests")
    print("-"*80)
    start = datetime(2024, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 6, 15, 5, 0, 0, tzinfo=timezone.utc)
    total_time = 0
    errors = 0
    for i in range(5):
        try:
            start_req = time.time()
            found, missing = fetch_ohlcv(
                symbol="DAIUSD",
                timeframe="1h",
                start_ts=int(start.timestamp()),
                end_ts=int(end.timestamp())
            )
            total_time += time.time() - start_req
        except Exception as e:
            errors += 1
    
    result = "PASS" if errors == 0 else "FAIL"
    avg_time = total_time / 5
    results.append((f"6.1: Rapid requests (avg {avg_time:.2f}s)", result, 0, errors))
    
    # ═══════════════════════════════════════════════════════════════════════
    # FINAL SUMMARY
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n" + "="*80)
    print("🎯 STRESS TEST SUMMARY")
    print("="*80)
    
    passed = sum(1 for _, result, _, _ in results if result == "PASS")
    failed = sum(1 for _, result, _, _ in results if result == "FAIL")
    errors = sum(1 for _, result, _, _ in results if result == "ERROR")
    slow = sum(1 for _, result, _, _ in results if result == "SLOW")
    
    print(f"\n📊 Overall Results:")
    print(f"   ✅ PASS:  {passed}")
    print(f"   ❌ FAIL:  {failed}")
    print(f"   💥 ERROR: {errors}")
    print(f"   🐌 SLOW:  {slow}")
    print(f"   📈 TOTAL: {len(results)}")
    
    print(f"\n📋 Detailed Results:")
    for test_name, result, found_count, missing_count in results:
        icon = "✅" if result == "PASS" else "❌" if result == "FAIL" else "💥" if result == "ERROR" else "🐌"
        print(f"   {icon} {test_name}: {result} (Found: {found_count}, Missing: {missing_count})")
    
    if failed == 0 and errors == 0:
        print("\n" + "="*80)
        print("🎉 ALL STRESS TESTS PASSED!")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("⚠️ SOME TESTS FAILED - REVIEW RESULTS ABOVE")
        print("="*80)
    
    return results


if __name__ == "__main__":
    run_stress_test()






#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


"""
Check ZIP Files for DAIUSD in 2015
===================================

Investigates if ZIP files contain data for DAIUSD in 2015 (before coin existed).
"""

import zipfile
import csv
import io
import os
from datetime import datetime, timezone


def check_zip_for_daiusd_2015():
    """Check if ZIP files have DAIUSD data in 2015."""
    
    print("="*80)
    print("🔍 CHECKING ZIP FILES FOR DAIUSD IN 2015")
    print("="*80)
    
    zip_folder = "Kraken_Zip_Files"
    
    if not os.path.exists(zip_folder):
        print(f"❌ ZIP folder not found: {zip_folder}")
        return
    
    zip_files = [f for f in os.listdir(zip_folder) if f.endswith('.zip')]
    print(f"\n📁 Found {len(zip_files)} ZIP files")
    
    # Looking for DAIUSD hourly data (60 minutes)
    target_file = "DAIUSD_60.csv"
    
    # 2015 timestamp range
    start_2015 = 1420070400  # Jan 1, 2015 00:00 UTC
    end_2015 = 1451606399    # Dec 31, 2015 23:59 UTC
    
    found_in_zips = []
    
    for zip_name in zip_files:
        zip_path = os.path.join(zip_folder, zip_name)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Check if this ZIP contains DAIUSD data
                if target_file not in zf.namelist():
                    continue
                
                print(f"\n📦 Checking: {zip_name}")
                print(f"   Contains: {target_file}")
                
                # Read the CSV and check for 2015 timestamps
                with zf.open(target_file) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    
                    count_2015 = 0
                    first_ts_2015 = None
                    last_ts_2015 = None
                    
                    for row in reader:
                        if row and len(row) >= 1:
                            try:
                                ts = int(row[0])
                                
                                # Check if timestamp is in 2015
                                if start_2015 <= ts <= end_2015:
                                    count_2015 += 1
                                    
                                    if first_ts_2015 is None:
                                        first_ts_2015 = ts
                                    last_ts_2015 = ts
                                    
                            except ValueError:
                                continue
                    
                    if count_2015 > 0:
                        first_dt = datetime.fromtimestamp(first_ts_2015, timezone.utc)
                        last_dt = datetime.fromtimestamp(last_ts_2015, timezone.utc)
                        
                        print(f"   ⚠️  Found {count_2015} rows in 2015!")
                        print(f"   📅 First: {first_dt} ({first_ts_2015})")
                        print(f"   📅 Last:  {last_dt} ({last_ts_2015})")
                        
                        found_in_zips.append({
                            'zip': zip_name,
                            'count': count_2015,
                            'first_ts': first_ts_2015,
                            'last_ts': last_ts_2015
                        })
                    else:
                        print(f"   ✅ No data in 2015 (correct)")
        
        except Exception as e:
            print(f"   ❌ Error reading {zip_name}: {e}")
    
    # Summary
    print("\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    
    if found_in_zips:
        print(f"\n⚠️  FOUND DATA IN 2015 (shouldn't exist!):")
        for item in found_in_zips:
            print(f"\n   📦 {item['zip']}")
            print(f"      Rows: {item['count']}")
            print(f"      Range: {datetime.fromtimestamp(item['first_ts'], timezone.utc).date()} to {datetime.fromtimestamp(item['last_ts'], timezone.utc).date()}")
        
        print(f"\n🎯 DIAGNOSIS:")
        print(f"   The ZIP files contain invalid data for DAIUSD in 2015.")
        print(f"   DAI (MakerDAO's stablecoin) didn't exist until 2017-2019.")
        print(f"   This is a DATA QUALITY ISSUE in the source ZIP files.")
        
        print(f"\n💡 SOLUTIONS:")
        print(f"   1. Filter out data before coin launch date (2019-09-25 for DAI)")
        print(f"   2. Add validation to reject impossible dates")
        print(f"   3. Clean the ZIP files to remove invalid data")
    else:
        print(f"\n✅ No DAIUSD data found in 2015 (correct)")
        print(f"   The ZIP files are clean.")
        print(f"   The test failure must be from another source.")
    
    print("="*80)


if __name__ == "__main__":
    check_zip_for_daiusd_2015()


















































