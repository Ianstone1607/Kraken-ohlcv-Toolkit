
# 🌐 GLOBAL CONFIGURATION + UTILITIES
# 🕒 Date & Time
from datetime import datetime, timedelta

# 📦 Standard Library
import os
import io
import csv
import time
import tempfile
import zipfile
from math import ceil
from collections import defaultdict
from pprint import pprint

# 🧠 Typing
from typing import Any, Dict, List, Optional, Tuple, Union

# 📊 Data & Timezones
import pandas as pd
import pytz

# 🌐 External Libraries
import ccxt
import psycopg2




def fetch_zip_kraken(config: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    from datetime import datetime
    import pytz
    import pandas as pd
    import zipfile
    import os

    symbol           = config["symbol"]
    interval_minutes = config["interval_minutes"]
    start_ts         = config["start_ts"]
    end_ts           = config["end_ts"]
    zip_path         = config["zip_path"]  # 👈 must be provided in config

    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")

    # ─── Generate expected timestamps ────────────────────────────────────────
    expected_ts = list(range(start_ts, end_ts + 1, interval_minutes * 60))

    tz_NY = pytz.timezone("America/New_York")
    fname = f"{symbol}_{interval_minutes}.csv"

    try:
        with zipfile.ZipFile(zip_path) as zf, zf.open(fname) as f:
            zip_df = pd.read_csv(
                f,
                header=None,
                names=["ts", "open", "high", "low", "close", "volume", "trades"]
            )
    except (KeyError, FileNotFoundError):
        # File not found in this ZIP → all timestamps are missing
        missing_df = pd.DataFrame({
            "timestamp_unix": expected_ts,
            "ny_time": [
                datetime.fromtimestamp(ts, pytz.utc)
                        .astimezone(tz_NY)
                        .strftime("%m/%d/%Y %I:%M %p")
                for ts in expected_ts
            ]
        })
        found_df = pd.DataFrame(columns=[
            "ny_time", "timestamp_unix", "open", "high", "low", "close", "volume"
        ])
        return found_df, missing_df

    # Filter to requested range
    zip_df = zip_df[(zip_df.ts >= start_ts) & (zip_df.ts <= end_ts)]

    # Keep only rows matching expected timestamps
    found_df = zip_df[zip_df.ts.isin(expected_ts)].copy()
    found_df["timestamp_unix"] = found_df["ts"]
    found_df["ny_time"] = found_df["timestamp_unix"].apply(
        lambda ts: datetime.fromtimestamp(ts, pytz.utc)
                            .astimezone(tz_NY)
                            .strftime("%m/%d/%Y %I:%M %p")
    )

    found_df = (
        found_df.loc[:, ["ny_time", "timestamp_unix", "open", "high", "low", "close", "volume"]]
        .drop_duplicates(subset=["timestamp_unix"])
        .sort_values("timestamp_unix")
        .reset_index(drop=True)
    )

    # Determine missing timestamps
    missing_ts = list(set(expected_ts) - set(found_df["timestamp_unix"]))
    missing_df = pd.DataFrame({
        "timestamp_unix": missing_ts,
        "ny_time": [
            datetime.fromtimestamp(ts, pytz.utc)
                    .astimezone(tz_NY)
                    .strftime("%m/%d/%Y %I:%M %p")
            for ts in missing_ts
        ]
    })

    return found_df, missing_df


def create_db_structure_tsDB(
    db_name: str,
    config: Dict[str, Any],
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "mysecretpassword"
):
    import psycopg2

    symbol    = config["symbol"]
    timeframe = config["timeframe"]

    # Create database if needed
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

    # Connect to target DB
    conn = psycopg2.connect(
        host=host, port=port, dbname=db_name, user=user, password=password
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Prep schema and table
    tbl = f"{timeframe.replace('m','min').replace('h','hour')}_ohlcv"
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{symbol}" AUTHORIZATION {user};')

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

    cur.execute(f'''
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1
            FROM timescaledb_information.hypertables
           WHERE hypertable_schema = '{symbol}'
             AND hypertable_name   = '{tbl}'
        ) THEN
          PERFORM create_hypertable('"{symbol}"."{tbl}"', 'timestamp_unix', if_not_exists => TRUE);
        END IF;
      END;
      $$;
    ''')

    cur.close()
    conn.close()




#_______________________________________________________________________________________________________________________________________________________________________________________________________________________________
#-----------------------------
def ohlcv_storage_DB(
    found_df: pd.DataFrame,
    db_name: str,
    config: Dict[str, Any],
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "mysecretpassword",
    batch_size: int = 1000
):
    import psycopg2
    from psycopg2.extras import execute_values

    if found_df.empty:
        print("📭 No rows to insert — found_df is empty.")
        return

    # ❌ Removed: create_db_structure_tsDB(...) 
    # Structure is now ensured once upfront by ensure_db_structure()

    # 🔗 Connect to target database
    conn = psycopg2.connect(
        host=host, port=port, dbname=db_name, user=user, password=password
    )
    conn.autocommit = False
    cur = conn.cursor()

    symbol    = config["symbol"]
    timeframe = config["timeframe"]
    tbl       = f"{timeframe.replace('m','min').replace('h','hour')}_ohlcv"

    insert_query = f'''
        INSERT INTO "{symbol}"."{tbl}" (
            date, time, timestamp_unix,
            open, high, low, close, volume
        )
        VALUES %s
        ON CONFLICT (timestamp_unix) DO NOTHING;
    '''

    # 🧱 Prepare rows as list of tuples
    rows = []
    for _, row in found_df.iterrows():
        ny_time = row["ny_time"]
        date_str, time_str = ny_time.split(" ", 1)
        rows.append((
            date_str, time_str, int(row["timestamp_unix"]),
            float(row["open"]), float(row["high"]),
            float(row["low"]), float(row["close"]),
            float(row["volume"])
        ))

    # 🚀 Batch insert
    rows_inserted = 0
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            execute_values(cur, insert_query, batch)
            rows_inserted += len(batch)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"❌ Insert failed: {e}")
    finally:
        cur.close()
        conn.close()

    print(f"📦 Inserted {rows_inserted} row(s) into '{symbol}.{tbl}'")


#Testing ^ for: ohlcv_storage_DB
"""
# Replace with actual import if needed
# from your_module import ohlcv_storage_DB, create_db_structure_tsDB

def test_ohlcv_storage_DB():
    # ─── Setup: Create Dummy DataFrame ──────────────────────────────────────
    rows = 5000
    start_ts = 1609459200  # Jan 1, 2021
    interval = 60  # 1-minute candles

    data = []
    for i in range(rows):
        ts = start_ts + i * interval
        ny_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))
        data.append({
            "ny_time": ny_time,
            "timestamp_unix": ts,
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.05,
            "volume": 100.0
        })
    df = pd.DataFrame(data)

    # ─── Config and DB Setup ────────────────────────────────────────────────
    db_name = "test_ohlcv_db"
    config = {
        "symbol": "TESTUSD",
        "timeframe": "60m"
    }

    # ─── Run Insert Function ────────────────────────────────────────────────
    print("🚀 Running ohlcv_storage_DB with batch insert...")
    start_time = time.time()
    ohlcv_storage_DB(
        found_df=df,
        db_name=db_name,
        config=config,
        host="localhost",
        port=5432,
        user="postgres",
        password="mysecretpassword",
        batch_size=1000
    )
    duration = time.time() - start_time
    print(f"✅ Insert completed in {duration:.2f} seconds")

    # ─── Validation: Check Row Count ────────────────────────────────────────
    tbl = f"{config['timeframe'].replace('m','min').replace('h','hour')}_ohlcv"
    conn = psycopg2.connect(
        host="localhost", port=5432, dbname=db_name,
        user="postgres", password="mysecretpassword"
    )
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM "{config["symbol"]}"."{tbl}";')
    count = cur.fetchone()[0]
    cur.close()
    conn.close()

    assert count == rows, f"❌ Row count mismatch: expected {rows}, got {count}"
    print(f"📊 Verified {count} row(s) inserted correctly.")

# Run the test
if __name__ == "__main__":
    test_ohlcv_storage_DB()

"""
#-----------------------------
#_______________________________________________________________________________________________________________________________________________________________________________________________________________________________




def get_usd_crypto_symbols_from_zip(zip_path) -> List[str]:
    import zipfile
    import os

    # ─── Step 1: Validate ZIP Path ───────────────────────────────────────────
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"ZIP file not found at: {zip_path}")

    # ─── Step 2: Scan ZIP for USD crypto symbols ─────────────────────────────
    with zipfile.ZipFile(zip_path, "r") as zf:
        filenames = zf.namelist()

    symbols = set()
    for fname in filenames:
        if not fname.endswith(".csv"):
            continue
        try:
            symbol, _ = fname.replace(".csv", "").split("_", 1)
        except ValueError:
            continue
        if symbol.upper().endswith("USD"):
            symbols.add(symbol.upper())

    # ─── Step 3: Return Sorted List ──────────────────────────────────────────
    return sorted(symbols)

#_______________________________________________________________________________________________________________________________________________________________________________________________________________________________
#-----------------------------
def build_zip_inventory(
    zip_paths: List[str],
    timeframes: List[str],
    symbols: Optional[List[str]] = None,
    symbol_suffix: Optional[str] = None
) -> Dict[str, Dict[str, Dict[str, Tuple[int, int, int]]]]:
    """
    Scans each ZIP once and builds complete inventory.

    Parameters
    ----------
    zip_paths : List[str]
        Paths to ZIP archives
    timeframes : List[str]
        Timeframes to include (e.g. ["1","60","1440"])
    symbols : Optional[List[str]]
        If provided, only include these symbols
    symbol_suffix : Optional[str]
        If provided, only include symbols ending with this suffix (e.g. "USD")

    Returns
    -------
    inventory : dict
        {
            "BTCUSD": {
                "60": {
                    "C:/full/path/to/Kraken_OHLCVT.zip": (start_ts, end_ts, row_count)
                }
            }
        }
    """
    inventory = defaultdict(lambda: defaultdict(dict))

    for zip_path in zip_paths:
        abs_path = os.path.abspath(zip_path)
        if not os.path.exists(abs_path):
            print(f"⚠️ ZIP file not found: {abs_path}")
            continue

        try:
            with zipfile.ZipFile(abs_path, "r") as zf:
                for fname in zf.namelist():
                    if not fname.endswith(".csv"):
                        continue

                    parts = fname.replace(".csv", "").split("_")
                    if len(parts) != 2:
                        continue  # Unexpected filename format

                    symbol, tf = parts

                    # Apply filters
                    if (symbols and symbol not in symbols):
                        continue
                    if symbol_suffix and not symbol.endswith(symbol_suffix):
                        continue
                    if tf not in timeframes:
                        continue

                    try:
                        with zf.open(fname) as f:
                            reader = csv.reader(io.TextIOWrapper(f))
                            start_ts = float("inf")
                            end_ts = float("-inf")
                            row_count = 0

                            for row in reader:
                                if not row or len(row) < 1:
                                    continue
                                try:
                                    ts = int(row[0])
                                    start_ts = min(start_ts, ts)
                                    end_ts = max(end_ts, ts)
                                    row_count += 1
                                except ValueError:
                                    continue  # Skip bad timestamp rows

                            if row_count > 0:
                                # ✅ Store absolute path
                                inventory[symbol][tf][abs_path] = (
                                    start_ts,
                                    end_ts,
                                    row_count
                                )
                    except Exception as e:
                        print(f"⚠️ Error reading {fname} in {abs_path}: {e}")
                        continue
        except Exception as e:
            print(f"⚠️ Failed to open ZIP {abs_path}: {e}")
            continue

    # Convert defaultdicts to normal dicts for return
    return {s: dict(tf_map) for s, tf_map in inventory.items()}

# Testing ^ For: build_zip_inventory
"""

def test_build_zip_inventory():
    # ─── Setup: Create Temporary ZIP with Dummy CSVs ─────────────────────────
    temp_dir = tempfile.mkdtemp()
    zip_name = "Kraken_OHLCVT.zip"
    zip_path = os.path.join(temp_dir, zip_name)

    symbols = ["BTCUSD", "ETHUSD"]
    timeframes = ["1", "60"]
    rows_per_file = 1000
    start_ts = 1609459200  # Jan 1, 2021
    interval = 60  # 1-minute candles

    with zipfile.ZipFile(zip_path, "w") as zf:
        for symbol in symbols:
            for tf in timeframes:
                fname = f"{symbol}_{tf}.csv"
                data = []
                for i in range(rows_per_file):
                    ts = start_ts + i * interval
                    row = [ts, 1.0, 1.1, 0.9, 1.05, 100.0, 10]
                    data.append(row)
                # Write to temp CSV
                with tempfile.NamedTemporaryFile("w", delete=False, newline="") as tmp_csv:
                    writer = csv.writer(tmp_csv)
                    writer.writerows(data)
                    tmp_csv_path = tmp_csv.name
                zf.write(tmp_csv_path, arcname=fname)
                os.remove(tmp_csv_path)

    # ─── Benchmark ──────────────────────────────────────────────────────────
    print("🔍 Running build_zip_inventory...")
    start_time = time.time()
    result = build_zip_inventory([zip_path], timeframes, symbols)
    duration = time.time() - start_time
    print(f"✅ Completed in {duration:.4f} seconds")

    # ─── Validation ─────────────────────────────────────────────────────────
    assert isinstance(result, dict), "Result should be a dictionary"
    for symbol in symbols:
        assert symbol in result, f"Missing symbol: {symbol}"
        for tf in timeframes:
            assert tf in result[symbol], f"Missing timeframe: {tf}"
            assert zip_name in result[symbol][tf], f"Missing ZIP entry: {zip_name}"
            entry = result[symbol][tf][zip_name]
            assert isinstance(entry, tuple) and len(entry) == 3, "Entry should be (start_ts, end_ts, row_count)"
            assert entry[2] == rows_per_file, f"Row count mismatch for {symbol}_{tf}"

    # ─── Output ─────────────────────────────────────────────────────────────
    print("\n📦 Inventory Output:")
    pprint(result)

    # ─── Cleanup ────────────────────────────────────────────────────────────
    os.remove(zip_path)
    os.rmdir(temp_dir)

# Run the test
if __name__ == "__main__":
    test_build_zip_inventory()

"""
#-----------------------------
#_______________________________________________________________________________________________________________________________________________________________________________________________________________________________



def format_zip_fetch_configs(
    symbol_ranges: Dict[str, Dict[str, Tuple[int, int]]]
) -> List[Dict[str, Any]]:
    configs = []

    for symbol, tf_dict in symbol_ranges.items():
        for tf_str, (start_ts, end_ts) in tf_dict.items():
            config = {
                "symbol": symbol,
                "interval_minutes": int(tf_str),
                "timeframe": tf_str,  # 👈 Added to support DB structure creation
                "start_ts": start_ts,
                "end_ts": end_ts
            }
            configs.append(config)

    return configs




#_______________________________________________________________________________________________________________________________________________________________________________________________________________________________
#-----------------------------
def extract_xbt_latest_per_zip(
    zip_paths: List[str],
    timeframes: List[str] = ["1", "5", "15", "30", "60", "240", "720", "1440"],
    output_csv: str = "xbt_latest_timestamps.csv"
) -> pd.DataFrame:

    # Ensure output folder exists
    output_folder = "Kraken_Zip_Files"
    os.makedirs(output_folder, exist_ok=True)
    output_csv_path = os.path.join(output_folder, output_csv)

    # Build inventory for XBTUSD only
    inventory = build_zip_inventory(zip_paths, timeframes, symbols=["XBTUSD"])

    rows = []
    for zip_path in zip_paths:
        zip_name = os.path.basename(zip_path)
        latest_ts_by_tf = {tf: None for tf in timeframes}

        for tf in timeframes:
            try:
                latest_ts_by_tf[tf] = inventory["XBTUSD"][tf][zip_name][1]  # end_ts
            except KeyError:
                continue  # Missing file or timeframe in this ZIP

        row = {
            "zip_path": zip_path,
            "zip_name": zip_name,
            **latest_ts_by_tf
        }
        rows.append(row)

    df_out = pd.DataFrame(rows)
    df_out.to_csv(output_csv_path, index=False)
    print(f"📝 Saved XBT latest timestamps per timeframe to {output_csv_path}")
    return df_out

# Testing ^ for: extract_xbt_latest_per_zip
"""
import time
import tempfile
import zipfile
import os
import csv
import pandas as pd
from pprint import pprint

# Replace with actual import if needed
# from your_module import extract_xbt_latest_per_zip

def test_extract_xbt_latest_per_zip():
    # ─── Setup: Create Temporary ZIP with Dummy XBTUSD CSVs ─────────────────
    temp_dir = tempfile.mkdtemp()
    zip_name = "Kraken_OHLCVT.zip"
    zip_path = os.path.join(temp_dir, zip_name)

    timeframes = ["1", "60"]
    rows_per_file = 1000
    start_ts = 1609459200  # Jan 1, 2021
    interval = 60  # 1-minute candles

    with zipfile.ZipFile(zip_path, "w") as zf:
        for tf in timeframes:
            fname = f"XBTUSD_{tf}.csv"
            data = []
            for i in range(rows_per_file):
                ts = start_ts + i * interval
                row = [ts, 1.0, 1.1, 0.9, 1.05, 100.0, 10]
                data.append(row)
            # Write to temp CSV
            with tempfile.NamedTemporaryFile("w", delete=False, newline="") as tmp_csv:
                writer = csv.writer(tmp_csv)
                writer.writerows(data)
                tmp_csv_path = tmp_csv.name
            zf.write(tmp_csv_path, arcname=fname)
            os.remove(tmp_csv_path)

    # ─── Benchmark ──────────────────────────────────────────────────────────
    print("🔍 Running extract_xbt_latest_per_zip...")
    start_time = time.time()
    df_out = extract_xbt_latest_per_zip([zip_path], timeframes)
    duration = time.time() - start_time
    print(f"✅ Completed in {duration:.4f} seconds")

    # ─── Validation ─────────────────────────────────────────────────────────
    assert isinstance(df_out, pd.DataFrame), "Output should be a DataFrame"
    assert not df_out.empty, "Output DataFrame should not be empty"
    assert "zip_name" in df_out.columns, "Missing 'zip_name' column"
    for tf in timeframes:
        assert tf in df_out.columns, f"Missing timeframe column: {tf}"
        latest_ts = df_out.iloc[0][tf]
        expected_ts = start_ts + (rows_per_file - 1) * interval
        assert latest_ts == expected_ts, f"Timestamp mismatch for tf={tf}: got {latest_ts}, expected {expected_ts}"

    # ─── Output ─────────────────────────────────────────────────────────────
    print("\n📦 Latest Timestamps Output:")
    pprint(df_out.to_dict(orient="records"))

    # ─── Cleanup ────────────────────────────────────────────────────────────
    os.remove(zip_path)
    os.rmdir(temp_dir)

# Run the test
if __name__ == "__main__":
    test_extract_xbt_latest_per_zip()
"""
#------------------------------
#_______________________________________________________________________________________________________________________________________________________________________________________________________________________________




#_______________________________________________________________________________________________________________________________________________________________________________________________________________________________
#-----------------------------
def select_optimal_zips(
    inventory: Dict[str, Dict[str, Dict[str, tuple]]],
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    symbol_suffix: Optional[str] = None   # ✅ optional filter
) -> List[Dict[str, object]]:
    """
    Selects ZIPs per symbol/timeframe that cover the requested range.

    Parameters
    ----------
    inventory : dict
        Output from build_zip_inventory
    start_ts : Optional[int]
        Start of desired range (Unix timestamp)
    end_ts : Optional[int]
        End of desired range (Unix timestamp)
    symbol_suffix : Optional[str]
        If provided, only include symbols ending with this suffix (e.g. "USD")

    Returns
    -------
    List[dict]
        [
            {
                "symbol": "ETHUSD",
                "timeframe": "60",
                "zip_path": "C:/full/path/to/Kraken_OHLCVT.zip",
                "start_ts": ...,
                "end_ts": ...
            },
            ...
        ]
    """
    selected = []

    for symbol, tf_map in inventory.items():
        # ✅ Apply suffix filter if requested
        if symbol_suffix and not symbol.endswith(symbol_suffix):
            continue

        for tf, zip_entries in tf_map.items():
            # Sort ZIPs by start_ts
            sorted_zips = sorted(zip_entries.items(), key=lambda x: x[1][0])

            for zip_path, (zip_start, zip_end, _) in sorted_zips:
                abs_path = os.path.abspath(zip_path)

                if end_ts is not None and zip_start > end_ts:
                    break
                if start_ts is not None and zip_end < start_ts:
                    continue

                clipped_start = max(zip_start, start_ts) if start_ts is not None else zip_start
                clipped_end = min(zip_end, end_ts) if end_ts is not None else zip_end

                selected.append({
                    "symbol": symbol,
                    "timeframe": tf,
                    "zip_path": abs_path,   # ✅ always absolute
                    "start_ts": clipped_start,
                    "end_ts": clipped_end
                })

                # Stop once we’ve covered the requested end
                if end_ts is not None and zip_end >= end_ts:
                    break

    return selected

#Testing ^ for: select_optimal_zips
"""
def test_select_optimal_zips():


    # ─── Mock Inventory ─────────────────────────────────────────────────────
    inventory = {
        "BTCUSD": {
            "60": {
                "zip_A.zip": (1609459200, 1609462800, 100),  # Jan 1, 2021 00:00–01:00
                "zip_B.zip": (1609462800, 1609466400, 100),  # Jan 1, 2021 01:00–02:00
            }
        },
        "ETHUSD": {
            "60": {
                "zip_B.zip": (1609462800, 1609466400, 100),
                "zip_C.zip": (1609466400, 1609470000, 100),  # Jan 1, 2021 02:00–03:00
            }
        },
        "SOLUSD": {
            "60": {
                "zip_C.zip": (1609466400, 1609470000, 100),
                "zip_D.zip": (1609470000, 1609473600, 100),  # Jan 1, 2021 03:00–04:00
            }
        }
    }

    # ─── Test Range ─────────────────────────────────────────────────────────
    start_ts = 1609461000  # Jan 1, 2021 00:30
    end_ts   = 1609470000  # Jan 1, 2021 03:00

    # ─── Run Function ───────────────────────────────────────────────────────
    print("🔍 Running select_optimal_zips...")
    result = select_optimal_zips(inventory, start_ts, end_ts)

    # ─── Validation ─────────────────────────────────────────────────────────
    assert isinstance(result, list), "Result should be a list"
    assert all(isinstance(entry, dict) for entry in result), "Each entry should be a dict"

    # Group results for easier assertions
    grouped = {}
    for entry in result:
        key = (entry["symbol"], entry["timeframe"])
        grouped.setdefault(key, []).append(entry)

    # BTCUSD should return zip_A.zip and zip_B.zip (both overlap with range)
    btc_zips = [e["zip_path"] for e in grouped.get(("BTCUSD", "60"), [])]
    assert btc_zips == ["zip_A.zip", "zip_B.zip"], f"BTCUSD mismatch: {btc_zips}"

    # ETHUSD should return zip_B.zip and zip_C.zip
    eth_zips = [e["zip_path"] for e in grouped.get(("ETHUSD", "60"), [])]
    assert eth_zips == ["zip_B.zip", "zip_C.zip"], f"ETHUSD mismatch: {eth_zips}"

    # SOLUSD should return zip_C.zip only (zip_D starts after end_ts)
    sol_zips = [e["zip_path"] for e in grouped.get(("SOLUSD", "60"), [])]
    assert sol_zips == ["zip_C.zip"], f"SOLUSD mismatch: {sol_zips}"

    # ─── Output ─────────────────────────────────────────────────────────────
    from pprint import pprint
    print("\n📦 Selected ZIPs:")
    pprint(result)

# Run the test
if __name__ == "__main__":
    test_select_optimal_zips()

"""
#------------------------------
#_______________________________________________________________________________________________________________________________________________________________________________________________________________________________




def ensure_db_structure(db_name, symbols, timeframes, host="localhost", port=5432, user="postgres", password="mysecretpassword"):
    """
    Ensure that the database, schemas, tables, and hypertables exist
    for all given symbols and timeframes. Called once before ingestion.
    """
    # Step 1: Connect to default DB and create target DB if missing
    conn = psycopg2.connect(dbname="postgres", host=host, port=port, user=user, password=password)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{db_name}"')
    cur.close()
    conn.close()

    # Step 2: Connect to target DB
    conn = psycopg2.connect(dbname=db_name, host=host, port=port, user=user, password=password)
    cur = conn.cursor()

    # Step 3: For each symbol/timeframe, ensure schema, table, hypertable
    for symbol in symbols:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{symbol}"')
        for tf in timeframes:
            tbl = tf.replace("m","min").replace("h","hour") + "_ohlcv"
            cur.execute(f'''
                CREATE TABLE IF NOT EXISTS "{symbol}"."{tbl}" (
                    date TEXT,
                    time TEXT,
                    timestamp_unix BIGINT PRIMARY KEY,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION
                );
            ''')
            # Try to promote to hypertable (ignore if already done)
            try:
                cur.execute(f'''
                    SELECT create_hypertable('"{symbol}"."{tbl}"', 'timestamp_unix', if_not_exists => TRUE);
                ''')
            except Exception:
                pass

    conn.commit()
    cur.close()
    conn.close()








def ingest_ohlcv_from_zip(
    db_name: str,
    timeframes: List[str],
    zip_paths: List[str],
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "mysecretpassword",
    symbols: Optional[List[str]] = None,
    exclude_symbols: Optional[List[str]] = None,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    symbol_suffix: Optional[str] = None
):
    """
    Optimized ZIP ingestion:
    - Absolute path normalization
    - Early ZIP existence verification
    - Optional suffix filter (e.g. only ingest symbols ending with 'USD')
    """

    # Step 0: Normalize and validate ZIP paths
    zip_paths = [os.path.abspath(p) for p in zip_paths]
    valid_zip_paths = [zp for zp in zip_paths if os.path.exists(zp)]
    missing_zips = [zp for zp in zip_paths if not os.path.exists(zp)]

    if missing_zips:
        print(f"⚠️ Missing ZIPs: {len(missing_zips)}")
        for zp in missing_zips:
            print(f"   - {os.path.basename(zp)}")

    if not valid_zip_paths:
        print("❌ No valid ZIP archives found — aborting.")
        return

    # Step 1: Build inventory
    print(f"\n🔍 Scanning {len(valid_zip_paths)} ZIP(s) for symbols...")
    inventory = build_zip_inventory(
        zip_paths=valid_zip_paths,
        timeframes=timeframes,
        symbols=symbols,
        symbol_suffix=symbol_suffix
    )
    if not inventory:
        print("❌ No data found in ZIPs.")
        return

    print(f"✅ Inventory: {len(inventory)} symbols, "
          f"{sum(len(tf_map) for tf_map in inventory.values())} symbol/timeframe pairs")

    # Step 2: Apply exclude filter
    if exclude_symbols:
        for symbol in exclude_symbols:
            inventory.pop(symbol, None)
        print(f"🚫 Excluded {len(exclude_symbols)} symbols")

    if not inventory:
        print("❌ No symbols remaining after filters.")
        return

    # Step 3: Select configs
    configs = select_optimal_zips(
        inventory=inventory,
        start_ts=start_ts,
        end_ts=end_ts,
        symbol_suffix=symbol_suffix
    )
    if not configs:
        print("❌ No configs generated (check filters or date range).")
        return

    print(f"📋 Tasks generated: {len(configs)}")
    print("   Example configs:")
    pprint(configs[:3], width=100, compact=True)

    # Step 4: Ensure DB structure
    unique_symbols = {c["symbol"] for c in configs}
    unique_timeframes = {c["timeframe"] for c in configs}
    print(f"\n🔧 Preparing DB for {len(unique_symbols)} symbols × {len(unique_timeframes)} timeframes...")
    ensure_db_structure(
        db_name=db_name,
        symbols=unique_symbols,
        timeframes=unique_timeframes,
        host=host,
        port=port,
        user=user,
        password=password
    )

    # Step 5: Extract and insert
    total_rows, successful, failed = 0, 0, 0

    def _probe_zip(zp: str) -> bool:
        try:
            with zipfile.ZipFile(zp, "r") as zf:
                return True
        except Exception:
            return False

    for i, config in enumerate(configs, 1):
        symbol, tf, zp = config["symbol"], config["timeframe"], os.path.abspath(config["zip_path"])
        config["zip_path"] = zp
        print(f"[{i}/{len(configs)}] {symbol} @ {tf}min ({os.path.basename(zp)})")

        if not os.path.exists(zp) or not _probe_zip(zp):
            print("   ❌ ZIP not accessible")
            failed += 1
            continue

        try:
            config["interval_minutes"] = int(tf)
            found_df, _ = fetch_zip_kraken(config)

            if found_df is None or found_df.empty:
                print("   ⚠️ No data")
                continue

            ohlcv_storage_DB(
                found_df=found_df,
                db_name=db_name,
                config=config,
                host=host,
                port=port,
                user=user,
                password=password
            )
            print(f"   ✅ Inserted {len(found_df)} rows")
            total_rows += len(found_df)
            successful += 1

        except Exception as e:
            print(f"   ❌ Error: {e}")
            failed += 1

    # Step 6: Summary
    print("\n" + "="*50)
    print("🎉 Ingestion Complete")
    print("="*50)
    print(f"✅ Successful: {successful}/{len(configs)}")
    print(f"❌ Failed: {failed}/{len(configs)}")
    print(f"📊 Rows inserted: {total_rows:,}")
    print(f"💾 Database: {db_name}")
    print("="*50 + "\n")

# ─── 1. ZIP archives to ingest ──────────────────────────────────────────────
zip_paths = [
    "Kraken_Zip_Files/Kraken_OHLCVT.zip",          # Main archive
    "Kraken_Zip_Files/Kraken_OHLCVT_Q1_2025.zip",  # Q1 2025 archive
    "Kraken_Zip_Files/Kraken_OHLCVT_Q2_2025.zip"   # Q2 2025 archive
]

# ─── 2. Timeframes (in minutes) ─────────────────────────────────────────────
# Examples: "1" = 1‑minute, "60" = 1‑hour, "1440" = 1‑day
timeframes = ["60", "1440"]   # Ingest hourly and daily candles

# ─── 3. Symbols to include/exclude ──────────────────────────────────────────
symbols         = None          # If None, auto‑discover all symbols
exclude_symbols = None          # Or provide a list, e.g. ["DOGEUSD"]
symbol_suffix   = "USD"         # ✅ Only ingest symbols ending with "USD"

# ─── 4. Date range (optional) ───────────────────────────────────────────────
# Leave as None to ingest all available data
start_ts = None  # e.g. 1722470400  # 2024‑08‑01 00:00:00 UTC
end_ts   = None  # e.g. 1732924800  # 2024‑11‑30 00:00:00 UTC

# ─── 5. Database connection ─────────────────────────────────────────────────
db_config = {
    "db_name": "ohlcv_data",
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "mysecretpassword"
}

# ─── 6. Run ingestion ───────────────────────────────────────────────────────
ingest_ohlcv_from_zip(
    db_name=db_config["db_name"],
    timeframes=timeframes,
    zip_paths=zip_paths,
    symbols=symbols,
    exclude_symbols=exclude_symbols,
    symbol_suffix=symbol_suffix,   # ✅ new parameter
    start_ts=start_ts,
    end_ts=end_ts,
    host=db_config["host"],
    port=db_config["port"],
    user=db_config["user"],
    password=db_config["password"]
)












#Check a table in the DB
def query_ohlcv_table(
    db_name: str,
    timeframe: str,
    symbol: str,
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "mysecretpassword"
):
    import psycopg2
    import pandas as pd

    tbl = f"{timeframe.replace('m','min').replace('h','hour')}_ohlcv"

    conn = psycopg2.connect(
        host=host, port=port, dbname=db_name, user=user, password=password
    )
    query = f'SELECT * FROM "{symbol}"."{tbl}" ORDER BY timestamp_unix ASC;'

    df = pd.read_sql_query(query, conn)
    conn.close()

    print(f"\n📊 Data for {symbol} @ {timeframe}min in '{db_name}':")
    print(df)


# query_ohlcv_table(
#     db_name="ohlcv_test",
#     timeframe="60",
#     symbol="AAVEUSD"
# )
