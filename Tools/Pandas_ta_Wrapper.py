#Run this file: python -m Tools.New_Indic



"""
Indicator Calculator
====================

Universal function for calculating indicators at a single point in time.
Works with any BaseIndicator subclass (PandasTAIndicator or custom).

Usage:
------
>>> from Tools.Indicator_Calculator import calculate_indicators
>>> from Tools.Indicators import PandasTAIndicator
>>> 
>>> indicators = {
>>>     "ema_200": {"name": "ema", "params": {"length": 200}},
>>>     "rsi_14": {"name": "rsi", "params": {"length": 14}}
>>> }
>>> 
>>> result = calculate_indicators(
>>>     symbol="BTCUSD",
>>>     timeframe="1h",
>>>     timestamp=1704067200,
>>>     indicators=indicators
>>> )
>>> print(result)
"""

import pandas as pd
from typing import Dict, Union
from datetime import datetime, timezone as tz
from ZZZ_Old_stuff.Indicators import BaseIndicator, PandasTAIndicator
from Tools.Kraken_ohlcv_Acquisitions import fetch_ohlcv


# ═══════════════════════════════════════════════════════════════════════
# TIMESTAMP ALIGNMENT UTILITY
# ═══════════════════════════════════════════════════════════════════════

def align_timestamp_to_timeframe(
    timestamp: int,
    timeframe: str,
    direction: str = "floor"
) -> int:
    """
    Align timestamp to the nearest valid candle for the given timeframe.
    
    Args:
        timestamp: Unix timestamp (any time)
        timeframe: Timeframe string (e.g., "1h", "15m", "1d")
        direction: "floor" (round down) or "ceil" (round up)
        
    Returns:
        int: Aligned timestamp
        
    Examples:
        >>> # 3:47 PM → align to 3:00 PM (hourly)
        >>> align_timestamp_to_timeframe(1718469220, "1h", "floor")
        1718467200
        
        >>> # June 15, 3:47 PM → align to June 15, 00:00 (daily)
        >>> align_timestamp_to_timeframe(1718469220, "1d", "floor")
        1718409600
        
        >>> # 8:43 AM → align to 9:00 AM (hourly, ceiling)
        >>> align_timestamp_to_timeframe(1718270580, "1h", "ceil")
        1718272800
    """
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
    
    if direction == "floor":
        return (timestamp // interval_seconds) * interval_seconds
    elif direction == "ceil":
        return ((timestamp + interval_seconds - 1) // interval_seconds) * interval_seconds
    else:
        raise ValueError(f"Invalid direction: {direction}. Must be 'floor' or 'ceil'.")


# ═══════════════════════════════════════════════════════════════════════
# MAIN CALCULATOR FUNCTION
# ═══════════════════════════════════════════════════════════════════════



def calculate_indicators(
    symbol: str,
    timeframe: str,
    timestamp: int,
    indicators: Dict[str, Union[dict, BaseIndicator]],
    db_name: str = "ohlcv_data",
    zip_folder: str = "Kraken_Zip_Files",
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "mysecretpassword",
    auto_align: bool = True,
    verbose: bool = False,
    debug: bool = False,
    print_summary: bool = False
) -> pd.DataFrame:
    """
    Calculate indicators for a single crypto at a single point in time.
    
    This function:
    1. Auto-aligns timestamp to valid candle boundary (optional)
    2. Determines the maximum lookback needed across all indicators
    3. Fetches OHLCV data using fetch_ohlcv (with extended range for lookback)
    4. Calculates each indicator using the full OHLCV data
    5. Extracts and merges only the row at the target timestamp
    6. Returns a single-row DataFrame with all indicator values
    
    Args:
        symbol: Trading pair (e.g., "BTCUSD", "ETHUSD")
        timeframe: Timeframe string (e.g., "1h", "15m", "1d")
        timestamp: Unix timestamp for target calculation point
        indicators: Dict of indicator specifications:
            - Key: Unique identifier (e.g., "ema_200", "rsi_14")
            - Value: Either:
                * Dict spec for PandasTAIndicator: 
                  {
                      "name": "ema", 
                      "params": {"length": 200},
                      "lookback_override": 250  # Optional: force specific lookback
                  }
                * BaseIndicator instance (for custom indicators)
        db_name: Database name for fetch_ohlcv (default: "ohlcv_data")
        zip_folder: ZIP folder path for fetch_ohlcv (default: "Kraken_Zip_Files")
        host: Database host (default: "localhost")
        port: Database port (default: 5432)
        user: Database user (default: "postgres")
        password: Database password (default: "mysecretpassword")
        auto_align: Automatically align timestamp to timeframe boundary (default: True)
        verbose: Show progress messages (default: False)
        debug: Show detailed debug information (default: False)
        print_summary: Print one-line summary at end (default: False)
        
    Returns:
        pd.DataFrame: Single-row DataFrame with columns:
            - timestamp_unix: Target timestamp (aligned if auto_align=True)
            - [indicator columns]: One or more columns per indicator
            
    Example:
        >>> # Silent mode (default)
        >>> result = calculate_indicators("BTCUSD", "1h", timestamp, indicators)
        
        >>> # With summary
        >>> result = calculate_indicators("BTCUSD", "1h", timestamp, indicators, print_summary=True)
        
        >>> # Verbose mode
        >>> result = calculate_indicators("BTCUSD", "1h", timestamp, indicators, verbose=True)
        
        >>> # Debug mode (most detailed)
        >>> result = calculate_indicators("BTCUSD", "1h", timestamp, indicators, debug=True)
        
    Raises:
        ValueError: If timeframe is invalid or indicators dict is empty
        RuntimeError: If OHLCV data fetch fails
        KeyError: If target timestamp not found in OHLCV data (when auto_align=False)
    """
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 1: VALIDATE INPUTS
    # ═══════════════════════════════════════════════════════════════════════
    
    if not indicators:
        raise ValueError("indicators dict cannot be empty")
    
    if debug or verbose:
        print("="*80)
        print(f"🧮 CALCULATING INDICATORS FOR {symbol} @ {timeframe}")
        print("="*80)
    
    if debug:
        print(f"Target timestamp: {timestamp}")
        print(f"Number of indicators: {len(indicators)}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 1.5: AUTO-ALIGN TIMESTAMP TO TIMEFRAME
    # ═══════════════════════════════════════════════════════════════════════
    
    if auto_align:
        original_timestamp = timestamp
        timestamp = align_timestamp_to_timeframe(timestamp, timeframe, direction="floor")
        
        if debug and original_timestamp != timestamp:
            orig_dt = datetime.fromtimestamp(original_timestamp, tz.utc)
            aligned_dt = datetime.fromtimestamp(timestamp, tz.utc)
            print(f"\n🔧 Auto-aligned timestamp:")
            print(f"   Original: {orig_dt} ({original_timestamp})")
            print(f"   Aligned:  {aligned_dt} ({timestamp})")
            print(f"   Timeframe: {timeframe}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 2: INSTANTIATE INDICATORS & DETERMINE MAX LOOKBACK
    # ═══════════════════════════════════════════════════════════════════════
    
    indicator_instances = {}
    max_lookback = 0
    
    for key, spec in indicators.items():
        # Check if spec is already a BaseIndicator instance
        if isinstance(spec, BaseIndicator):
            indicator = spec
            # Use indicator's built-in lookback
            lookback = indicator.lookback()
        else:
            # Assume it's a dict spec for PandasTAIndicator
            indicator = PandasTAIndicator.from_spec(spec)
            
            # Check for lookback_override in spec
            if "lookback_override" in spec:
                lookback = spec["lookback_override"]
                if debug:
                    print(f"  • {key}: {indicator.name} (lookback: {indicator.lookback()} → OVERRIDE: {lookback})")
            else:
                lookback = indicator.lookback()
                if debug:
                    print(f"  • {key}: {indicator.name} (lookback: {lookback})")
        
        # Store instance
        indicator_instances[key] = indicator
        
        # Track max lookback
        if lookback > max_lookback:
            max_lookback = lookback
    
    if debug:
        print(f"\n📏 Maximum lookback needed: {max_lookback} candles")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 3: CALCULATE EXTENDED TIME RANGE
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
    
    # Calculate start timestamp (go back by max_lookback candles)
    start_ts = timestamp - (max_lookback * interval_seconds)
    end_ts = timestamp
    
    if debug:
        start_dt = datetime.fromtimestamp(start_ts, tz.utc)
        end_dt = datetime.fromtimestamp(end_ts, tz.utc)
        print(f"\n📅 Fetching OHLCV data:")
        print(f"  Start: {start_dt} ({start_ts})")
        print(f"  End:   {end_dt} ({end_ts})")
        print(f"  Range: {max_lookback} candles")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 4: FETCH OHLCV DATA
    # ═══════════════════════════════════════════════════════════════════════
    
    try:
        ohlcv_df, missing_df = fetch_ohlcv(
            symbol=symbol,
            timeframe=timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            db_name=db_name,
            zip_folder=zip_folder,
            host=host,
            port=port,
            user=user,
            password=password,
            verbose=False,  # Let calculate_indicators control verbosity
            debug=False,
            print_summary=False
        )
    except Exception as e:
        # Always print errors
        print(f"❌ Error fetching OHLCV data for {symbol}: {e}")
        raise RuntimeError(f"Failed to fetch OHLCV data: {e}")
    
    if ohlcv_df.empty:
        # Always print critical errors
        print(f"❌ No OHLCV data found for {symbol} @ {timeframe} in range {start_ts}-{end_ts}")
        raise RuntimeError(f"No OHLCV data found for {symbol} @ {timeframe} in range {start_ts}-{end_ts}")
    
    if debug or verbose:
        print(f"\n✅ Fetched {len(ohlcv_df)} OHLCV candles")
    
    if debug and not missing_df.empty:
        print(f"⚠️  Warning: {len(missing_df)} candles missing in range")
    
    # Check if target timestamp exists in data
    if timestamp not in ohlcv_df['timestamp_unix'].values:
        # Always print critical errors
        print(f"❌ Target timestamp {timestamp} not found in OHLCV data for {symbol}")
        raise KeyError(
            f"Target timestamp {timestamp} not found in OHLCV data. "
            f"Available range: {ohlcv_df['timestamp_unix'].min()} to {ohlcv_df['timestamp_unix'].max()}"
        )
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 5: CALCULATE EACH INDICATOR
    # ═══════════════════════════════════════════════════════════════════════
    
    if debug or verbose:
        print(f"\n🔄 Calculating indicators...")
    
    # Start with base DataFrame containing only target timestamp
    result_df = pd.DataFrame({"timestamp_unix": [timestamp]})
    
    for key, indicator in indicator_instances.items():
        if debug:
            print(f"  • Calculating {key}...")
        
        try:
            # Calculate indicator on full OHLCV data
            indicator_full_df = indicator.calculate_from_df(ohlcv_df)
            
            if debug:
                print(f"    Full output shape: {indicator_full_df.shape}")
                print(f"    Columns: {list(indicator_full_df.columns)}")
            
            # Add timestamp_unix column if missing
            if 'timestamp_unix' not in indicator_full_df.columns:
                # Match timestamps from ohlcv_df by index alignment
                indicator_full_df['timestamp_unix'] = ohlcv_df['timestamp_unix'].values
                
                if debug:
                    print(f"    🔧 Added timestamp_unix column from ohlcv_df")
            
            # Extract only the row at target timestamp
            indicator_row = indicator_full_df[
                indicator_full_df['timestamp_unix'] == timestamp
            ].copy()
            
            if indicator_row.empty:
                if debug or verbose:
                    print(f"    ⚠️  Warning: No value at target timestamp for {key}")
                continue
            
            # Drop 'id' column if present (not needed in final output)
            if 'id' in indicator_row.columns:
                indicator_row = indicator_row.drop(columns=['id'])
            
            # Drop timestamp_unix to avoid duplicate when merging
            if 'timestamp_unix' in indicator_row.columns:
                indicator_row = indicator_row.drop(columns=['timestamp_unix'])
            
            # Reset index to enable clean merge
            indicator_row = indicator_row.reset_index(drop=True)
            result_df = result_df.reset_index(drop=True)
            
            # Merge horizontally (add columns)
            result_df = pd.concat([result_df, indicator_row], axis=1)
            
            if debug:
                print(f"    ✅ Added {len(indicator_row.columns)} column(s)")
        
        except Exception as e:
            # Always print errors
            print(f"❌ Error calculating indicator '{key}' for {symbol}: {e}")
            if debug:
                import traceback
                traceback.print_exc()
            raise RuntimeError(f"Failed to calculate indicator '{key}': {e}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # STEP 6: RETURN RESULT
    # ═══════════════════════════════════════════════════════════════════════
    
    if debug or verbose:
        print(f"\n✅ Calculation complete!")
    
    if debug:
        print(f"📊 Result shape: {result_df.shape}")
        print(f"📋 Columns: {list(result_df.columns)}")
    
    if print_summary:
        print(f"📊 {symbol} @ {timeframe}: Calculated {len(result_df.columns)-1} indicator column(s)")
    
    if debug or verbose:
        print("\n" + "="*80)
    
    return result_df



# ═══════════════════════════════════════════════════════════════════════
# 🧪 TEST SUITE
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    from datetime import datetime, timezone
    
    print("="*80)
    print("🧪 TESTING INDICATOR CALCULATOR WITH NEW FEATURES")
    print("="*80)
    
    # Test 1: Auto-alignment for hourly timeframe
    print("\n" + "─"*80)
    print("TEST 1: Auto-Alignment - Hourly (misaligned timestamp)")
    print("─"*80)
    
    indicators_test1 = {
        "ema_200": {"name": "ema", "params": {"length": 200}}
    }
    
    # Use a misaligned timestamp (3:47 PM)
    misaligned_ts = int(datetime(2024, 6, 15, 15, 47, 0, tzinfo=timezone.utc).timestamp())
    
    try:
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=misaligned_ts,
            indicators=indicators_test1,
            auto_align=True,
            debug=True
        )
        print("\n📊 Result:")
        print(result)
    except Exception as e:
        print(f"❌ Test 1 failed: {e}")
    
    # Test 2: Auto-alignment for daily timeframe
    print("\n" + "─"*80)
    print("TEST 2: Auto-Alignment - Daily (noon timestamp)")
    print("─"*80)
    
    indicators_test2 = {
        "ema_20": {"name": "ema", "params": {"length": 20}},
        "sma_50": {"name": "sma", "params": {"length": 50}}
    }
    
    # Use noon timestamp (should align to midnight)
    noon_ts = int(datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    
    try:
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1d",
            timestamp=noon_ts,
            indicators=indicators_test2,
            auto_align=True,
            debug=True
        )
        print("\n📊 Result:")
        print(result)
    except Exception as e:
        print(f"❌ Test 2 failed: {e}")
    
    # Test 3: Lookback override for MACD
    print("\n" + "─"*80)
    print("TEST 3: Lookback Override - MACD with increased buffer")
    print("─"*80)
    
    indicators_test3 = {
        "macd_default": {
            "name": "macd",
            "params": {"fast": 12, "slow": 26, "signal": 9},
            "lookback_override": 100  # Force 100 candles instead of default
        }
    }
    
    target_ts = int(datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    
    try:
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators_test3,
            auto_align=True,
            debug=True
        )
        print("\n📊 Result:")
        print(result)
    except Exception as e:
        print(f"❌ Test 3 failed: {e}")
    
    # Test 4: Multiple indicators with mixed lookback overrides
    print("\n" + "─"*80)
    print("TEST 4: Mixed Lookback Overrides")
    print("─"*80)
    
    indicators_test4 = {
        "ema_200": {
            "name": "ema",
            "params": {"length": 200},
            "lookback_override": 250  # Extra buffer
        },
        "rsi_14": {
            "name": "rsi",
            "params": {"length": 14}
            # No override - uses default
        },
        "macd_default": {
            "name": "macd",
            "params": {"fast": 12, "slow": 26, "signal": 9},
            "lookback_override": 100
        }
    }
    
    try:
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators_test4,
            auto_align=True,
            debug=True
        )
        print("\n📊 Result:")
        print(result)
        print("\n📋 Columns:")
        for col in result.columns:
            print(f"  • {col}")
    except Exception as e:
        print(f"❌ Test 4 failed: {e}")
    
    # Test 5: 15-minute timeframe with misaligned timestamp
    print("\n" + "─"*80)
    print("TEST 5: Auto-Alignment - 15-minute (8:43 timestamp)")
    print("─"*80)
    
    indicators_test5 = {
        "ema_50": {"name": "ema", "params": {"length": 50}},
        "rsi_14": {"name": "rsi", "params": {"length": 14}}
    }
    
    # Use 8:43 timestamp (should align to 8:30 for 15m)
    misaligned_15m = int(datetime(2024, 6, 15, 8, 43, 0, tzinfo=timezone.utc).timestamp())
    
    try:
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="15m",
            timestamp=misaligned_15m,
            indicators=indicators_test5,
            auto_align=True,
            debug=True
        )
        print("\n📊 Result:")
        print(result)
    except Exception as e:
        print(f"❌ Test 5 failed: {e}")
    
    print("\n" + "="*80)
    print("✅ TESTS COMPLETE")
    print("="*80)

# ═══════════════════════════════════════════════════════════════════════
# 🧪 TEST SUITE
# ═══════════════════════════════════════════════════════════════════════

# if __name__ == "__main__":
#     from datetime import datetime, timezone
    
#     print("="*80)
#     print("🧪 TESTING INDICATOR CALCULATOR")
#     print("="*80)
    
#     # Test 1: Single indicator (EMA)
#     print("\n" + "─"*80)
#     print("TEST 1: Single Indicator (EMA 200)")
#     print("─"*80)
    
#     indicators_test1 = {
#         "ema_200": {"name": "ema", "params": {"length": 200}}
#     }
    
#     target_ts = int(datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    
#     try:
#         result = calculate_indicators(
#             symbol="ETHUSD",
#             timeframe="1h",
#             timestamp=target_ts,
#             indicators=indicators_test1,
#             debug=True
#         )
#         print("\n📊 Result:")
#         print(result)
#     except Exception as e:
#         print(f"❌ Test 1 failed: {e}")
    
#     # Test 2: Multiple indicators
#     print("\n" + "─"*80)
#     print("TEST 2: Multiple Indicators (EMA, RSI, MACD)")
#     print("─"*80)
    
#     indicators_test2 = {
#         "ema_200": {"name": "ema", "params": {"length": 200}},
#         "rsi_14": {"name": "rsi", "params": {"length": 14}},
#         "macd_default": {"name": "macd", "params": {"fast": 12, "slow": 26, "signal": 9}}
#     }
    
#     try:
#         result = calculate_indicators(
#             symbol="ETHUSD",
#             timeframe="1h",
#             timestamp=target_ts,
#             indicators=indicators_test2,
#             debug=True
#         )
#         print("\n📊 Result:")
#         print(result)
#         print("\n📋 Columns:")
#         for col in result.columns:
#             print(f"  • {col}")
#     except Exception as e:
#         print(f"❌ Test 2 failed: {e}")
    
#     print("\n" + "="*80)
#     print("✅ TESTS COMPLETE")
#     print("="*80)



#     """
# Custom SMA Indicator Example
# =============================

# Demonstrates how to create a custom indicator using BaseIndicator.
# This is a simple SMA implementation to show the pattern.
# """

import pandas as pd
from ZZZ_Old_stuff.Indicators import BaseIndicator


class CustomSMA(BaseIndicator):
    """
    Custom Simple Moving Average indicator using BaseIndicator.
    
    This demonstrates how to create your own indicators that work
    seamlessly with calculate_indicators().
    
    Args:
        name: Indicator name (default: "custom_sma")
        params: Dict with "length" key for SMA period
        
    Example:
        >>> sma_50 = CustomSMA(name="custom_sma", params={"length": 50})
        >>> result_df = sma_50.calculate_from_df(ohlcv_df)
    """
    
    def __init__(self, name: str = "custom_sma", params: dict = None):
        """
        Initialize Custom SMA indicator.
        
        Args:
            name: Indicator name (default: "custom_sma")
            params: Parameters dict, must contain "length" (default: {"length": 20})
        """
        if params is None:
            params = {"length": 20}
        
        # Validate that length is provided
        if "length" not in params:
            raise ValueError("CustomSMA requires 'length' parameter")
        
        super().__init__(name=name, params=params)
    
    def lookback(self) -> int:
        """
        Returns number of candles needed for SMA calculation.
        
        For SMA, we need the length + a small buffer for edge cases.
        
        Returns:
            int: Number of historical candles required
        """
        length = self.params.get("length", 20)
        buffer = 5  # Small buffer for safety
        return length + buffer
    
    def calculate_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate Simple Moving Average from OHLCV DataFrame.
        
        Args:
            df: OHLCV DataFrame with columns: timestamp_unix, open, high, low, close, volume
            
        Returns:
            pd.DataFrame: Result with columns:
                - timestamp_unix: Unix timestamps
                - SMA_{length}: Calculated SMA values
                - id: Indicator identifier
                
        Example:
            >>> sma = CustomSMA(params={"length": 50})
            >>> result = sma.calculate_from_df(ohlcv_df)
            >>> print(result.tail())
        """
        length = self.params["length"]
        
        # Calculate SMA using pandas rolling mean
        sma_values = df['close'].rolling(window=length).mean()
        
        # Create result DataFrame with proper structure
        result_df = pd.DataFrame({
            'timestamp_unix': df['timestamp_unix'].values,
            f'SMA_{length}': sma_values.values,
            'id': self.id
        })
        
        return result_df


def create_custom_sma(length: int = 20) -> CustomSMA:
    """
    Helper function to create a CustomSMA indicator.
    
    This is a convenience function for quick indicator creation.
    
    Args:
        length: SMA period (default: 20)
        
    Returns:
        CustomSMA: Configured Custom SMA indicator instance
        
    Example:
        >>> # Create SMA(50)
        >>> sma_50 = create_custom_sma(length=50)
        >>> 
        >>> # Use with calculate_indicators
        >>> indicators = {
        ...     "sma_50": sma_50,
        ...     "ema_200": {"name": "ema", "params": {"length": 200}}
        ... }
        >>> result = calculate_indicators("BTCUSD", "1h", timestamp, indicators)
    """
    return CustomSMA(
        name="custom_sma",
        params={"length": length}
    )


# ═══════════════════════════════════════════════════════════════════════
# 🧪 TEST CUSTOM SMA
# ═══════════════════════════════════════════════════════════════════════

# if __name__ == "__main__":
#     from datetime import datetime, timezone
#     from Tools.New_C_I import fetch_ohlcv
    
#     print("="*80)
#     print("🧪 TESTING CUSTOM SMA INDICATOR")
#     print("="*80)
    
#     # Test 1: Create indicator and test lookback
#     print("\n" + "─"*80)
#     print("TEST 1: Indicator Creation & Lookback")
#     print("─"*80)
    
#     sma_50 = create_custom_sma(length=50)
#     print(f"✅ Created: {sma_50.name}")
#     print(f"📋 Parameters: {sma_50.params}")
#     print(f"🆔 ID: {sma_50.id}")
#     print(f"📏 Lookback: {sma_50.lookback()} candles")
    
#     # Test 2: Calculate from real OHLCV data
#     print("\n" + "─"*80)
#     print("TEST 2: Calculate from OHLCV Data")
#     print("─"*80)
    
#     # Fetch some data
#     target_ts = int(datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp())
#     start_ts = target_ts - (100 * 3600)  # 100 hours back
    
#     print(f"📥 Fetching OHLCV data...")
#     ohlcv_df, _ = fetch_ohlcv(
#         symbol="ETHUSD",
#         timeframe="1h",
#         start_ts=start_ts,
#         end_ts=target_ts
#     )
    
#     print(f"✅ Fetched {len(ohlcv_df)} candles")
    
#     # Calculate SMA
#     print(f"\n🔄 Calculating SMA(50)...")
#     result_df = sma_50.calculate_from_df(ohlcv_df)
    
#     print(f"✅ Calculated SMA")
#     print(f"📊 Result shape: {result_df.shape}")
#     print(f"📋 Columns: {list(result_df.columns)}")
    
#     print(f"\n📈 Last 5 values:")
#     print(result_df.tail())
    
#     # Test 3: Compare with pandas_ta SMA
#     print("\n" + "─"*80)
#     print("TEST 3: Comparison with pandas_ta SMA")
#     print("─"*80)
    
#     import pandas_ta as ta
    
#     # Calculate using pandas_ta
#     pandas_ta_sma = ta.sma(ohlcv_df['close'], length=50)
    
#     # Get our custom SMA values
#     custom_sma = result_df['SMA_50'].values
    
#     # Compare (skip NaN values)
#     valid_mask = ~pd.isna(custom_sma) & ~pd.isna(pandas_ta_sma)
#     difference = abs(custom_sma[valid_mask] - pandas_ta_sma[valid_mask])
#     max_diff = difference.max()
    
#     print(f"📊 Maximum difference: {max_diff:.10f}")
    
#     if max_diff < 0.0001:
#         print(f"✅ Results match pandas_ta perfectly!")
#     else:
#         print(f"⚠️  Small differences detected (expected due to floating point)")
    
#     # Test 4: Use with calculate_indicators (if available)
#     print("\n" + "─"*80)
#     print("TEST 4: Integration with calculate_indicators")
#     print("─"*80)
    
#     try:
        
        
#         # Create indicator dict mixing custom and pandas_ta
#         indicators = {
#             "custom_sma_50": create_custom_sma(length=50),
#             "pandas_sma_20": {"name": "sma", "params": {"length": 20}},
#             "ema_200": {"name": "ema", "params": {"length": 200}}
#         }
        
#         print(f"🧮 Calculating indicators at timestamp {target_ts}...")
#         result = calculate_indicators(
#             symbol="ETHUSD",
#             timeframe="1h",
#             timestamp=target_ts,
#             indicators=indicators,
#             debug=False
#         )
        
#         print(f"\n✅ Calculation complete!")
#         print(f"📊 Result:")
#         print(result)
        
#         print(f"\n📋 All columns:")
#         for col in result.columns:
#             print(f"  • {col}")
            
#     except ImportError:
#         print("⚠️  calculate_indicators not available - skipping integration test")
    
#     print("\n" + "="*80)
#     print("✅ ALL TESTS COMPLETE")
#     print("="*80)




"""
COMPREHENSIVE STRESS TEST SUITE FOR INDICATOR CALCULATOR
=========================================================

Attempts to break calculate_indicators() through:
- Edge cases
- Parameter conflicts
- Data quality issues
- Type mismatches
- Resource exhaustion
- Boundary conditions
"""

import pandas as pd
from datetime import datetime, timezone, timedelta
from ZZZ_Old_stuff.Indicators import BaseIndicator, PandasTAIndicator
import traceback


# ═══════════════════════════════════════════════════════════════════════
# CUSTOM TEST INDICATORS
# ═══════════════════════════════════════════════════════════════════════

class CustomSMA(BaseIndicator):
    """Custom SMA for testing."""
    
    def lookback(self) -> int:
        return self.params.get("length", 20) + 5
    
    def calculate_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        length = self.params["length"]
        sma_values = df['close'].rolling(window=length).mean()
        return pd.DataFrame({
            'timestamp_unix': df['timestamp_unix'].values,
            f'SMA_{length}': sma_values.values,
            'id': self.id
        })


class BrokenIndicator(BaseIndicator):
    """Intentionally broken indicator to test error handling."""
    
    def lookback(self) -> int:
        return 10
    
    def calculate_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Intentionally raise an error
        raise RuntimeError("This indicator is intentionally broken!")


class WeirdOutputIndicator(BaseIndicator):
    """Returns non-standard output to test normalization."""
    
    def lookback(self) -> int:
        return 10
    
    def calculate_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Return DataFrame without timestamp_unix
        return pd.DataFrame({
            'weird_col_1': [1.0] * len(df),
            'weird_col_2': [2.0] * len(df),
            'id': self.id
        })


class ZeroLookbackIndicator(BaseIndicator):
    """Indicator with zero lookback."""
    
    def lookback(self) -> int:
        return 0
    
    def calculate_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({
            'timestamp_unix': df['timestamp_unix'].values,
            'zero_lookback_value': [42.0] * len(df),
            'id': self.id
        })


class MassiveLookbackIndicator(BaseIndicator):
    """Indicator requiring huge lookback to test resource limits."""
    
    def lookback(self) -> int:
        return 10000  # Requires 10k candles
    
    def calculate_from_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Simple calculation
        result = df['close'].mean()
        return pd.DataFrame({
            'timestamp_unix': df['timestamp_unix'].values,
            'massive_avg': [result] * len(df),
            'id': self.id
        })


# ═══════════════════════════════════════════════════════════════════════
# STRESS TEST RUNNER
# ═══════════════════════════════════════════════════════════════════════

class StressTestRunner:
    """Runs comprehensive stress tests and tracks results."""
    
    def __init__(self):
        self.results = []
        self.passed = 0
        self.failed = 0
        self.errors = 0
        
    def run_test(self, category: str, name: str, test_fn: callable, should_fail: bool = False):
        """
        Run a single test and track results.
        
        Args:
            category: Test category (e.g., "EDGE_CASES")
            name: Test name
            test_fn: Function that runs the test
            should_fail: If True, test passes only if it raises an exception
        """
        print(f"\n{'─'*80}")
        print(f"🧪 {category}: {name}")
        print(f"{'─'*80}")
        
        try:
            result = test_fn()
            
            if should_fail:
                # Test should have failed but didn't
                print(f"❌ FAIL: Expected error but got success")
                self.failed += 1
                self.results.append({
                    'category': category,
                    'name': name,
                    'status': 'FAIL',
                    'reason': 'Expected failure but succeeded'
                })
            else:
                print(f"✅ PASS")
                self.passed += 1
                self.results.append({
                    'category': category,
                    'name': name,
                    'status': 'PASS',
                    'result': result
                })
                
        except Exception as e:
            if should_fail:
                # Expected error occurred
                print(f"✅ PASS: Correctly raised {type(e).__name__}: {e}")
                self.passed += 1
                self.results.append({
                    'category': category,
                    'name': name,
                    'status': 'PASS',
                    'reason': f'Expected error: {type(e).__name__}'
                })
            else:
                # Unexpected error
                print(f"❌ ERROR: {type(e).__name__}: {e}")
                traceback.print_exc()
                self.errors += 1
                self.results.append({
                    'category': category,
                    'name': name,
                    'status': 'ERROR',
                    'error': str(e)
                })
    
    def print_summary(self):
        """Print final test summary."""
        print("\n" + "="*80)
        print("📊 STRESS TEST SUMMARY")
        print("="*80)
        
        # Group by category
        categories = {}
        for result in self.results:
            cat = result['category']
            if cat not in categories:
                categories[cat] = {'PASS': 0, 'FAIL': 0, 'ERROR': 0}
            categories[cat][result['status']] += 1
        
        # Print by category
        for cat, counts in categories.items():
            total = sum(counts.values())
            print(f"\n📁 {cat}:")
            print(f"   ✅ PASS:  {counts['PASS']}/{total}")
            print(f"   ❌ FAIL:  {counts['FAIL']}/{total}")
            print(f"   💥 ERROR: {counts['ERROR']}/{total}")
        
        # Overall
        total_tests = len(self.results)
        print(f"\n🎯 OVERALL RESULTS:")
        print(f"   Total Tests: {total_tests}")
        print(f"   ✅ Passed:   {self.passed} ({self.passed/total_tests*100:.1f}%)")
        print(f"   ❌ Failed:   {self.failed} ({self.failed/total_tests*100:.1f}%)")
        print(f"   💥 Errors:   {self.errors} ({self.errors/total_tests*100:.1f}%)")
        
        if self.failed == 0 and self.errors == 0:
            print(f"\n🎉 ALL TESTS PASSED! Function is robust!")
        else:
            print(f"\n⚠️  ISSUES DETECTED - Review failures above")
        
        print("="*80)


# ═══════════════════════════════════════════════════════════════════════
# TEST SUITE
# ═══════════════════════════════════════════════════════════════════════

def run_stress_tests():
    """Execute comprehensive stress test suite."""
    
    runner = StressTestRunner()
    
    # Common test timestamp
    target_ts = int(datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    
    print("="*80)
    print("🔥 INDICATOR CALCULATOR STRESS TEST SUITE")
    print("="*80)
    print(f"Target timestamp: {target_ts}")
    print(f"Symbol: ETHUSD")
    print(f"Timeframe: 1h")
    print("="*80)
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 1: SAME INDICATOR, DIFFERENT PARAMETERS
    # ═══════════════════════════════════════════════════════════════════════
    
    def test_same_indicator_different_params():
        """Test calling same indicator (EMA) with different parameters."""
        indicators = {
            "ema_10": {"name": "ema", "params": {"length": 10}},
            "ema_20": {"name": "ema", "params": {"length": 20}},
            "ema_50": {"name": "ema", "params": {"length": 50}},
            "ema_100": {"name": "ema", "params": {"length": 100}},
            "ema_200": {"name": "ema", "params": {"length": 200}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        # Verify all columns exist and have different values
        expected_cols = ['timestamp_unix', 'EMA_10', 'EMA_20', 'EMA_50', 'EMA_100', 'EMA_200']
        for col in expected_cols:
            if col not in result.columns:
                raise AssertionError(f"Missing column: {col}")
        
        # Check values are different
        ema_values = [result['EMA_10'].values[0], result['EMA_20'].values[0], 
                      result['EMA_50'].values[0], result['EMA_100'].values[0], 
                      result['EMA_200'].values[0]]
        
        if len(set(ema_values)) != 5:
            raise AssertionError(f"EMA values should be different but got: {ema_values}")
        
        print(f"   All 5 EMAs calculated with unique values: {ema_values}")
        return result
    
    runner.run_test("PARAMETER_CONFLICTS", "Same indicator (EMA) with 5 different parameters", 
                    test_same_indicator_different_params)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_same_custom_indicator_different_params():
        """Test custom SMA with different parameters."""
        indicators = {
            "custom_sma_10": CustomSMA(name="custom_sma", params={"length": 10}),
            "custom_sma_50": CustomSMA(name="custom_sma", params={"length": 50}),
            "custom_sma_200": CustomSMA(name="custom_sma", params={"length": 200})
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        # Verify columns
        if 'SMA_10' not in result.columns or 'SMA_50' not in result.columns or 'SMA_200' not in result.columns:
            raise AssertionError(f"Missing SMA columns. Got: {list(result.columns)}")
        
        print(f"   Custom SMAs calculated: {list(result.columns)}")
        return result
    
    runner.run_test("PARAMETER_CONFLICTS", "Custom SMA with 3 different parameters", 
                    test_same_custom_indicator_different_params)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_mixed_same_indicator():
        """Test same indicator name but different types (pandas_ta vs custom)."""
        indicators = {
            "pandas_sma_20": {"name": "sma", "params": {"length": 20}},
            "custom_sma_20": CustomSMA(name="custom_sma", params={"length": 20}),
            "pandas_sma_50": {"name": "sma", "params": {"length": 50}},
            "custom_sma_50": CustomSMA(name="custom_sma", params={"length": 50})
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        print(f"   Mixed indicators calculated: {list(result.columns)}")
        return result
    
    runner.run_test("PARAMETER_CONFLICTS", "Same indicator (SMA) - pandas_ta vs custom", 
                    test_mixed_same_indicator)
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 2: MULTI-OUTPUT INDICATORS
    # ═══════════════════════════════════════════════════════════════════════
    
    def test_macd_multiple_outputs():
        """Test MACD which returns 3 columns."""
        indicators = {
            "macd_default": {
                "name": "macd",
                "params": {"fast": 12, "slow": 26, "signal": 9},
                "lookback_override": 100  # ← FIXED: Force enough data for MACD
            }
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        # MACD returns 3 columns: MACD, MACDh (histogram), MACDs (signal)
        macd_cols = [col for col in result.columns if 'MACD' in col]
        if len(macd_cols) != 3:
            raise AssertionError(f"Expected 3 MACD columns but got {len(macd_cols)}: {macd_cols}")
        
        print(f"   MACD columns: {macd_cols}")
        return result
    
    runner.run_test("MULTI_OUTPUT", "MACD (3 output columns)", test_macd_multiple_outputs)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_bbands_multiple_outputs():
        """Test Bollinger Bands which returns 3 columns."""
        indicators = {
            "bbands_20": {"name": "bbands", "params": {"length": 20, "std": 2}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        # BBands returns 3 columns: lower, mid, upper
        bbands_cols = [col for col in result.columns if 'BBL' in col or 'BBM' in col or 'BBU' in col]
        if len(bbands_cols) != 3:
            raise AssertionError(f"Expected 3 BBands columns but got {len(bbands_cols)}: {bbands_cols}")
        
        print(f"   BBands columns: {bbands_cols}")
        return result
    
    runner.run_test("MULTI_OUTPUT", "Bollinger Bands (3 output columns)", test_bbands_multiple_outputs)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_multiple_multi_output_indicators():
        """Test multiple indicators that each return multiple columns."""
        indicators = {
            "macd_1": {
                "name": "macd",
                "params": {"fast": 12, "slow": 26, "signal": 9},
                "lookback_override": 100
            },
            "macd_2": {
                "name": "macd",
                "params": {"fast": 5, "slow": 13, "signal": 5},
                "lookback_override": 100
            },
            "bbands_20": {"name": "bbands", "params": {"length": 20, "std": 2}},
            "bbands_50": {"name": "bbands", "params": {"length": 50, "std": 2.5}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        # Should have: timestamp + 3 MACD cols + 3 MACD cols + 3 BBands + 3 BBands = 13 columns
        expected_min_cols = 13
        if len(result.columns) < expected_min_cols:
            raise AssertionError(f"Expected at least {expected_min_cols} columns but got {len(result.columns)}")
        
        print(f"   Total columns: {len(result.columns)}")
        print(f"   Columns: {list(result.columns)}")
        return result
    
    runner.run_test("MULTI_OUTPUT", "Multiple multi-output indicators together", 
                    test_multiple_multi_output_indicators)
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 3: MIXING CUSTOM AND PANDAS_TA
    # ═══════════════════════════════════════════════════════════════════════
    
    def test_mixed_indicator_types():
        """Test mixing custom indicators and pandas_ta indicators."""
        indicators = {
            "custom_sma_50": CustomSMA(name="custom_sma", params={"length": 50}),
            "pandas_ema_200": {"name": "ema", "params": {"length": 200}},
            "pandas_rsi_14": {"name": "rsi", "params": {"length": 14}},
            "custom_sma_20": CustomSMA(name="custom_sma", params={"length": 20}),
            "pandas_macd": {
                "name": "macd",
                "params": {"fast": 12, "slow": 26, "signal": 9},
                "lookback_override": 100
            }
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        print(f"   Mixed types calculated: {len(result.columns)} columns")
        return result
    
    runner.run_test("MIXED_TYPES", "Custom + pandas_ta indicators mixed", test_mixed_indicator_types)
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 4: EDGE CASES - EMPTY/INVALID INPUTS
    # ═══════════════════════════════════════════════════════════════════════
    
    def test_empty_indicators_dict():
        """Test with empty indicators dict (should fail)."""
        indicators = {}
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        return result
    
    runner.run_test("EDGE_CASES", "Empty indicators dict", test_empty_indicators_dict, should_fail=True)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_invalid_symbol():
        """Test with non-existent symbol."""
        indicators = {
            "ema_20": {"name": "ema", "params": {"length": 20}}
        }
        
        result = calculate_indicators(
            symbol="FAKECOIN",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        return result
    
    runner.run_test("EDGE_CASES", "Invalid symbol (FAKECOIN)", test_invalid_symbol, should_fail=True)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_invalid_timeframe():
        """Test with invalid timeframe."""
        indicators = {
            "ema_20": {"name": "ema", "params": {"length": 20}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="99x",  # Invalid
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        return result
    
    runner.run_test("EDGE_CASES", "Invalid timeframe (99x)", test_invalid_timeframe, should_fail=True)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_future_timestamp():
        """Test with timestamp in the future."""
        future_ts = int(datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())
        indicators = {
            "ema_20": {"name": "ema", "params": {"length": 20}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=future_ts,
            indicators=indicators,
            debug=False
        )
        return result
    
    runner.run_test("EDGE_CASES", "Future timestamp (2030)", test_future_timestamp, should_fail=True)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_invalid_pandas_ta_indicator():
        """Test with non-existent pandas_ta indicator."""
        indicators = {
            "fake_indicator": {"name": "this_does_not_exist", "params": {}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        return result
    
    runner.run_test("EDGE_CASES", "Non-existent pandas_ta indicator", 
                    test_invalid_pandas_ta_indicator, should_fail=True)
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 5: EXTREME LOOKBACK VALUES
    # ═══════════════════════════════════════════════════════════════════════
    
    def test_zero_lookback():
        """Test indicator with zero lookback."""
        indicators = {
            "zero_lookback": ZeroLookbackIndicator(name="zero", params={})
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        print(f"   Zero lookback handled successfully")
        return result
    
    runner.run_test("EXTREME_LOOKBACK", "Zero lookback indicator", test_zero_lookback)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_single_candle_lookback():
        """Test indicator requiring only 1 candle."""
        indicators = {
            "ema_1": {"name": "ema", "params": {"length": 1}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        print(f"   Single candle lookback handled")
        return result
    
    runner.run_test("EXTREME_LOOKBACK", "Single candle lookback (EMA 1)", test_single_candle_lookback)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_massive_lookback():
        """Test indicator requiring huge lookback (may fail due to data availability)."""
        indicators = {
            "massive": MassiveLookbackIndicator(name="massive", params={})
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        print(f"   Massive lookback (10000 candles) handled!")
        return result
    
    runner.run_test("EXTREME_LOOKBACK", "Massive lookback (10000 candles)", 
                    test_massive_lookback, should_fail=False)  # May pass or fail depending on data
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 6: BROKEN INDICATORS
    # ═══════════════════════════════════════════════════════════════════════
    
    def test_broken_indicator():
        """Test intentionally broken indicator."""
        indicators = {
            "broken": BrokenIndicator(name="broken", params={})
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        return result
    
    runner.run_test("ERROR_HANDLING", "Intentionally broken indicator", 
                    test_broken_indicator, should_fail=True)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_weird_output_indicator():
        """Test indicator returning non-standard output."""
        indicators = {
            "weird": WeirdOutputIndicator(name="weird", params={})
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        print(f"   Weird output handled, columns: {list(result.columns)}")
        return result
    
    runner.run_test("ERROR_HANDLING", "Indicator with weird output format", test_weird_output_indicator)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_one_broken_among_good():
        """Test one broken indicator among good ones."""
        indicators = {
            "ema_20": {"name": "ema", "params": {"length": 20}},
            "broken": BrokenIndicator(name="broken", params={}),
            "rsi_14": {"name": "rsi", "params": {"length": 14}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        return result
    
    runner.run_test("ERROR_HANDLING", "One broken indicator among good ones", 
                    test_one_broken_among_good, should_fail=True)
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 7: MASSIVE SCALE
    # ═══════════════════════════════════════════════════════════════════════
    
    def test_many_indicators():
        """Test calculating 20+ indicators at once."""
        indicators = {}
        
        # Add 10 EMAs
        for length in [5, 10, 20, 30, 50, 100, 150, 200, 250, 300]:
            indicators[f"ema_{length}"] = {"name": "ema", "params": {"length": length}}
        
        # Add 5 SMAs
        for length in [10, 20, 50, 100, 200]:
            indicators[f"sma_{length}"] = {"name": "sma", "params": {"length": length}}
        
        # Add 5 RSIs
        for length in [7, 14, 21, 28, 35]:
            indicators[f"rsi_{length}"] = {"name": "rsi", "params": {"length": length}}
        
        # Add 3 MACDs with lookback overrides
        indicators["macd_1"] = {
            "name": "macd",
            "params": {"fast": 12, "slow": 26, "signal": 9},
            "lookback_override": 100
        }
        indicators["macd_2"] = {
            "name": "macd",
            "params": {"fast": 5, "slow": 13, "signal": 5},
            "lookback_override": 100
        }
        indicators["macd_3"] = {
            "name": "macd",
            "params": {"fast": 8, "slow": 21, "signal": 7},
            "lookback_override": 100
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1h",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        print(f"   Calculated {len(indicators)} indicators successfully")
        print(f"   Result has {len(result.columns)} columns")
        return result
    
    runner.run_test("MASSIVE_SCALE", "20+ indicators at once", test_many_indicators)
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 8: DIFFERENT TIMEFRAMES
    # ═══════════════════════════════════════════════════════════════════════
    
    def test_15min_timeframe():
        """Test with 15-minute timeframe."""
        indicators = {
            "ema_50": {"name": "ema", "params": {"length": 50}},
            "rsi_14": {"name": "rsi", "params": {"length": 14}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="15m",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        print(f"   15-minute timeframe successful")
        return result
    
    runner.run_test("TIMEFRAMES", "15-minute timeframe", test_15min_timeframe)
    
    # ───────────────────────────────────────────────────────────────────────
    
    def test_daily_timeframe():
        """Test with daily timeframe."""
        indicators = {
            "ema_20": {"name": "ema", "params": {"length": 20}},
            "sma_50": {"name": "sma", "params": {"length": 50}}
        }
        
        result = calculate_indicators(
            symbol="ETHUSD",
            timeframe="1d",
            timestamp=target_ts,
            indicators=indicators,
            debug=False
        )
        
        print(f"   Daily timeframe successful")
        return result
    
    runner.run_test("TIMEFRAMES", "Daily timeframe", test_daily_timeframe)
    
    # ═══════════════════════════════════════════════════════════════════════
    # CATEGORY 9: STRESS - REPEATED CALLS
    # ═══════════════════════════════════════════════════════════════════════
    
    def test_rapid_consecutive_calls():
        """Test making 10 consecutive calls rapidly."""
        indicators = {
            "ema_20": {"name": "ema", "params": {"length": 20}},
            "rsi_14": {"name": "rsi", "params": {"length": 14}}
        }
        
        results = []
        for i in range(10):
            result = calculate_indicators(
                symbol="ETHUSD",
                timeframe="1h",
                timestamp=target_ts,
                indicators=indicators,
                debug=False
            )
            results.append(result)
        
        # Verify all results are identical
        first_ema = results[0]['EMA_20'].values[0]
        for r in results[1:]:
            if r['EMA_20'].values[0] != first_ema:
                raise AssertionError("Consecutive calls returned different results!")
        
        print(f"   10 consecutive calls returned consistent results")
        return results[0]
    
    runner.run_test("STRESS", "10 rapid consecutive calls", test_rapid_consecutive_calls)
    
    # ═══════════════════════════════════════════════════════════════════════
    # PRINT FINAL SUMMARY
    # ═══════════════════════════════════════════════════════════════════════
    
    runner.print_summary()


# ═══════════════════════════════════════════════════════════════════════
# RUN TESTS
# ═══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_stress_tests()