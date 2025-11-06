import time
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

class KrakenAPIEnforcer:
    def __init__(
        self,
        db_folder="Api mangment data",
        db_filename="kraken_api_usage.db",
        window_seconds=10,
        max_requests=20
    ):
        self.db_folder     = Path(db_folder)
        self.db_path       = self.db_folder / db_filename
        self.window        = timedelta(seconds=window_seconds)
        self.max_requests  = max_requests
        self._init_db()

    def _init_db(self):
        # Ensure the directory exists
        self.db_folder.mkdir(parents=True, exist_ok=True)

        # Connect to the database
        self.conn = sqlite3.connect(self.db_path, timeout=10, isolation_level=None)

        # Create table and index if they don't exist
        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS api_calls (
                timestamp TEXT
            )
        ''')
        self.conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_timestamp ON api_calls(timestamp)
        ''')

    def _now_utc(self):
        return datetime.utcnow()

    def log_and_maybe_pause(self, endpoint=None, symbol=None, timeframe=None, limit=None):
        now = self._now_utc()
        cutoff = now - self.window

        self.conn.execute(
            "DELETE FROM api_calls WHERE timestamp < ?",
            (cutoff.isoformat(),)
        )

        cur = self.conn.execute(
            "SELECT timestamp FROM api_calls ORDER BY timestamp ASC"
        )
        recent_calls = [datetime.fromisoformat(row[0]) for row in cur.fetchall()]

        wait_time = 0
        if len(recent_calls) >= self.max_requests:
            oldest = recent_calls[0]
            wait_time = (oldest + self.window - now).total_seconds()
            print(f"⏸ Limit hit: {len(recent_calls)} requests in last {self.window.total_seconds()}s")
            print(f"🕒 Kraken rate limit hit — waiting {wait_time:.2f} seconds before proceeding")
            time.sleep(wait_time)

        # Log this call (you can expand this to include endpoint/symbol/etc later)
        self.conn.execute(
            "INSERT INTO api_calls (timestamp) VALUES (?)",
            (now.isoformat(),)
        )

        return wait_time
