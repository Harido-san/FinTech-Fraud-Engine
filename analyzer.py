"""
FinTech-Fraud-Engine — Live Fraud Consumer
==========================================
Continuously tails ``live_transactions.log`` in real-time (like tail -f),
maintains a rolling window of the last 100 transactions, and fires a
SOC alert whenever a Card Testing pattern is detected.

Rule — Card Testing:
    An account with **more than 4 transactions under $2.00 within the
    last 60 seconds** is flagged.

Usage:
    python analyzer.py           # runs indefinitely — Ctrl+C to stop
"""

import json
import sys
import os
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, Any, Deque

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

LOG_FILE = "live_transactions.log"
WINDOW_SIZE = 100
MICRO_THRESHOLD_USD = 2.00
MICRO_COUNT_TRIGGER = 4
MICRO_WINDOW_SECONDS = 60

_BORDER = "=" * 70


class LiveConsumer:
    """Tails a transaction log and applies real-time fraud detection."""

    def __init__(self, log_path: str = LOG_FILE) -> None:
        self.log_path = log_path
        self.window: Deque[Dict[str, Any]] = deque(maxlen=WINDOW_SIZE)
        # Track which accounts we have already alerted on to avoid spam
        self._alerted: set = set()

    # ------------------------------------------------------------------ #
    #  Detection: Card Testing                                            #
    # ------------------------------------------------------------------ #
    def _check_card_testing(self, txn: Dict[str, Any]) -> None:
        """
        Evaluate whether the incoming transaction's account violates the
        Card Testing rule within the rolling window.

        Args:
            txn: The newly ingested transaction dict.
        """
        acct = txn["account_id"]
        now = datetime.fromisoformat(txn["timestamp"])
        cutoff = now - timedelta(seconds=MICRO_WINDOW_SECONDS)

        # Count micro-transactions for this account inside the time window
        micro_count = sum(
            1
            for t in self.window
            if t["account_id"] == acct
            and t["amount_usd"] < MICRO_THRESHOLD_USD
            and datetime.fromisoformat(t["timestamp"]) >= cutoff
        )

        if micro_count > MICRO_COUNT_TRIGGER and acct not in self._alerted:
            self._alerted.add(acct)
            self._fire_alert(acct, micro_count, txn)

    def _fire_alert(
        self, account: str, micro_count: int, latest: Dict[str, Any]
    ) -> None:
        """Print a high-visibility SOC alert."""
        print(f"\n{_BORDER}")
        print(f"  🚨🚨🚨  [CRITICAL]  CARD TESTING ATTACK DETECTED  🚨🚨🚨")
        print(_BORDER)
        print(f"  ▸ Account:            {account}")
        print(f"  ▸ Micro-txns (< $2):  {micro_count} in the last {MICRO_WINDOW_SECONDS}s")
        print(f"  ▸ Latest amount:      ${latest['amount_usd']:,.2f}")
        print(f"  ▸ Location:           {latest['location_country']}")
        print(f"  ▸ Timestamp:          {latest['timestamp']}")
        print(f"  ▸ Action:             BLOCK card & escalate to L2 SOC Analyst")
        print(f"{_BORDER}\n")

    # ------------------------------------------------------------------ #
    #  Tail loop                                                          #
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        """Tail the log file forever, processing each new JSON line."""
        print("=" * 70)
        print("   FinTech-Fraud-Engine  ·  Live Consumer  ·  MONITORING")
        print("=" * 70)
        print(f"   Watching: {os.path.abspath(self.log_path)}")
        print("   Press Ctrl+C to stop.\n")

        # Wait for the log file to exist
        while not os.path.exists(self.log_path):
            print(f"   Waiting for {self.log_path} to be created...")
            time.sleep(1)

        with open(self.log_path, "r", encoding="utf-8") as fh:
            # Seek to end so we only process new lines
            fh.seek(0, 2)
            print("   [*] Tailing log file from current position...\n")

            try:
                while True:
                    line = fh.readline()
                    if not line:
                        time.sleep(0.1)       # brief sleep to avoid busy-wait
                        continue

                    line = line.strip()
                    if not line:
                        continue

                    try:
                        txn = json.loads(line)
                    except json.JSONDecodeError:
                        continue              # skip malformed lines

                    # Add to rolling window
                    self.window.append(txn)

                    # Print terminal blip
                    amt = txn["amount_usd"]
                    marker = "  🔸" if amt < MICRO_THRESHOLD_USD else "  ·"
                    print(
                        f"{marker}  {txn['account_id']}  "
                        f"${amt:>8.2f}  "
                        f"{txn['location_country']:<10}  "
                        f"{txn['merchant_category']}"
                    )

                    # Run detection
                    self._check_card_testing(txn)

            except KeyboardInterrupt:
                print(f"\n[*] Consumer stopped. {len(self.window)} txns in window.")


if __name__ == "__main__":
    LiveConsumer().run()
