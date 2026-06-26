"""
FinTech-Fraud-Engine — Live Transaction Producer
=================================================
Runs as a continuous process, emitting one financial transaction every
0.5–2.0 seconds to ``live_transactions.log`` (JSON-Lines format).
With a 5 % probability each cycle, a Card Testing burst is injected.

Usage:
    python generator.py          # runs indefinitely — Ctrl+C to stop
"""

import json
import uuid
import random
import sys
import time
import os
from datetime import datetime
from typing import Dict, Any

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

LOG_FILE = "live_transactions.log"


class LiveProducer:
    """Continuously emits financial transactions to a log file."""

    ACCOUNTS = [f"ACCT-{i:04d}" for i in range(1, 26)]
    CATEGORIES = [
        "Groceries", "Electronics", "Travel", "Dining",
        "Fuel", "Subscription", "Healthcare", "Retail",
    ]
    COUNTRIES = ["USA", "UK", "Germany", "Canada", "France", "Japan", "Australia"]

    def __init__(self, log_path: str = LOG_FILE) -> None:
        self.log_path = log_path
        # Create or truncate the file so the consumer can start cleanly
        open(self.log_path, "a").close()
        self._txn_count = 0

    # ------------------------------------------------------------------ #
    #  Helpers                                                            #
    # ------------------------------------------------------------------ #
    @staticmethod
    def _txn_id() -> str:
        return f"TXN-{uuid.uuid4().hex[:12].upper()}"

    def _build_txn(
        self,
        account_id: str | None = None,
        amount: float | None = None,
        country: str | None = None,
    ) -> Dict[str, Any]:
        """Build a single transaction dict."""
        return {
            "transaction_id": self._txn_id(),
            "account_id": account_id or random.choice(self.ACCOUNTS),
            "timestamp": datetime.now().isoformat(),
            "amount_usd": amount or round(random.uniform(5.0, 500.0), 2),
            "merchant_category": random.choice(self.CATEGORIES),
            "location_country": country or random.choice(self.COUNTRIES),
            "status": "approved",
        }

    def _append(self, txn: Dict[str, Any]) -> None:
        """Append a single JSON line to the log file."""
        with open(self.log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(txn) + "\n")
        self._txn_count += 1

    # ------------------------------------------------------------------ #
    #  Normal transaction                                                 #
    # ------------------------------------------------------------------ #
    def emit_normal(self) -> None:
        """Emit one normal transaction and print a terminal blip."""
        txn = self._build_txn()
        self._append(txn)
        print(
            f"  [{self._txn_count:>5}]  "
            f"{txn['account_id']}  "
            f"${txn['amount_usd']:>8.2f}  "
            f"{txn['location_country']:<10}  "
            f"{txn['merchant_category']}"
        )

    # ------------------------------------------------------------------ #
    #  Card Testing burst                                                 #
    # ------------------------------------------------------------------ #
    def emit_card_testing_burst(self) -> None:
        """
        Inject a Card Testing attack: 10 rapid micro-transactions
        (< $2.00) followed by one large purchase (> $1 000).
        """
        attacker = f"ACCT-FRAUD-{random.randint(100, 999)}"
        print(f"\n  ⚡ INJECTING Card Testing burst on {attacker} ⚡")

        for i in range(10):
            txn = self._build_txn(
                account_id=attacker,
                amount=round(random.uniform(0.10, 1.99), 2),
                country="USA",
            )
            self._append(txn)
            print(
                f"  [{self._txn_count:>5}]  "
                f"{txn['account_id']}  "
                f"${txn['amount_usd']:>8.2f}  "
                f"{'USA':<10}  micro-txn {i + 1}/10"
            )
            time.sleep(random.uniform(0.05, 0.15))

        # Large follow-up
        big = self._build_txn(
            account_id=attacker,
            amount=round(random.uniform(1_200.0, 5_000.0), 2),
            country="USA",
        )
        self._append(big)
        print(
            f"  [{self._txn_count:>5}]  "
            f"{big['account_id']}  "
            f"${big['amount_usd']:>8.2f}  "
            f"{'USA':<10}  ** BIG PURCHASE **\n"
        )

    # ------------------------------------------------------------------ #
    #  Main loop                                                          #
    # ------------------------------------------------------------------ #
    def run(self) -> None:
        """Run the producer indefinitely."""
        print("=" * 65)
        print("   FinTech-Fraud-Engine  ·  Live Producer  ·  STREAMING")
        print("=" * 65)
        print(f"   Writing to: {os.path.abspath(self.log_path)}")
        print("   Press Ctrl+C to stop.\n")

        try:
            while True:
                # 5 % chance of a Card Testing burst
                if random.random() < 0.05:
                    self.emit_card_testing_burst()
                else:
                    self.emit_normal()

                time.sleep(random.uniform(0.5, 2.0))
        except KeyboardInterrupt:
            print(f"\n[*] Producer stopped. {self._txn_count} transactions emitted.")


if __name__ == "__main__":
    LiveProducer().run()
