"""
data_generator.py
-----------------
Generates synthetic payments & settlements datasets for reconciliation testing.

ASSUMPTIONS:
  - Platform records transactions at the moment of customer payment (real-time).
  - Bank batches and settles 1-2 business days later (T+1 or T+2).
  - All amounts are in INR (Indian Rupee) — trivially swappable.
  - transaction_ids are globally unique UUIDs on the platform side.
  - Settlements reference transaction_ids, but bank IDs are their own sequence.
  - Refund amounts are stored as negative values.
  - "Rounding difference" is introduced via float imprecision during aggregation,
    not on individual row amounts — this reflects real-world FX conversion or
    sub-paisa truncation scenarios.
  - Month boundary used: January 2024 → February 2024.
"""

import uuid
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

# ── helpers ──────────────────────────────────────────────────────────────────

def _txn_id():
    return "TXN-" + str(uuid.uuid4())[:8].upper()

def _settle_id(n):
    return f"SET-{n:05d}"

def _rand_amount(lo=100, hi=50_000):
    """Round to 2 decimal places — mimics payment gateway precision."""
    return round(random.uniform(lo, hi), 2)

def _jan_ts(day: int, hour: int = 12) -> datetime:
    return datetime(2024, 1, day, hour, random.randint(0, 59), random.randint(0, 59))

def _feb_ts(day: int) -> datetime:
    return datetime(2024, 2, day, random.randint(8, 18), random.randint(0, 59))

# ── main generator ────────────────────────────────────────────────────────────

def generate_datasets(n_normal: int = 20):
    """
    Returns (transactions_df, settlements_df) with four injected anomalies.

    Anomaly map
    -----------
    A  CROSS_MONTH      — transaction on Jan 30, settled Feb 2
    B  ROUNDING_DIFF    — sum of float amounts differs by ~0.01 due to precision
    C  DUPLICATE_TXN    — same transaction appears twice in transactions table
    D  ORPHAN_REFUND    — refund in settlements with no matching original txn
    """

    transactions = []
    settlements  = []
    settle_ctr   = 1

    # ── normal matched transactions (Jan 1-28) ────────────────────────────────
    for i in range(n_normal):
        tid    = _txn_id()
        amount = _rand_amount()
        txn_ts = _jan_ts(day=random.randint(1, 28), hour=random.randint(8, 20))
        # settled 1-2 days later, still in January
        settle_date = (txn_ts + timedelta(days=random.randint(1, 2))).date()

        transactions.append({
            "transaction_id": tid,
            "user_id":        f"USR-{random.randint(1000, 9999)}",
            "amount":         amount,
            "timestamp":      txn_ts,
            "status":         "COMPLETED",
            "anomaly_tag":    None,
        })
        settlements.append({
            "settlement_id":  _settle_id(settle_ctr),
            "transaction_id": tid,
            "amount":         amount,
            "settlement_date": settle_date,
            "anomaly_tag":    None,
        })
        settle_ctr += 1

    # ── ANOMALY A: cross-month settlement ─────────────────────────────────────
    tid_a  = _txn_id()
    amt_a  = _rand_amount(500, 10_000)
    transactions.append({
        "transaction_id": tid_a,
        "user_id":        "USR-8001",
        "amount":         amt_a,
        "timestamp":      _jan_ts(day=30, hour=23),   # last day of Jan
        "status":         "COMPLETED",
        "anomaly_tag":    "CROSS_MONTH",
    })
    settlements.append({
        "settlement_id":  _settle_id(settle_ctr),
        "transaction_id": tid_a,
        "amount":         amt_a,
        "settlement_date": _feb_ts(day=2).date(),      # settled in Feb
        "anomaly_tag":    "CROSS_MONTH",
    })
    settle_ctr += 1

    # ── ANOMALY B: rounding difference ────────────────────────────────────────
    # Transaction stores full float; settlement stores a value with
    # a sub-paisa difference that only surfaces when you sum both sides.
    tid_b  = _txn_id()
    amt_b_txn    = 1234.565           # platform records
    amt_b_settle = 1234.56            # bank truncates (not rounds)
    transactions.append({
        "transaction_id": tid_b,
        "user_id":        "USR-8002",
        "amount":         amt_b_txn,
        "timestamp":      _jan_ts(day=15),
        "status":         "COMPLETED",
        "anomaly_tag":    "ROUNDING_DIFF",
    })
    settlements.append({
        "settlement_id":  _settle_id(settle_ctr),
        "transaction_id": tid_b,
        "amount":         amt_b_settle,
        "settlement_date": _jan_ts(day=16).date(),
        "anomaly_tag":    "ROUNDING_DIFF",
    })
    settle_ctr += 1

    # ── ANOMALY C: duplicate transaction in platform records ──────────────────
    tid_c  = _txn_id()
    amt_c  = _rand_amount(200, 5_000)
    txn_ts_c = _jan_ts(day=10, hour=14)
    for tag in ("DUPLICATE_ORIGINAL", "DUPLICATE_COPY"):
        transactions.append({
            "transaction_id": tid_c,          # same id — double POST
            "user_id":        "USR-8003",
            "amount":         amt_c,
            "timestamp":      txn_ts_c,
            "status":         "COMPLETED",
            "anomaly_tag":    tag,
        })
    # Only ONE settlement for this transaction
    settlements.append({
        "settlement_id":  _settle_id(settle_ctr),
        "transaction_id": tid_c,
        "amount":         amt_c,
        "settlement_date": (txn_ts_c + timedelta(days=1)).date(),
        "anomaly_tag":    "DUPLICATE_ORIGINAL",
    })
    settle_ctr += 1

    # ── ANOMALY D: orphan refund ───────────────────────────────────────────────
    # Bank shows a refund settlement; no original transaction in platform records
    settlements.append({
        "settlement_id":  _settle_id(settle_ctr),
        "transaction_id": "TXN-ORPHAN1",      # does not exist in transactions
        "amount":         -2500.00,            # negative = refund
        "settlement_date": datetime(2024, 1, 20).date(),
        "anomaly_tag":    "ORPHAN_REFUND",
    })
    settle_ctr += 1

    txn_df     = pd.DataFrame(transactions)
    settle_df  = pd.DataFrame(settlements)

    # tidy dtypes
    txn_df["timestamp"]       = pd.to_datetime(txn_df["timestamp"])
    txn_df["month"]           = txn_df["timestamp"].dt.to_period("M")
    settle_df["settlement_date"] = pd.to_datetime(settle_df["settlement_date"])
    settle_df["settle_month"] = settle_df["settlement_date"].dt.to_period("M")

    return txn_df, settle_df


if __name__ == "__main__":
    t, s = generate_datasets()
    print("=== Transactions ===")
    print(t[["transaction_id","amount","timestamp","anomaly_tag"]].to_string())
    print("\n=== Settlements ===")
    print(s[["settlement_id","transaction_id","amount","settlement_date","anomaly_tag"]].to_string())
