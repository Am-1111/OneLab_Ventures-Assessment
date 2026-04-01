"""
test_reconciler.py
------------------
Test cases validating all four anomaly types and core reconciliation logic.
Written to run with pytest OR with the built-in unittest runner.

Run:  python test_reconciler.py
"""

import unittest
import pandas as pd
from datetime import datetime, date

# import from our modules
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from data_generator import generate_datasets
from reconciler import (
    detect_duplicates,
    match_records,
    classify_matched,
    flag_orphan_refunds,
    build_summary,
    run_reconciliation,
    AMOUNT_TOLERANCE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_txn(tid, amount, ts, user="USR-001"):
    return {
        "transaction_id": tid,
        "user_id": user,
        "amount": amount,
        "timestamp": pd.Timestamp(ts),
        "status": "COMPLETED",
        "anomaly_tag": None,
    }

def _make_settle(sid, tid, amount, settle_date):
    return {
        "settlement_id": sid,
        "transaction_id": tid,
        "amount": amount,
        "settlement_date": pd.Timestamp(settle_date),
        "anomaly_tag": None,
    }

def _df_txn(rows):
    if not rows:
        return pd.DataFrame(columns=["transaction_id","user_id","amount","timestamp","status","anomaly_tag","month"])
    df = pd.DataFrame(rows)
    df["month"] = df["timestamp"].dt.to_period("M")
    return df

def _df_settle(rows):
    df = pd.DataFrame(rows)
    df["settle_month"] = df["settlement_date"].dt.to_period("M")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Test classes
# ─────────────────────────────────────────────────────────────────────────────

class TestDuplicateDetection(unittest.TestCase):

    def test_no_duplicates(self):
        txns = _df_txn([
            _make_txn("T001", 100.0, "2024-01-05 10:00"),
            _make_txn("T002", 200.0, "2024-01-06 11:00"),
        ])
        clean, dupes = detect_duplicates(txns)
        self.assertEqual(len(dupes), 0)
        self.assertEqual(len(clean), 2)

    def test_single_duplicate_pair(self):
        txns = _df_txn([
            _make_txn("T001", 100.0, "2024-01-05 10:00"),
            _make_txn("T001", 100.0, "2024-01-05 10:00"),   # duplicate
            _make_txn("T002", 200.0, "2024-01-06 11:00"),
        ])
        clean, dupes = detect_duplicates(txns)
        # Both rows with T001 are flagged
        self.assertEqual(len(dupes), 2)
        # Clean set keeps first occurrence only
        self.assertEqual(len(clean), 2)
        self.assertIn("T001", clean["transaction_id"].values)
        self.assertIn("T002", clean["transaction_id"].values)

    def test_triple_duplicate(self):
        txns = _df_txn([_make_txn("T001", 50.0, "2024-01-01 09:00")] * 3)
        clean, dupes = detect_duplicates(txns)
        self.assertEqual(len(dupes), 3)
        self.assertEqual(len(clean), 1)


class TestMissingSettlement(unittest.TestCase):

    def test_transaction_with_no_settlement(self):
        txns = _df_txn([
            _make_txn("T001", 500.0, "2024-01-10 09:00"),
            _make_txn("T002", 300.0, "2024-01-11 09:00"),   # no settlement
        ])
        settles = _df_settle([
            _make_settle("S001", "T001", 500.0, "2024-01-11"),
        ])
        _, unmatched, _ = match_records(txns, settles, "2024-01")
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched.iloc[0]["transaction_id"], "T002")

    def test_all_transactions_settled(self):
        txns = _df_txn([_make_txn("T001", 500.0, "2024-01-10 09:00")])
        settles = _df_settle([_make_settle("S001","T001",500.0,"2024-01-11")])
        _, unmatched, _ = match_records(txns, settles, "2024-01")
        self.assertEqual(len(unmatched), 0)


class TestRoundingMismatch(unittest.TestCase):

    def test_exact_match_is_ok(self):
        txns = _df_txn([_make_txn("T001", 1234.56, "2024-01-05 10:00")])
        settles = _df_settle([_make_settle("S001","T001",1234.56,"2024-01-06")])
        matched, _, _ = match_records(txns, settles, "2024-01")
        classified = classify_matched(matched)
        self.assertEqual(classified.iloc[0]["amount_status"], "OK")

    def test_sub_penny_diff_is_rounding(self):
        txns = _df_txn([_make_txn("T001", 1234.565, "2024-01-05 10:00")])
        settles = _df_settle([_make_settle("S001","T001",1234.56,"2024-01-06")])
        matched, _, _ = match_records(txns, settles, "2024-01")
        classified = classify_matched(matched)
        self.assertEqual(classified.iloc[0]["amount_status"], "ROUNDING_DIFF")

    def test_large_diff_is_mismatch(self):
        txns = _df_txn([_make_txn("T001", 1234.56, "2024-01-05 10:00")])
        settles = _df_settle([_make_settle("S001","T001",1200.00,"2024-01-06")])
        matched, _, _ = match_records(txns, settles, "2024-01")
        classified = classify_matched(matched)
        self.assertEqual(classified.iloc[0]["amount_status"], "AMOUNT_MISMATCH")

    def test_tolerance_boundary(self):
        """Amount diff of exactly AMOUNT_TOLERANCE should still be OK."""
        txns = _df_txn([_make_txn("T001", 100.00 + AMOUNT_TOLERANCE, "2024-01-05 10:00")])
        settles = _df_settle([_make_settle("S001","T001",100.00,"2024-01-06")])
        matched, _, _ = match_records(txns, settles, "2024-01")
        classified = classify_matched(matched)
        # Diff = AMOUNT_TOLERANCE exactly → should be OK (boundary is inclusive)
        self.assertIn(classified.iloc[0]["amount_status"], ("OK", "ROUNDING_DIFF"))


class TestCrossMonthSettlement(unittest.TestCase):

    def test_same_month_is_on_time(self):
        txns = _df_txn([_make_txn("T001", 500.0, "2024-01-20 10:00")])
        settles = _df_settle([_make_settle("S001","T001",500.0,"2024-01-22")])
        matched, _, _ = match_records(txns, settles, "2024-01")
        classified = classify_matched(matched)
        self.assertEqual(classified.iloc[0]["timing_status"], "ON_TIME")

    def test_next_month_settlement_is_cross_month(self):
        txns = _df_txn([_make_txn("T001", 500.0, "2024-01-30 22:00")])
        settles = _df_settle([_make_settle("S001","T001",500.0,"2024-02-02")])
        matched, _, _ = match_records(txns, settles, "2024-01")
        classified = classify_matched(matched)
        self.assertEqual(classified.iloc[0]["timing_status"], "CROSS_MONTH")

    def test_settlement_outside_lag_window_is_unmatched(self):
        """Settlements arriving > SETTLE_LAG_DAYS after month-end are excluded."""
        from reconciler import SETTLE_LAG_DAYS
        txns = _df_txn([_make_txn("T001", 500.0, "2024-01-15 10:00")])
        # settlement far in the future — beyond lag window
        settles = _df_settle([_make_settle("S001","T001",500.0,"2024-03-01")])
        _, unmatched, _ = match_records(txns, settles, "2024-01")
        self.assertEqual(len(unmatched), 1)


class TestOrphanRefund(unittest.TestCase):

    def test_negative_settlement_without_txn_is_orphan_refund(self):
        txns    = _df_txn([_make_txn("T001", 500.0, "2024-01-10 09:00")])
        settles = _df_settle([
            _make_settle("S001", "T001",        500.0, "2024-01-11"),
            _make_settle("S002", "TXN-MISSING", -250.0, "2024-01-12"),  # orphan refund
        ])
        _, _, orphans = match_records(txns, settles, "2024-01")
        flagged = flag_orphan_refunds(orphans)
        self.assertEqual(len(flagged[flagged["issue"] == "ORPHAN_REFUND"]), 1)
        self.assertEqual(flagged.iloc[0]["transaction_id"], "TXN-MISSING")

    def test_positive_orphan_settlement_is_missing_txn(self):
        txns    = _df_txn([])   # empty
        settles = _df_settle([
            _make_settle("S001", "TXN-GHOST", 999.0, "2024-01-05"),  # no matching txn
        ])
        _, _, orphans = match_records(txns, settles, "2024-01")
        flagged = flag_orphan_refunds(orphans)
        self.assertEqual(flagged.iloc[0]["issue"], "MISSING_TRANSACTION")


class TestEndToEndWithGeneratedData(unittest.TestCase):
    """
    Runs the full pipeline on the generated dataset and asserts
    all four injected anomalies are detected.
    """

    @classmethod
    def setUpClass(cls):
        txns, settles = generate_datasets(n_normal=20)
        cls.results = run_reconciliation(txns, settles, "2024-01")

    def test_duplicate_detected(self):
        self.assertGreaterEqual(self.results["summary"]["duplicate_transaction_ids"], 1)

    def test_cross_month_detected(self):
        self.assertGreaterEqual(self.results["summary"]["matched_cross_month"], 1)

    def test_rounding_diff_detected(self):
        self.assertGreaterEqual(self.results["summary"]["matched_rounding_diff"], 1)

    def test_orphan_refund_detected(self):
        self.assertGreaterEqual(self.results["summary"]["orphan_refunds"], 1)

    def test_all_normal_transactions_matched(self):
        # There should be plenty of cleanly-matched records
        self.assertGreaterEqual(self.results["summary"]["matched_clean"], 15)


# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    loader  = unittest.TestLoader()
    suite   = loader.discover(start_dir=".", pattern="test_*.py")
    runner  = unittest.TextTestRunner(verbosity=2)
    result  = runner.run(suite)
    exit(0 if result.wasSuccessful() else 1)
