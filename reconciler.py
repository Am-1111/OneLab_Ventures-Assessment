"""
reconciler.py
-------------
Core reconciliation engine.

Design principles
-----------------
  - Stateless functions — each step can be tested in isolation.
  - Tolerance-aware matching — amounts compared within AMOUNT_TOLERANCE.
  - Cross-month awareness — settlements up to SETTLE_LAG_DAYS after month-end
    are flagged as timing mismatches rather than hard failures.
  - All outputs are DataFrames so they compose easily into a report or API.
"""

from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Tuple

# ── constants ────────────────────────────────────────────────────────────────

AMOUNT_TOLERANCE  = 0.004  # paise / cent — diffs ≤ this are truly negligible
                            # (0.005 is a meaningful rounding error, not noise)
SETTLE_LAG_DAYS   = 5      # settlements arriving this many days after month-end
                            # are cross-month timing mismatches, not true gaps

ROUNDING_THRESHOLD = 0.05  # if matched amounts differ by ≤ this → rounding error

# ── step 1: detect duplicates ─────────────────────────────────────────────────

def detect_duplicates(transactions: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (clean_transactions, duplicates_df).
    A duplicate = same transaction_id appearing more than once.
    We keep the FIRST occurrence as canonical.
    """
    dup_mask = transactions.duplicated(subset=["transaction_id"], keep=False)
    duplicates = transactions[dup_mask].copy()
    duplicates["issue"] = "DUPLICATE_TRANSACTION_ID"

    # For reconciliation, use de-duplicated set
    clean = transactions.drop_duplicates(subset=["transaction_id"], keep="first").copy()
    return clean, duplicates


# ── step 2: match transactions ↔ settlements ─────────────────────────────────

def match_records(
    transactions: pd.DataFrame,
    settlements:  pd.DataFrame,
    recon_month:  str = "2024-01",        # period string e.g. "2024-01"
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Performs a left-join of transactions → settlements on transaction_id.

    Returns
    -------
    matched_df       — rows that joined successfully
    unmatched_txn_df — transactions with no settlement (within recon_month + lag)
    orphan_settle_df — settlements with no matching transaction
    """
    period = pd.Period(recon_month, freq="M")

    # Restrict to transactions in the reconciliation month
    txn_month = transactions[transactions["month"] == period].copy()

    # Settlements allowed: same month OR up to SETTLE_LAG_DAYS into next month
    month_end  = period.to_timestamp(how="end")
    lag_cutoff = month_end + pd.Timedelta(days=SETTLE_LAG_DAYS)
    settle_window = settlements[settlements["settlement_date"] <= lag_cutoff].copy()

    # Merge
    merged = txn_month.merge(
        settle_window[["settlement_id","transaction_id","amount","settlement_date","settle_month"]],
        on="transaction_id",
        how="outer",
        suffixes=("_txn", "_settle"),
        indicator=True,
    )

    matched      = merged[merged["_merge"] == "both"].copy()
    unmatched_t  = merged[merged["_merge"] == "left_only"].copy()   # txn, no settlement
    orphan_s     = merged[merged["_merge"] == "right_only"].copy()  # settlement, no txn

    return matched, unmatched_t, orphan_s


# ── step 3: classify matched records ─────────────────────────────────────────

def classify_matched(matched: pd.DataFrame) -> pd.DataFrame:
    """
    Adds columns to matched_df:
      amount_diff       — txn amount minus settle amount
      amount_status     — OK | ROUNDING_DIFF | AMOUNT_MISMATCH
      timing_status     — ON_TIME | CROSS_MONTH
    """
    df = matched.copy()

    df["amount_diff"] = (df["amount_txn"] - df["amount_settle"]).round(4)
    abs_diff = df["amount_diff"].abs()

    df["amount_status"] = np.where(
        abs_diff <= AMOUNT_TOLERANCE,  "OK",
        np.where(
            abs_diff <= ROUNDING_THRESHOLD, "ROUNDING_DIFF",
            "AMOUNT_MISMATCH"
        )
    )

    # timing: compare the transaction's month vs the settlement's month
    df["timing_status"] = np.where(
        df["month"] == df["settle_month"], "ON_TIME", "CROSS_MONTH"
    )

    return df


# ── step 4: identify orphan refunds ──────────────────────────────────────────

def flag_orphan_refunds(orphan_settlements: pd.DataFrame) -> pd.DataFrame:
    """
    From orphan settlements, isolate negative-amount rows → orphan refunds.
    Positive-amount orphans are also captured as MISSING_TRANSACTION.
    """
    df = orphan_settlements.copy()
    df["issue"] = np.where(
        df["amount_settle"] < 0, "ORPHAN_REFUND", "MISSING_TRANSACTION"
    )
    return df


# ── step 5: aggregate summary ─────────────────────────────────────────────────

def build_summary(
    original_txn:    pd.DataFrame,
    duplicates:      pd.DataFrame,
    matched:         pd.DataFrame,
    unmatched_txn:   pd.DataFrame,
    orphan_settle:   pd.DataFrame,
) -> dict:
    """Returns a flat dict of KPIs for the reconciliation report."""

    classified = classify_matched(matched)
    orphan_flagged = flag_orphan_refunds(orphan_settle)

    total_txn         = len(original_txn)
    dup_count         = len(duplicates) - len(duplicates["transaction_id"].unique())
    matched_ok        = (classified["amount_status"] == "OK").sum()
    rounding_issues   = (classified["amount_status"] == "ROUNDING_DIFF").sum()
    amount_mismatches = (classified["amount_status"] == "AMOUNT_MISMATCH").sum()
    cross_month       = (classified["timing_status"] == "CROSS_MONTH").sum()
    unmatched_count   = len(unmatched_txn)
    orphan_refunds    = (orphan_flagged["issue"] == "ORPHAN_REFUND").sum()
    missing_txn       = (orphan_flagged["issue"] == "MISSING_TRANSACTION").sum()

    return {
        "total_platform_transactions":  total_txn,
        "duplicate_transaction_ids":    dup_count,
        "matched_clean":                int(matched_ok),
        "matched_rounding_diff":        int(rounding_issues),
        "matched_amount_mismatch":      int(amount_mismatches),
        "matched_cross_month":          int(cross_month),
        "unmatched_transactions":       int(unmatched_count),
        "orphan_refunds":               int(orphan_refunds),
        "missing_txn_for_settlement":   int(missing_txn),
    }


# ── master runner ─────────────────────────────────────────────────────────────

def run_reconciliation(
    transactions: pd.DataFrame,
    settlements:  pd.DataFrame,
    recon_month:  str = "2024-01",
):
    """
    End-to-end reconciliation. Returns a results dict with all artefacts.
    """
    # 1. Deduplicate
    clean_txn, duplicates = detect_duplicates(transactions)

    # 2. Match
    matched, unmatched_txn, orphan_settle = match_records(
        clean_txn, settlements, recon_month
    )

    # 3. Classify
    matched_classified  = classify_matched(matched)
    orphan_classified   = flag_orphan_refunds(orphan_settle)

    # 4. Summary
    summary = build_summary(
        original_txn  = transactions,
        duplicates    = duplicates,
        matched       = matched_classified,
        unmatched_txn = unmatched_txn,
        orphan_settle = orphan_classified,
    )

    return {
        "summary":           summary,
        "matched":           matched_classified,
        "unmatched_txn":     unmatched_txn,
        "orphan_settlements":orphan_classified,
        "duplicates":        duplicates,
    }
