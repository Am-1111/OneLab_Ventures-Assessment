"""
report.py
---------
Generates a structured plain-text / DataFrame reconciliation report.
Designed to be importable by a Streamlit app or REST API handler.
"""

from __future__ import annotations
import textwrap
import pandas as pd


def _divider(char="─", width=72):
    return char * width


def print_report(results: dict, recon_month: str = "2024-01") -> None:
    """
    Prints a human-readable reconciliation report to stdout.
    In a web deployment this would be serialised to JSON / HTML instead.
    """
    s   = results["summary"]
    matched     = results["matched"]
    unmatched   = results["unmatched_txn"]
    orphans     = results["orphan_settlements"]
    duplicates  = results["duplicates"]

    # ── Header ───────────────────────────────────────────────────────────────
    print(_divider("═"))
    print(f"  PAYMENT RECONCILIATION REPORT  |  Period: {recon_month}")
    print(_divider("═"))

    # ── Summary metrics ──────────────────────────────────────────────────────
    print("\n📊  SUMMARY METRICS")
    print(_divider())
    metrics = {
        "Total platform transactions":       s["total_platform_transactions"],
        "Duplicate transaction IDs found":   s["duplicate_transaction_ids"],
        "Matched — clean (OK)":              s["matched_clean"],
        "Matched — rounding diff":           s["matched_rounding_diff"],
        "Matched — amount mismatch":         s["matched_amount_mismatch"],
        "Matched — cross-month timing":      s["matched_cross_month"],
        "Unmatched transactions (no settle)":s["unmatched_transactions"],
        "Orphan refunds (no source txn)":    s["orphan_refunds"],
        "Settlements without platform txn":  s["missing_txn_for_settlement"],
    }
    for k, v in metrics.items():
        flag = " ⚠️" if v > 0 and k != "Total platform transactions" and k != "Matched — clean (OK)" else ""
        print(f"  {k:<44} {v:>5}{flag}")

    # ── Cross-month mismatches ───────────────────────────────────────────────
    cross = matched[matched["timing_status"] == "CROSS_MONTH"]
    if not cross.empty:
        print(f"\n\n🗓️  CROSS-MONTH SETTLEMENTS  ({len(cross)} found)")
        print(_divider())
        print(textwrap.dedent("""
        These transactions were processed in the reconciliation month but their
        bank settlement arrived in a later month. They appear as open items in
        the current month's books but are NOT true discrepancies — they will
        resolve in the next cycle. Typical cause: transactions processed near
        month-end with T+2 settlement lag.
        """).strip())
        cols = ["transaction_id","amount_txn","timestamp","settlement_date","settle_month"]
        print("\n" + cross[cols].to_string(index=False))

    # ── Rounding differences ─────────────────────────────────────────────────
    rounding = matched[matched["amount_status"] == "ROUNDING_DIFF"]
    if not rounding.empty:
        print(f"\n\n🔢  ROUNDING DISCREPANCIES  ({len(rounding)} found)")
        print(_divider())
        print(textwrap.dedent("""
        Transaction amount and settled amount differ by a sub-paisa value.
        Invisible on individual rows but surfaces as a balance mismatch when
        totals are summed. Common cause: FX conversion truncation vs. rounding,
        or payment gateway applying a different precision rule than the bank.
        """).strip())
        cols = ["transaction_id","amount_txn","amount_settle","amount_diff"]
        print("\n" + rounding[cols].to_string(index=False))

    # ── Duplicates ───────────────────────────────────────────────────────────
    if not duplicates.empty:
        print(f"\n\n♊  DUPLICATE TRANSACTIONS  ({len(duplicates)} rows)")
        print(_divider())
        print(textwrap.dedent("""
        The same transaction_id appears more than once in the platform records.
        Likely cause: double POST from a retry mechanism or webhook replay.
        The first occurrence is used for reconciliation; excess rows are
        quarantined. Left undetected, this would inflate gross transaction
        volume and create phantom settlement gaps.
        """).strip())
        cols = ["transaction_id","user_id","amount","timestamp","anomaly_tag"]
        print("\n" + duplicates[cols].to_string(index=False))

    # ── Orphan refunds ───────────────────────────────────────────────────────
    refunds = orphans[orphans["issue"] == "ORPHAN_REFUND"]
    if not refunds.empty:
        print(f"\n\n↩️   ORPHAN REFUNDS  ({len(refunds)} found)")
        print(_divider())
        print(textwrap.dedent("""
        The bank settled a negative amount (refund) referencing a transaction_id
        that does not exist in platform records. Possible causes:
          • Refund was initiated outside the platform (manual bank reversal).
          • Source transaction was soft-deleted from platform DB.
          • transaction_id was mis-typed during manual refund entry.
        Requires manual investigation before month-end close.
        """).strip())
        cols = ["settlement_id","transaction_id","amount_settle","settlement_date","issue"]
        print("\n" + refunds[cols].to_string(index=False))

    # ── Unmatched transactions (open items) ──────────────────────────────────
    if not unmatched.empty:
        print(f"\n\n❌  UNMATCHED TRANSACTIONS — NO SETTLEMENT  ({len(unmatched)} found)")
        print(_divider())
        print(textwrap.dedent("""
        Platform recorded a transaction but no bank settlement arrived within
        the allowed lag window. Could indicate:
          • Payment failed after platform recorded it (gateway timeout).
          • Settlement delayed beyond the expected T+2 window.
          • Bank file was incomplete or mis-transmitted.
        """).strip())
        cols = ["transaction_id","amount_txn","timestamp"]
        print("\n" + unmatched[cols].to_string(index=False))

    # ── Balance check ─────────────────────────────────────────────────────────
    print(f"\n\n💰  BALANCE RECONCILIATION")
    print(_divider())
    total_txn_amt    = matched["amount_txn"].sum()
    total_settle_amt = matched["amount_settle"].sum()
    delta            = round(total_txn_amt - total_settle_amt, 4)
    print(f"  Sum of matched transaction amounts:   ₹{total_txn_amt:>14,.4f}")
    print(f"  Sum of matched settlement amounts:    ₹{total_settle_amt:>14,.4f}")
    print(f"  Net difference:                       ₹{delta:>14,.4f}  {'✅ Balanced' if abs(delta) < 0.01 else '⚠️  OUT OF BALANCE'}")

    print(f"\n{_divider('═')}")
    print("  END OF REPORT")
    print(_divider("═"))


def export_report_df(results: dict) -> pd.DataFrame:
    """
    Returns a single DataFrame with all issues in a normalised schema —
    useful for feeding into a dashboard, database, or CSV export.
    """
    rows = []

    # Rounding diffs
    for _, r in results["matched"][results["matched"]["amount_status"] == "ROUNDING_DIFF"].iterrows():
        rows.append({
            "issue_type":     "ROUNDING_DIFF",
            "transaction_id": r["transaction_id"],
            "settlement_id":  r.get("settlement_id",""),
            "txn_amount":     r["amount_txn"],
            "settle_amount":  r["amount_settle"],
            "delta":          r["amount_diff"],
            "txn_date":       r["timestamp"],
            "settle_date":    r["settlement_date"],
            "description":    f"Amount mismatch of ₹{r['amount_diff']:.4f}",
        })

    # Cross-month
    for _, r in results["matched"][results["matched"]["timing_status"] == "CROSS_MONTH"].iterrows():
        rows.append({
            "issue_type":     "CROSS_MONTH",
            "transaction_id": r["transaction_id"],
            "settlement_id":  r.get("settlement_id",""),
            "txn_amount":     r["amount_txn"],
            "settle_amount":  r["amount_settle"],
            "delta":          0.0,
            "txn_date":       r["timestamp"],
            "settle_date":    r["settlement_date"],
            "description":    f"Settled in {r['settle_month']}, expected {r['month']}",
        })

    # Duplicates
    for _, r in results["duplicates"].iterrows():
        rows.append({
            "issue_type":     "DUPLICATE_TRANSACTION",
            "transaction_id": r["transaction_id"],
            "settlement_id":  "",
            "txn_amount":     r["amount"],
            "settle_amount":  None,
            "delta":          None,
            "txn_date":       r["timestamp"],
            "settle_date":    None,
            "description":    "Duplicate transaction_id in platform records",
        })

    # Orphan refunds
    for _, r in results["orphan_settlements"].iterrows():
        rows.append({
            "issue_type":     r["issue"],
            "transaction_id": r.get("transaction_id",""),
            "settlement_id":  r.get("settlement_id",""),
            "txn_amount":     None,
            "settle_amount":  r["amount_settle"],
            "delta":          None,
            "txn_date":       None,
            "settle_date":    r["settlement_date"],
            "description":    f"Settlement with no platform transaction ({r['issue']})",
        })

    return pd.DataFrame(rows)
