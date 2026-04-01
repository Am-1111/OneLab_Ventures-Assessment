"""
main.py
-------
Payments Reconciliation System — OneLab Ventures AI Assessment
Author: Amey Mule  |  muleamey13@gmail.com

Runs as a Streamlit web app (primary) or CLI (fallback).

Usage:
    streamlit run main.py        # launch web dashboard
    python main.py               # CLI pipeline
    python main.py --csv         # CLI + export issues.csv
"""

import os
import sys
import argparse

from data_generator import generate_datasets
from reconciler     import run_reconciliation
from report         import print_report, export_report_df

RECON_MONTH = "2024-01"
OUTPUT_DIR  = os.path.dirname(__file__)

# ── Streamlit Dashboard ───────────────────────────────────────────────────────
try:
    import streamlit as st
    _STREAMLIT = True
except ImportError:
    _STREAMLIT = False

if _STREAMLIT:
    import pandas as pd

    st.set_page_config(
        page_title="Payments Reconciliation | OneLab",
        page_icon="💳",
        layout="wide",
    )

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(
        """
        <div style='background:#0F2B5B;padding:24px 32px;border-radius:10px;margin-bottom:24px'>
          <h1 style='color:#00BFA6;margin:0;font-size:28px'>💳 Payments Reconciliation System</h1>
          <p style='color:#A0AEC0;margin:6px 0 0'>OneLab Ventures — AI Fitness Assessment &nbsp;|&nbsp;
             <b style='color:white'>Amey Mule</b> &nbsp;|&nbsp; muleamey13@gmail.com</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Sidebar controls ──────────────────────────────────────────────────────
    with st.sidebar:
        st.header("⚙️ Controls")
        month = st.selectbox("Reconciliation Month", ["2024-01", "2024-02"], index=0)
        n_txns = st.slider("Normal transactions to generate", 10, 100, 20)
        run_btn = st.button("🔄 Run Reconciliation", use_container_width=True)
        st.markdown("---")
        st.caption("GitHub: [Am-1111/OneLab_Ventures-Assessment](https://github.com/Am-1111/OneLab_Ventures-Assessment)")

    # ── Run on load or button press ───────────────────────────────────────────
    if "results" not in st.session_state or run_btn:
        with st.spinner("Running reconciliation…"):
            txns, settles = generate_datasets(n_normal=n_txns)
            results = run_reconciliation(txns, settles, month)
            st.session_state["results"] = results
            st.session_state["txns"]    = txns
            st.session_state["settles"] = settles

    results = st.session_state["results"]
    txns    = st.session_state["txns"]
    settles = st.session_state["settles"]
    s       = results["summary"]

    # ── KPI tiles ─────────────────────────────────────────────────────────────
    st.subheader("📊 Summary Metrics")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Transactions",  s["total_platform_transactions"])
    c2.metric("Matched Clean ✅",    s["matched_clean"])
    c3.metric("Duplicates ⚠️",       s["duplicate_transaction_ids"])
    c4.metric("Cross-Month 🗓️",      s["matched_cross_month"])
    c5.metric("Orphan Refunds ↩️",   s["orphan_refunds"])

    c6, c7, c8 = st.columns(3)
    c6.metric("Rounding Diffs 🔢",   s["matched_rounding_diff"])
    c7.metric("Unmatched Txns ❌",   s["unmatched_transactions"])
    c8.metric("Amount Mismatches",   s["matched_amount_mismatch"])

    st.markdown("---")

    # ── Balance check ─────────────────────────────────────────────────────────
    matched = results["matched"]
    total_t = matched["amount_txn"].sum()
    total_s = matched["amount_settle"].sum()
    delta   = round(total_t - total_s, 4)

    st.subheader("💰 Balance Reconciliation")
    bc1, bc2, bc3 = st.columns(3)
    bc1.metric("Platform Total (₹)",   f"{total_t:,.4f}")
    bc2.metric("Settlement Total (₹)", f"{total_s:,.4f}")
    bc3.metric("Net Difference (₹)",   f"{delta:,.4f}",
               delta_color="normal" if abs(delta) < 0.01 else "inverse")

    st.markdown("---")

    # ── Issues detail tabs ────────────────────────────────────────────────────
    st.subheader("🔍 Issue Breakdown")
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "🗓 Cross-Month", "🔢 Rounding", "♊ Duplicates",
        "↩️ Orphan Refunds", "❌ Unmatched", "📋 All Issues"
    ])

    with tab1:
        cross = matched[matched["timing_status"] == "CROSS_MONTH"]
        if cross.empty:
            st.info("No cross-month settlements found.")
        else:
            st.warning(f"{len(cross)} transaction(s) settled in a later month than processed.")
            st.dataframe(cross[["transaction_id","amount_txn","timestamp","settlement_date","settle_month"]], use_container_width=True)

    with tab2:
        rounding = matched[matched["amount_status"] == "ROUNDING_DIFF"]
        if rounding.empty:
            st.info("No rounding discrepancies found.")
        else:
            st.warning(f"{len(rounding)} amount(s) differ by a sub-paisa rounding error.")
            st.dataframe(rounding[["transaction_id","amount_txn","amount_settle","amount_diff"]], use_container_width=True)

    with tab3:
        dupes = results["duplicates"]
        if dupes.empty:
            st.info("No duplicate transaction IDs found.")
        else:
            st.error(f"{len(dupes)} rows share a duplicate transaction_id.")
            st.dataframe(dupes[["transaction_id","user_id","amount","timestamp","anomaly_tag"]], use_container_width=True)

    with tab4:
        orphans = results["orphan_settlements"]
        refunds = orphans[orphans["issue"] == "ORPHAN_REFUND"]
        if refunds.empty:
            st.info("No orphan refunds found.")
        else:
            st.error(f"{len(refunds)} refund(s) with no matching platform transaction.")
            st.dataframe(refunds[["settlement_id","transaction_id","amount_settle","settlement_date"]], use_container_width=True)

    with tab5:
        unmatched = results["unmatched_txn"]
        if unmatched.empty:
            st.success("All transactions have a matching settlement.")
        else:
            st.error(f"{len(unmatched)} transaction(s) with no settlement received.")
            st.dataframe(unmatched[["transaction_id","amount_txn","timestamp"]], use_container_width=True)

    with tab6:
        issues_df = export_report_df(results)
        st.dataframe(issues_df, use_container_width=True)
        csv = issues_df.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download Issues CSV", csv, "reconciliation_issues.csv", "text/csv")

    # ── Raw data expander ─────────────────────────────────────────────────────
    with st.expander("📂 View Raw Datasets"):
        col_t, col_s = st.columns(2)
        with col_t:
            st.caption("Platform Transactions")
            st.dataframe(txns[["transaction_id","user_id","amount","timestamp","anomaly_tag"]], use_container_width=True)
        with col_s:
            st.caption("Bank Settlements")
            st.dataframe(settles[["settlement_id","transaction_id","amount","settlement_date","anomaly_tag"]], use_container_width=True)

    st.caption("Built with Python · Pandas · Streamlit  |  OneLab Ventures AI Assessment 2024")

# ── CLI fallback ──────────────────────────────────────────────────────────────
else:
    def main(export_csv: bool = False):
        print("\n⏳  Generating synthetic datasets …")
        transactions, settlements = generate_datasets(n_normal=20)
        print(f"   → {len(transactions)} transaction rows (incl. duplicates)")
        print(f"   → {len(settlements)} settlement rows (incl. anomalies)\n")
        print("⚙️   Running reconciliation …\n")
        results = run_reconciliation(transactions, settlements, RECON_MONTH)
        print_report(results, RECON_MONTH)
        if export_csv:
            issues_df = export_report_df(results)
            out_path  = os.path.join(OUTPUT_DIR, "reconciliation_issues.csv")
            issues_df.to_csv(out_path, index=False)
            print(f"\n📄  Issues exported → {out_path}")
        return results

    if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument("--csv", action="store_true", help="Export issues to CSV")
        args = parser.parse_args()
        main(export_csv=args.csv)
