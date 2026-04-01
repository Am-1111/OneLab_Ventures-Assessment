"""
main.py
-------
Orchestrator — generate data → reconcile → print report → export CSV.
Also the entry point for a future FastAPI / Streamlit wrapper.

Usage:
    python main.py               # runs full pipeline
    python main.py --csv         # also exports issues.csv
"""

import argparse
import os
import sys

from data_generator import generate_datasets
from reconciler     import run_reconciliation
from report         import print_report, export_report_df

RECON_MONTH = "2024-01"
OUTPUT_DIR  = os.path.dirname(__file__)


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


# ── Extension point for FastAPI ───────────────────────────────────────────────
# from fastapi import FastAPI
# app = FastAPI()
#
# @app.get("/reconcile")
# def reconcile_endpoint(month: str = "2024-01"):
#     txns, settles = generate_datasets()          # replace with DB queries
#     results = run_reconciliation(txns, settles, month)
#     return {
#         "summary": results["summary"],
#         "issues":  export_report_df(results).to_dict(orient="records"),
#     }

# ── Extension point for Streamlit ────────────────────────────────────────────
# import streamlit as st
# if __name__ == "__streamlit__":
#     st.title("Payments Reconciliation Dashboard")
#     month = st.selectbox("Reconciliation Month", ["2024-01","2024-02"])
#     txns, settles = generate_datasets()
#     results = run_reconciliation(txns, settles, month)
#     st.json(results["summary"])
#     st.dataframe(export_report_df(results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="store_true", help="Export issues to CSV")
    args = parser.parse_args()
    main(export_csv=args.csv)
