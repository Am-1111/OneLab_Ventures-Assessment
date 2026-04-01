# 📊 AI-Powered Transaction Reconciliation System

## 🚀 Overview
This project simulates and solves a real-world financial reconciliation problem where a payment platform's internal transaction records do not match bank settlement records.

The goal is to identify discrepancies, explain gaps, and generate actionable insights using an AI-assisted approach.

---

## 🧠 Problem Statement
A payments company processes transactions instantly, but bank settlements occur with a delay (1–2 days).  
At month-end, both records should match — but they don’t.

This system:
- Generates synthetic financial data
- Injects real-world discrepancies
- Performs reconciliation
- Identifies and explains mismatches

---

## ⚙️ Features

### ✅ Data Simulation
- Generates:
  - Transactions dataset (platform)
  - Settlements dataset (bank)
- Includes realistic anomalies:
  - Cross-month settlement delay
  - Duplicate entries
  - Refund without original transaction
  - Rounding discrepancies (aggregate level)

---

### 🔍 Reconciliation Engine
- Matches transactions with settlements
- Detects:
  - Unmatched transactions
  - Missing settlements
  - Duplicate records
  - Refund inconsistencies
  - Timing mismatches

---

### 📈 Insights & Reporting
- Summary metrics:
  - Total transactions
  - Matched vs unmatched
  - Discrepancy breakdown
- Tabular reconciliation report
- Clear explanations of each issue

---

### 🧪 Testing
Custom test cases validate:
- Duplicate detection logic
- Missing settlement identification
- Rounding mismatch detection
- Cross-month reconciliation handling

---

## 🏗️ Tech Stack
- Python
- Pandas
- NumPy
- (Optional) Streamlit for UI

---



