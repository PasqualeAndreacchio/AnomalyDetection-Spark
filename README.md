# AnomalyDetection-Spark

> **Distributed Anomaly Detection & Predictive Maintenance for Industrial Refrigeration Devices using Apache Spark**

---

## Project Overview

This project was developed as part of the **Management and Analysis of Physics Datasets (MAPD - Part B)** course.  
The goal is to perform large-scale **anomaly detection** and **predictive maintenance** on real-world telemetry data from **industrial refrigeration devices (Chillers / Heat Pumps)**, leveraging **Apache Spark** for distributed computation on the **CloudVeneto** HPC cluster.

The dataset contains ~6 months of asynchronous, multi-variate sensor telemetry collected from 4 devices (`SW-065`, `SW-088`, `SW-106`, `SW-115`), stored in **Parquet** format on an S3-compatible object storage bucket (`s3a://MAPDB-Group5/data_parquet`). The raw data follows a *long* schema: one row per measurement, with columns `when` (UNIX timestamp in ms), `hwid` (device ID), `metric` (sensor name), and `value`.

---

## Authors

| Name | Notebook | Role |
|---|---|---|
| **Pasquale Andreacchio** | `data_preparation.ipynb` | Data ingestion, alarm decoding, resampling & benchmarking |
| **Matteo Renato Calcagni** | `anomaly1.ipynb` | Compressor engine switch anomaly detection & correlation analysis |
| **Agostina Gonzati** | `anomaly2.ipynb` | Device load vs. external temperature correlation & sizing analysis |
| **Nicola Lavarda** | `predictive_maintenance.ipynb` | Overheating alarm prediction with supervised ML & MLlib |

---

## Repository Structure

```
AnomalyDetection-Spark/
├── data_preparation.ipynb          # Task 1 – Data ingestion, alarm decoding & preprocessing
├── anomaly1.ipynb                  # Task 2 – Engine state-switch anomaly detection
├── anomaly2.ipynb                  # Task 3 – Load vs. temperature correlation analysis
└── predictive_maintenance.ipynb    # Task 4 – Predictive maintenance via supervised ML
```

---

## Notebook Descriptions

### 1. `data_preparation.ipynb` — Data Preparation
**Author:** Pasquale Andreacchio

This notebook is the **foundation of the entire pipeline**. It ingests the raw Parquet dataset from the S3 bucket, decodes packed alarm registers, normalizes the sampling frequency, and produces a clean wide-format DataFrame consumed by all downstream analyses.

**Key steps:**
- **Spark Session Setup:** Connects to the standalone CloudVeneto cluster (`spark://master:7077`) with S3A connectors configured for the Ceph Object Storage endpoint.
- **Alarm Register Decoding (Task 1):** Converts the `A5` (Circuit 1) and `A9` (Circuit 2) integer metrics from their raw 16-bit packed representation to a binary bit-string, then applies bitmask logic to detect overheating events. An `is_overheated` flag is raised when **at least one of bits 6, 7, or 8** is set in either register.
- **1-Minute Resampling (Task 2):** Timestamps are bucketed to 1-minute discrete intervals via `date_trunc("minute", ...)`. A device-level overheating flag is propagated across each `(hwid, timestamp_bucket)`.
- **Smart Aggregation (Task 3):** Selects the thermodynamically most relevant metrics and applies physics-aware aggregation: `mean` for continuous measurements (temperatures, pressures, capacities) and `max` for binary/discrete states (engine ON/OFF status, alarms).
- **Forward-Fill Imputation (Task 4):** Handles asynchronous sensor gaps by forward-filling missing values within each device partition using an unbounded cumulative window.
- **Scalability Benchmark:** Characterizes strong-scaling behaviour of all four pipeline tasks across N ∈ {1, 2, 4, 6} executor cores, with analysis of wall-clock time, speedup S(N), parallel efficiency E(N), JVM GC overhead, and `spark.sql.shuffle.partitions` sensitivity.

---

### 2. `anomaly1.ipynb` — Anomaly Detection 1: Engine State Switches
**Author:** Matteo Renato Calcagni

This notebook identifies anomalies by tracking **ON/OFF state switches of the compressor engines** and correlating their frequency with physical sensor readings.

**Key steps:**
- **Data Loading & Preprocessing:** Reads the raw Parquet dataset, parses timestamps and aligns the schema.
- **Engine Switch Detection (Task 1):** Uses Spark window functions to detect state transitions (0→1 or 1→0) for each of the four compressor engines (`S117`, `S118`, `S169`, `S170`). Aggregates the total number of switches per engine within **1-hour tumbling windows**.
- **Sensor Correlation Analysis (Task 2):** Joins hourly engine switch counts with the 1-hour averages of multiple sensor groups (temperatures, pressures, capacities, discharge temperatures, superheat, inverter/valve signals, power & energy, subcooling). Pearson correlation heatmaps are generated for each sensor group to detect hidden relationships between abnormal switching behaviour and physical operating conditions.
- **Scalability Benchmark:** Demonstrates **super-linear speedup** (>100% parallel efficiency) when scaling from 1 to 4/6 cores, attributed to I/O-bound caching effects on the distributed file system. Results are analysed against Amdahl's Law.

---

### 3. `anomaly2.ipynb` — Anomaly Detection 2: Load vs. External Temperature
**Author:** Agostina Gonzati

This notebook investigates the **correlation between device loading percentage and external ambient temperature**, and identifies anomalous or unexpected load behaviours across the four devices.

**Key steps:**
- **Metric Selection:** Filters the dataset to the three variables of interest — `S125` (Circuit 1 Capacity, %), `S181` (Circuit 2 Capacity, %), and `S41` (External Temperature, °C) — as a narrow transformation before any shuffle.
- **1-Minute Resampling & Pivoting:** Converts the long-format dataset to a wide-format table with one row per `(hwid, minute)`, computing mean capacity and temperature per bucket.
- **Temperature–Load Correlation Analysis:** Computes per-device Pearson correlations for all minutes, device-running minutes, and both-circuits-running minutes, showing that the naive all-minutes correlation is largely an artefact of idle periods. Devices SW-088 and SW-065 exhibit **negative or near-zero** circuit-to-circuit correlation when idle minutes are excluded, revealing circuit alternation rather than synchronous operation.
- **Temporal Drift Analysis:** Weekly time-series profiles expose a **step change** in SW-088's Circuit 2 in early January 2021, where the circuit drops to zero and never recovers — a clear fault signature.
- **Device Sizing Analysis:** Inspects load percentile distributions (mean, median, p95) and time spent in high (≥90%) and low (≤30%) load bands. SW-115 and SW-088 saturate regularly (p95 = 100%), while SW-065 stays below 30% for over half its active time.
- **Hardware Cross-Check:** Counts the distinct raw load values per circuit to verify capacity control logic. Every device reports exactly **4 discrete levels** (0%, and two intermediate values summing to 100%), confirming a two-compressor stepped design rather than continuous modulation.
- **Operating Mode Validation:** Uses `S1`, `S2`, `S123`, and `S179` mode variables to corroborate findings — SW-065 operates exclusively in code-1 (both circuits modulated together), while SW-088, SW-106, SW-115 use higher codes, consistent with their load profiles.
- **Scalability Benchmark:** Measures three pipeline phases (map-like, group-like, advanced mix) from 1 to 6 cores, reporting speedups of 3.37×, 3.54×, and 2.22× respectively. The advanced-mix phase scales worst due to a global ordering stage that serialises execution into a single partition.

---

### 4. `predictive_maintenance.ipynb` — Predictive Maintenance: Engine Overheating Prediction
**Author:** Nicola Lavarda

This notebook implements a full **supervised ML pipeline** to predict future engine overheating alarms using physically-motivated features derived from prior timesteps.

**Key steps:**
- **Bitwise Alarm Extraction:** Decodes `A5` and `A9` packed registers using both native bitwise masking (`bitwiseAND(224)`) and string-based conversion, confirming 100% logical concordance between methods.
- **1-Minute Resampling & Smart Aggregation:** Normalizes asynchronous telemetry to a 1-minute discrete grid, applying `mean` for continuous physical metrics and `max` for discrete engine states and alarms.
- **Memory-Safe Wide Pivoting:** Selects a curated subset of 30 thermodynamically critical features (temperatures, pressures, capacities, superheat, electrical power) to avoid OOM errors during Spark pivot operations.
- **Forward-Fill Imputation:** Fills missing sensor values within each `hwid` partition using a chronological cumulative window.
- **Correlation Analysis (Section 3.3.3):** Computes Pearson correlations between all physical telemetry and the `is_overheated` flag, and generates a bivariate profile comparing mean sensor values in Normal (0) vs. Overheated (1) states. Findings guide downstream feature engineering.
- **Feature Engineering:** Constructs lagged predictors at T-1 and T-5, dynamic thermal gradient features (ΔT per minute), and cyclical temporal features (`hour_sin`, `hour_cos`, `is_night_defrost_window`) to capture periodic defrost alarm patterns.
- **Blocked Temporal Train/Test Split:** Divides the time series into 10 contiguous chronological blocks (7 train / 3 test) with purging and embargo to prevent leakage from regime shifts and avoid overrepresentation of seasonal alarm clusters.
- **Model Training & Imbalanced Class Handling:** Trains three classifiers — **Logistic Regression**, **Random Forest**, and **HistGradientBoosting** — with mild stratified undersampling (1:100) to address extreme class imbalance (~0.02% overheating prevalence).
- **Model Evaluation:** Reports PR-AUC (primary metric), ROC-AUC, precision, recall, and F₁ across all models. Includes multi-scenario decision calibration for industrial operational thresholds.
- **Feature Importance & Thermodynamic Interpretation:** Identifies the most predictive physical sensors (discharge temperatures, superheat, pressure ratios) and validates results against known refrigeration physics.
- **Distributed MLlib Pipeline:** Implements a native PySpark MLlib pipeline (`VectorAssembler` + `GBTClassifier`) with distributed blocked temporal splitting and stratified sampling for full cluster execution.
- **Scalability Benchmark:** Profiles four computational archetypes (bitwise extraction, wide pivot, gradient boosting, MLlib training) across N ∈ {1, 2, 4, 6} cores with Amdahl's Law analysis and shuffle partition tuning (`spark.sql.shuffle.partitions` ∈ {6, 12, 32, 200}).

---

## Technology Stack

| Component | Technology |
|---|---|
| Distributed Computing | Apache Spark (PySpark) |
| Storage | S3-compatible Ceph Object Storage (CloudVeneto) |
| Data Format | Apache Parquet |
| ML Framework | scikit-learn, PySpark MLlib |
| Cluster | CloudVeneto HPC (standalone Spark master + worker nodes) |
| Notebook Environment | Jupyter Notebooks |
| Visualization | matplotlib, seaborn |

---