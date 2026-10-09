# Enterprise E-Commerce Data Ingestion Framework (Medallion + DLQ + Watermarking)

## Executive Summary
An enterprise-grade, fault-tolerant Data Engineering pipeline built with Python, PySpark, and Airflow. Designed to handle high-frequency e-commerce stream data ingestion, enforce strict schema validation, guarantee idempotency through watermarking, and isolate corrupted records using a Dead Letter Queue (DLQ) pattern.

This repository demonstrates production-ready patterns for building scalable ETL/ELT data platforms with zero data loss.

---

## System Architecture & Data Flow

- **Raw Stream Input (JSON / CSV)** -> **Bronze Layer (Raw Landing)**
- **Bronze Layer** -> **Dead Letter Queue / DLQ (Malformed Data Quarantine)**
- **Bronze Layer** -> **Silver Layer (Cleansed, Conformed, Watermarked)**
- **Silver Layer** -> **Gold Layer (Analytics-Ready Data Mart)**

---

## Core Technical Features

1. **Medallion Architecture (Bronze -> Silver -> Gold):**
   - **Bronze:** Raw data ingestion retaining source payload fidelity and ingestion metadata.
   - **Silver:** Cleaned, deduplicated, and strictly typed records ready for downstream analytics.
   - **Gold:** Aggregated business metrics formatted for BI tools and dashboards.

2. **Dead Letter Queue (DLQ) & Fault Tolerance:**
   - Automated routing of corrupted payloads or schema mismatches into an isolated quarantine state.
   - Prevents pipeline crashes while allowing continuous processing of valid data.

3. **Incremental Watermarking & Idempotency:**
   - Tracks processed timestamps to ensure strictly incremental loads and prevent duplicates.

4. **Orchestration & Automation:**
   - Managed DAG integration for Apache Airflow (`e_commerce_stream_dag.py`).
   - Automated Spark execution runner (`e_commerce_stream_run_spark.sh`).

---

## Tech Stack
- **Language:** Python 3.9+
- **Framework:** Apache Spark / PySpark
- **Orchestration:** Apache Airflow
- **Data Engineering Concepts:** Medallion Architecture, DLQ, Watermarking, Streaming
-
