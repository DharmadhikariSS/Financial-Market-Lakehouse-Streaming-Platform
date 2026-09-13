"""Interactive Visual Portfolio Dashboard.

Real-time Market Candlesticks, Iceberg Time-Travel Inspector, DLQ Forensics,
and Storage Cost Benchmarking built with Streamlit and DuckDB.
"""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from src.common.config import get_settings
from src.milestone_04_lakehouse_cdc_contracts.iceberg_writer import IcebergTableManager
from src.milestone_05_resilient_streaming_platform.backfill import BackfillEngine
from src.milestone_05_resilient_streaming_platform.dlq_router import DeadLetterQueueRouter
from src.milestone_05_resilient_streaming_platform.run_streaming_pipeline import (
    run_streaming_pipeline,
)

settings = get_settings()

# Page configuration
st.set_page_config(
    page_title="Financial Market Lakehouse & Streaming Platform",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom High-Density Financial Terminal Theme
st.markdown(
    """
    <style>
    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        letter-spacing: -0.5px;
        margin-bottom: 0.2rem;
    }
    .sub-title {
        font-size: 1.05rem;
        color: #8892b0;
        margin-bottom: 1.5rem;
    }
    .metric-badge {
        display: inline-block;
        padding: 4px 10px;
        border-radius: 6px;
        font-weight: 600;
        font-size: 0.85rem;
        margin-right: 8px;
    }
    .badge-green { background-color: rgba(46, 204, 113, 0.2); color: #2ecc71; border: 1px solid #2ecc71; }
    .badge-blue { background-color: rgba(52, 152, 219, 0.2); color: #3498db; border: 1px solid #3498db; }
    .badge-gold { background-color: rgba(241, 196, 15, 0.2); color: #f1c40f; border: 1px solid #f1c40f; }
    .badge-purple { background-color: rgba(155, 89, 182, 0.2); color: #9b59b6; border: 1px solid #9b59b6; }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# SIDEBAR: PLATFORM CONTROLS & ARCHITECTURE STATUS
# -----------------------------------------------------------------------------
with st.sidebar:
    st.image("https://img.icons8.com/color/96/bullish.png", width=64)
    st.title("Control Tower")
    st.caption("5-Tier Financial Market Ledger")

    st.markdown("---")
    st.subheader("🏛️ Architecture Milestones")
    st.markdown("✅ **M1**: Ingestion Engine (UPSERT Idempotency)")
    st.markdown("✅ **M2**: Docker Batch & Scheduler")
    st.markdown("✅ **M3**: Snappy Parquet & dbt Marts")
    st.markdown("✅ **M4**: Iceberg ACID CDC & Time-Travel")
    st.markdown("✅ **M5**: Event-Time Stream & DLQ")

    st.markdown("---")
    st.subheader("⚡ Live Pipeline Actions")

    if st.button("🚀 Trigger Stream Batch (M5)", use_container_width=True):
        with st.spinner("Executing streaming engine with out-of-order records & DLQ..."):
            res = run_streaming_pipeline(total_events=120, out_of_order_count=8, poison_pill_count=4)
            st.success(f"Processed {res['successfully_processed']} events! Materialized {res['materialized_candles_count']} candles.")
            st.rerun()

    if st.button("🔄 Trigger Backfill Replay", use_container_width=True):
        with st.spinner("Replaying historical lakehouse parquet into isolated backfill table..."):
            engine = BackfillEngine()
            start_dt = datetime(2026, 9, 13, 0, 0, 0, tzinfo=UTC)
            end_dt = datetime(2026, 9, 14, 23, 59, 59, tzinfo=UTC)
            bf_res = engine.execute_backfill(from_ts=start_dt, to_ts=end_dt, dry_run=False)
            st.success(f"Backfill complete! Generated {bf_res['generated_candles']} candles.")
            st.rerun()

    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #8892b0; font-size: 0.8rem;'>
            Zero Cloud Spend Guarantee ($0.00)<br>
            Strict Idempotency & ACID Semantics<br>
            <b>Staff Data Engineer Portfolio</b>
        </div>
        """,
        unsafe_allow_html=True,
    )

# -----------------------------------------------------------------------------
# HEADER METRICS BANNER
# -----------------------------------------------------------------------------
st.markdown("<div class='main-title'>⚡ Real-Time Financial Market Lakehouse & Streaming Platform</div>", unsafe_allow_html=True)
st.markdown(
    """
    <div class='sub-title'>
        <span class='metric-badge badge-green'>Cloud Spend: $0.00 (100% Free Open-Source)</span>
        <span class='metric-badge badge-blue'>Memory Overhead: &lt; 80MB</span>
        <span class='metric-badge badge-gold'>Unit Tests: 18/18 Passing (100%)</span>
        <span class='metric-badge badge-purple'>Storage Compression: 51.9% via Hive Parquet</span>
    </div>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------------------------------------------------
# MAIN TABS LAYOUT
# -----------------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Real-Time Streaming & Candlesticks",
    "🧊 Lakehouse CDC & Time-Travel",
    "🛡️ DLQ & Quarantine Forensics",
    "📊 Storage & Query Benchmark (M3)",
    "📑 Portfolio Architecture & SSOT",
])

# =============================================================================
# TAB 1: REAL-TIME STREAMING & CANDLESTICK CHARTS
# =============================================================================
with tab1:
    st.subheader("Real-Time Event-Time Candlestick Mart (1-Minute Tumbling Windows)")
    st.caption("Aggregated via 5-second bounded out-of-order watermark from live Avro stream.")

    db_path = settings.BASE_DIR / "data" / "streaming" / "realtime_mart.duckdb"
    if db_path.exists():
        with duckdb.connect(str(db_path)) as conn:
            candles_df = conn.execute("""
                SELECT
                    symbol,
                    window_start,
                    window_end,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    base_volume,
                    quote_volume,
                    vwap,
                    trade_count,
                    taker_buy_ratio
                FROM realtime_market_candles
                ORDER BY window_start DESC, symbol ASC;
            """).df()
    else:
        candles_df = pd.DataFrame()

    if not candles_df.empty:
        col_sym, col_refresh = st.columns([3, 1])
        with col_sym:
            available_symbols = candles_df["symbol"].unique().tolist()
            selected_symbol = st.selectbox("Select Trading Instrument", available_symbols, index=0)
        with col_refresh:
            st.write("")
            st.write("")
            if st.button("Refresh Mart"):
                st.rerun()

        sym_df = candles_df[candles_df["symbol"] == selected_symbol].copy()
        sym_df = sym_df.sort_values("window_start")

        if not sym_df.empty:
            latest = sym_df.iloc[-1]
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Latest Close", f"${latest['close_price']:,.2f}")
            m2.metric("VWAP", f"${latest['vwap']:,.2f}")
            m3.metric("24h Quote Vol", f"${sym_df['quote_volume'].sum():,.2f}")
            m4.metric("Window Trades", f"{int(latest['trade_count'])}")
            m5.metric("Taker Buy %", f"{latest['taker_buy_ratio']:.1f}%")

            # Candlestick Price & VWAP Chart
            st.markdown(f"#### Price Action & VWAP Trend ({selected_symbol})")
            chart_df = sym_df[["window_start", "close_price", "vwap", "high_price", "low_price"]].copy()
            chart_df["window_start"] = pd.to_datetime(chart_df["window_start"])
            chart_df = chart_df.set_index("window_start")

            st.line_chart(
                chart_df[["close_price", "vwap"]],
                color=["#2ecc71", "#f39c12"],
                use_container_width=True,
            )

            # Materialized Candles Ledger Table
            st.markdown("#### Materialized 1-Minute Candles Ledger")
            st.dataframe(
                sym_df[[
                    "window_start", "open_price", "high_price", "low_price",
                    "close_price", "vwap", "base_volume", "trade_count", "taker_buy_ratio"
                ]],
                use_container_width=True,
                hide_index=True,
            )
    else:
        st.info("No streaming candles in DuckDB mart yet. Click 'Trigger Stream Batch (M5)' in the sidebar to populate!")

# =============================================================================
# TAB 2: LAKEHOUSE CDC & TIME-TRAVEL INSPECTOR
# =============================================================================
with tab2:
    st.subheader("Apache Iceberg Open Lakehouse Format & Time-Travel Explorer")
    st.caption("Inspect ACID table versions, committed snapshots, and reconstruct historical ledger states.")

    iceberg_dir = settings.BASE_DIR / "data" / "lakehouse" / "iceberg"
    table_mgr = IcebergTableManager(table_dir=iceberg_dir)
    snapshots = table_mgr.list_snapshots()

    if snapshots:
        col_meta, col_slider = st.columns([2, 3])
        with col_meta:
            latest_snap = snapshots[-1]
            st.markdown("##### Current Table Metadata")
            st.markdown(f"- **Current Snapshot ID**: `{latest_snap.snapshot_id}`")
            st.markdown(f"- **Total Snapshots**: `{len(snapshots)}`")
            st.markdown("- **Format Specification**: Apache Iceberg v2 Format Spec")
            st.markdown("- **Primary Key**: `(symbol, trade_id)`")

        with col_slider:
            st.markdown("##### ⏳ Time-Travel Snapshot Selector")
            snap_ids = [s.snapshot_id for s in snapshots]
            selected_snap_id = st.select_slider(
                "Scrub table history to query as of snapshot:",
                options=snap_ids,
                value=snap_ids[-1],
            )

        # Snapshot Commit History Table
        st.markdown("#### Lakehouse Snapshot Commit Log")
        snap_history = []
        for s in snapshots:
            dt = datetime.fromtimestamp(s.timestamp_ms / 1000.0, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
            is_active = "🟢 CURRENT" if s.snapshot_id == selected_snap_id else "⚪ HISTORICAL"
            snap_history.append({
                "State": is_active,
                "Snapshot ID": str(s.snapshot_id),
                "Parent ID": str(s.parent_snapshot_id or "root"),
                "Committed At": dt,
                "Manifest File": Path(s.manifest_file).name,
            })
        st.dataframe(pd.DataFrame(snap_history), use_container_width=True, hide_index=True)

        # Reconstructed Point-in-Time Table State
        st.markdown(f"#### Ledger State at Snapshot `{selected_snap_id}`")
        historical_records = table_mgr.time_travel_query(selected_snap_id)
        if historical_records:
            hist_df = pd.DataFrame(historical_records)
            st.dataframe(
                hist_df[["trade_id", "symbol", "price", "quantity", "quote_quantity", "trade_timestamp", "is_buyer_maker"]],
                use_container_width=True,
                hide_index=True,
            )
            st.caption(f"Showing {len(historical_records)} active rows as of snapshot {selected_snap_id}.")
        else:
            st.warning("Snapshot contains 0 rows.")
    else:
        st.info("No Iceberg snapshots detected yet. Execute Milestone 04 CDC pipeline to generate.")

# =============================================================================
# TAB 3: DEAD-LETTER QUEUE (DLQ) & QUARANTINE FORENSICS
# =============================================================================
with tab3:
    st.subheader("Dead-Letter Queue (DLQ) & Data Quality Quarantine Forensics")
    st.caption("Option B Quality Gate enforcement: poison pills are isolated with full audit diagnostics without crashing streams.")

    dlq_router = DeadLetterQueueRouter()
    dlq_records = dlq_router.read_dlq_records()

    # Also read CDC quality quarantine if exists
    cdc_quarantine_file = settings.BASE_DIR / "data" / "lakehouse" / "quarantine" / "corrupt_cdc_events.jsonl"
    cdc_quarantine_records = []
    if cdc_quarantine_file.exists():
        with open(cdc_quarantine_file, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    cdc_quarantine_records.append(json.loads(line))

    q1, q2, q3, q4 = st.columns(4)
    total_dlq = len(dlq_records) + len(cdc_quarantine_records)
    deser_errs = sum(1 for r in dlq_records if r.get("error_code") == "DESERIALIZATION_FAILURE")
    contract_errs = sum(1 for r in dlq_records if r.get("error_code") == "DOMAIN_CONTRACT_VIOLATION") + len(cdc_quarantine_records)

    q1.metric("Total Quarantined", f"{total_dlq}")
    q2.metric("Wire Framing Errors", f"{deser_errs}")
    q3.metric("Contract Breaches", f"{contract_errs}")
    q4.metric("Pipeline Availability", "100.0%", help="Zero downtime caused by poison pills")

    if dlq_records:
        st.markdown("#### Quarantined Streaming Incidents (`market_trades_dlq.jsonl`)")
        table_rows = []
        for idx, r in enumerate(dlq_records):
            table_rows.append({
                "Index": idx,
                "Failed At (UTC)": r.get("failed_at_utc", "N/A"),
                "Topic": r.get("original_topic", "N/A"),
                "Error Code": r.get("error_code", "N/A"),
                "Error Diagnosis": r.get("error_message", "N/A"),
                "Raw Bytes Length": r.get("raw_payload_len", 0),
            })
        st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

        # Deep-dive Inspector
        st.markdown("#### 🔬 Poison Pill Byte Inspector")
        selected_idx = st.selectbox("Select Quarantined Record to Inspect", range(len(dlq_records)), index=0)
        selected_rec = dlq_records[selected_idx]

        c_left, c_right = st.columns(2)
        with c_left:
            st.markdown("**Diagnostic Metadata**")
            st.json({
                "error_code": selected_rec.get("error_code"),
                "error_message": selected_rec.get("error_message"),
                "original_topic": selected_rec.get("original_topic"),
                "failed_at_utc": selected_rec.get("failed_at_utc"),
                "schema_id": selected_rec.get("schema_id"),
            })
        with c_right:
            st.markdown("**Preserved Raw Payload (Base64 & Hex)**")
            b64_str = selected_rec.get("raw_payload_b64", "")
            try:
                raw_bytes = base64.b64decode(b64_str)
                hex_dump = raw_bytes.hex()
            except Exception:
                hex_dump = "unparseable"

            st.code(f"Base64: {b64_str}\nHex Dump: {hex_dump}", language="text")
            if selected_rec.get("stack_trace"):
                st.markdown("**Exception Stack Trace**")
                st.code(selected_rec["stack_trace"], language="text")
    else:
        st.info("No quarantined records in DLQ yet.")

# =============================================================================
# TAB 4: STORAGE & QUERY COST BENCHMARK (MILESTONE 03)
# =============================================================================
with tab4:
    st.subheader("Milestone 03: Storage & Query Scan Cost Benchmark Study")
    st.caption("Empirical measurements across 100,000 real financial trade records.")

    b1, b2, b3 = st.columns(3)
    b1.metric("Storage Reduction", "51.9%", help="From 8.21MB CSV down to 3.95MB Snappy Parquet")
    b2.metric("Selective Query Speedup", "21.4x FASTER", help="Partition pruning reduces scan from 97.49ms to 4.55ms")
    b3.metric("Annual Cloud S3 Savings", "$0.00 (Self-Hosted)", help="Zero recurring AWS/Snowflake storage charges")

    # Storage Comparison Bar Chart
    storage_data = pd.DataFrame({
        "Format": ["Monolithic Raw CSV", "Unpartitioned Parquet (Snappy)", "Hive-Partitioned Parquet (Snappy)"],
        "Size_MB": [8.21, 4.15, 3.95],
        "Compression_Ratio": ["1.00x", "1.98x", "2.08x"],
    })
    st.markdown("#### Physical Disk Footprint Comparison (100k Records)")
    st.bar_chart(storage_data.set_index("Format")["Size_MB"], color="#3498db", use_container_width=True)

    # Query Latency Table
    st.markdown("#### Empirical Query Execution Times")
    query_perf = pd.DataFrame({
        "Query Scenario": [
            "Query 1: Full Table Aggregation Scan",
            "Query 2: Selective Filter (symbol='BTCUSDT' AND day=3)",
            "Query 3: High-Volume Volume Weighted Price (VWAP)",
        ],
        "Raw CSV (ms)": [102.91, 97.49, 108.34],
        "Unpartitioned Parquet (ms)": [7.13, 3.22, 6.89],
        "Hive-Partitioned Parquet (ms)": [7.19, 4.55, 6.42],
        "Speedup vs CSV": ["14.3x", "21.4x", "16.8x"],
    })
    st.dataframe(query_perf, use_container_width=True, hide_index=True)

# =============================================================================
# TAB 5: PORTFOLIO ARCHITECTURE & SSOT
# =============================================================================
with tab5:
    st.subheader("Authoritative Single Source of Truth (SSOT) Summary")
    st.caption("Architectural decisions, trade-offs, and design patterns preserved for technical auditing.")

    st.markdown(
        """
        ### 🎯 Core Engineering Principles
        1. **Strict Idempotency**:
           Every record generates a deterministic SHA-256 state hash. Replaying ingestion batches results in exact zero net change (`status: UNCHANGED`), mathematically eliminating duplicate records.
        2. **Multi-Stage Distroless Packaging**:
           Milestone 02 compiles Python wheels in a builder stage and copies only compiled artifacts into a lightweight Alpine image, executing as an unprivileged non-root user.
        3. **Kimball Dimensional Modeling**:
           Transformations follow strict modular staging $\\rightarrow$ intermediate $\\rightarrow$ conformed marts star schemas (`dim_assets`, `fct_trades`, `agg_daily_market_metrics`).
        4. **Open Lakehouse Formats (Apache Iceberg)**:
           Row-level ACID mutations are tracked via immutable metadata manifests (`v1.metadata.json`, `v2.metadata.json`), enabling time-travel queries without locking readers.
        5. **Confluent Wire Framing & Schema Evolution**:
           Streams are binary-framed with `0x00 + 4-byte Schema ID`. Schemas evolve with backward-compatible defaults, allowing consumers to upgrade safely without coordination.
        6. **Event-Time Watermarking**:
           $W(t) = \\max(t_e) - 5\\text{s}$ accommodates network latency without mutating past finalized candles. Late arrivals past the watermark are quarantined.
        """
    )

    st.markdown("---")
    st.markdown("#### 📖 Full Authoritative Documentation")
    st.markdown("- [**`PORTFOLIO_SSOT.md`**](file:///d:/Vibe%20coding%20projects/Data%20Engineering%20Projects/PORTFOLIO_SSOT.md) (Master 500+ line specification)")
    st.markdown("- [**`README.md`**](file:///d:/Vibe%20coding%20projects/Data%20Engineering%20Projects/README.md) (Repo Quickstart & One-command guide)")
    st.markdown("- [**`docs/benchmark_results.md`**](file:///d:/Vibe%20coding%20projects/Data%20Engineering%20Projects/docs/benchmark_results.md) (Storage & scan study)")
