# -*- coding: utf-8 -*-
"""
RETAIL DATA PIPELINE - ETL & ANALYTICS
Author: [Giorgio Orlando]
Version: 1.0 (March 2026)

This script automates the ingestion, pseudonymization, and loading 
of retail datasets into Azure SQL for Power BI visualization.
It handles dynamic time intelligence and GDPR-compliant data masking.
"""
# Standard Library Imports (File system, Regex, Utils)
import os
import re
import sys
import random
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, List

# Data Manipulation (The core engine)
import numpy as np
import pandas as pd
import polars as pl

# GUI & File Management
import tkinter as tk
from tkinter import filedialog

# Database Connectivity & Environment
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

"""
DEPENDENCIES & ARCHITECTURE NOTES:
- Polars: Selected for high-performance, vectorized pseudonymization.
- Pandas: Used as a bridge for SQLAlchemy and SQL Server compatibility.
- SQLAlchemy: Manages robust connections to the Azure SQL Cloud environment.
- Tkinter: Implemented to ensure a seamless file-selection experience for the user.
"""

# ===============================================================
# 0. PATHS & EXTERNAL DICTIONARIES CONFIGURATION
# ===============================================================
"""
Initializes the system environment and imports business-specific dictionaries.

This block resolves dynamic project paths to ensure cross-platform compatibility 
and loads external lookup tables used for data normalization and pseudonymization.
"""
CODE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CODE_DIR)
INPUT_DIR = os.path.join(PROJECT_ROOT, "Input")
if INPUT_DIR not in sys.path:
    sys.path.append(INPUT_DIR)

try:
    """
        Attempts to import lookup tables and replacement dictionaries.
        
        These external resources define the business rules for:
        - Vendor categorization (GOV_LIST, SELLER_B_LIST)
        - Device and IDP normalization (DEVICE_REPLACEMENTS, IDP_REPLACEMENTS)
        - Status and Mode translations (STATUS_TRANS, MODE_TRANS)
        - Holiday management (ITALIAN_PUBLIC_HOLIDAYS)
        """
    from Keys.dictionaries import (
        GOV_LIST,
        SELLER_B_LIST,
        DEVICE_REPLACEMENTS,
        IDP_REPLACEMENTS,
        STATUS_TRANS,
        MODE_TRANS,
        ITALIAN_PUBLIC_HOLIDAYS,
    )
except ImportError:
    print("❌ ERROR: Input/Keys/dictionaries.py not found! Please verify the path.")
    sys.exit(1)


# ===============================================================
# 1. LOGGER
# ===============================================================
class AppLogger:
    """
    A utility class to handle system logging for the ETL pipeline.

    This class creates a timestamped log file and provides methods to write 
    messages to both the console and the persistent log file.
    """
    def __init__(self, log_dir: str):
        """
        Initializes the logger and creates the log directory if it doesn't exist.

        Args:
            log_dir (str): The directory path where log files will be stored.
        """
        os.makedirs(log_dir, exist_ok=True)
        self.log_path = os.path.join(
            log_dir, f"ETL_Log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

    def log(self, message: str) -> None:
        """
        Formats and writes a message with a timestamp to the console and the log file.

        Args:
            message (str): The text message to be recorded.
        """
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {message}"
        print(line)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")


# ===============================================================
# 2. PATH CONFIG
# ===============================================================
@dataclass
class PathConfig:
    """
    Manages the project's directory structure and file paths for the ETL process.

    This class centralizes path management for input sources, output datasets (Parquet), 
    logging, and sensitive lookup files used for pseudonymization.
    """
    project_root: str

    def __post_init__(self):
        """
        Initializes subdirectories and ensures that required output and secret folders 
        are created upon instantiation.
        """
        self.input_dir = os.path.join(self.project_root, "Input")
        self.output_dir = os.path.join(self.project_root, "Output")
        self.logs_dir = os.path.join(self.project_root, "Logs")

        # Lookup persistence (security backup)
        # Dedicated directory for sensitive de-anonymization keys
        self.secrets_dir = os.path.join(self.output_dir, "DEANON_KEYS")
        os.makedirs(self.secrets_dir, exist_ok=True)

        self.ra_lookup = os.path.join(self.secrets_dir, "SECRET_Lookup_SellingPoint.csv")
        self.req_lookup = os.path.join(self.secrets_dir, "SECRET_Lookup_RequestID.csv")
        self.dev_idp_lookup = os.path.join(self.secrets_dir, "SECRET_Lookup_Device_IDP.csv")
        self.cloud_orders_lookup = os.path.join(self.secrets_dir, "SECRET_Lookup_CloudOrders.csv")
        self.status_lookup = os.path.join(self.secrets_dir, "SECRET_Lookup_RequestStatus.csv")
        self.mode_lookup = os.path.join(self.secrets_dir, "SECRET_Lookup_Mode.csv")

        # Parquet data lake
        self.parquet_dir = os.path.join(self.output_dir, "PARQUET")
        os.makedirs(self.parquet_dir, exist_ok=True)

        os.makedirs(self.logs_dir, exist_ok=True)

    @staticmethod
    def detect_project_root() -> str:
        """
        Automatically identifies the project's root directory based on the script's location.

        Returns:
            str: The absolute path to the project root.
        """
        code_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.dirname(code_dir)


# ===============================================================
# 3. FILE PICKER
# ===============================================================
def select_file_optional(title: str, initial_dir: str) -> Optional[str]:
    """
    Opens a native OS file dialog for user-driven file selection.

    This utility initializes a hidden Tkinter root to present a top-most 
    selection window, ensuring a seamless user experience. It supports 
    optional selection: if the user cancels, the pipeline continues or 
    skips the specific phase without crashing.

    Args:
        title (str): The descriptive title displayed on the dialog window.
        initial_dir (str): The starting directory path for the file picker.

    Returns:
        Optional[str]: The absolute path to the selected file, or None if 
                       the selection was cancelled.
    """
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    path = filedialog.askopenfilename(
        title=title,
        initialdir=initial_dir,
        filetypes=[("All Files", "*.*"), ("CSV Files", "*.csv")],
    )
    root.destroy()
    return path if path else None


# ===============================================================
# 4. DATABASE
# ===============================================================
class Database:
    """
    Handles secure connectivity and data operations with Azure SQL Database.

    This class provides a robust wrapper around SQLAlchemy to manage 
    cloud connections, environment-based authentication, and efficient 
    batch data loading.
    """
    def __init__(self, logger: AppLogger):
        """
        Initializes the Database handler with a logger and a null engine.
        """
        self.logger = logger
        self.engine: Optional[Engine] = None

    def query_scalar(self, sql: str):
        """
        Executes a SQL query and returns a single scalar value.
        
        Useful for quick database checks like counting rows or verifying IDs.
        """
        if self.engine is None:
            raise RuntimeError("Engine is not initialized.")
        with self.engine.connect() as conn:
            res = conn.execute(text(sql))
            return res.scalar()

    def load_env_from_file(self, path_env: str) -> None:
        """
        Loads database credentials from an external .env file for security.
        """
        load_dotenv(path_env)
        self.logger.log(f"🔐 Credentials loaded from: {path_env}")

    def connect_from_env(self) -> None:
        """
        Builds the connection string and initializes the SQLAlchemy engine.

        Features:
        - URL-encoded passwords for special character handling.
        - Encryption enabled for Azure SQL compatibility.
        - Connection pooling (pool_pre_ping) to handle stale cloud connections.
        """
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_server = os.getenv("DB_SERVER")
        db_name = os.getenv("DB_NAME")

        if not all([db_user, db_password, db_server, db_name]):
            raise RuntimeError("Missing DB variables in .env (DB_USER, DB_PASSWORD, DB_SERVER, DB_NAME).")

        # URL Encoding handles passwords with special characters (@, #, !, etc.)
        pwd_encoded = urllib.parse.quote_plus(str(db_password))


        conn_str = (
            f"mssql+pyodbc://{db_user}:{pwd_encoded}@{db_server}:1433/{db_name}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
            f"&Encrypt=yes"
            f"&TrustServerCertificate=yes"
            f"&LoginTimeout=120"   
            f"&Timeout=120"        
        )


        self.engine = create_engine(
            conn_str, 
            pool_pre_ping=True,
            pool_size=
            connect_args=5,
            max_overflow=2,
                "timeout": 120
            }
        )
        self.logger.log("🔌 SQLAlchemy connection is ready.")

    def execute(self, sql: str) -> None:
        """
        Executes a direct SQL command (e.g., DROP, DELETE) with transaction commit.
        """
        if self.engine is None:
            raise RuntimeError("Engine is not initialized.")
        with self.engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

    def to_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
        chunksize: Optional[int] = None,
    ) -> None:
        """
        Performs high-speed batch loading of DataFrames into SQL tables.

        Args:
            df (pd.DataFrame): The dataset to upload.
            table_name (str): Destination table name in the DB.
            if_exists (str): Action if table exists ('fail', 'replace', 'append').
            chunksize (int): Number of rows to write per batch (optimizes memory).
        """
        if self.engine is None:
            raise RuntimeError("Engine is not initialized.")
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=index, chunksize=chunksize)


# ===============================================================
# 5. LOOKUP STORE (CSV)
# ===============================================================
class LookupStore:
    """
    Persistence layer for pseudonymization mappings and historical tracking.

    This class manages the lifecycle of 'secret' lookup tables (CSV-based). 
    It ensures that anonymized identifiers remain consistent across different 
    ETL runs by loading existing maps and persisting new ones.
    """
    def __init__(self, logger: AppLogger, paths: PathConfig):
        """
        Initializes the store with logging and path configurations.
        """
        self.logger = logger
        self.paths = paths

    @staticmethod
    def _load_history(filepath: str, key_col: str, val_col: str) -> Dict[str, str]:
        """
        Loads a CSV lookup file into a Python dictionary for rapid access.

        Args:
            filepath (str): Path to the lookup CSV.
            key_col (str): Column name containing original values.
            val_col (str): Column name containing pseudonymized values.

        Returns:
            Dict[str, str]: A mapping dictionary {Original: Pseudonym}.
        """
        if os.path.exists(filepath):
            df = pd.read_csv(filepath, sep=";", dtype=str)
            return dict(zip(df[key_col], df[val_col]))
        return {}

    @staticmethod
    def _max_counter_for_prefix(history_dict: Dict[str, str], prefix: str) -> int:
        """
        Scans existing lookups to find the highest numerical suffix for a given prefix.
        
        This prevents overlapping IDs when generating new pseudonymized sequences.
        """
        max_val = 0
        for v in history_dict.values():
            if pd.isna(v):
                continue
            m = re.search(rf"^{re.escape(prefix)}(\d+)$", str(v))
            if m:
                max_val = max(max_val, int(m.group(1)))
        return max_val

    # ---- SellingPoint lookup ----
    def load_sp_lookup(self) -> Dict[str, str]:
        """Retrieves the Selling Point pseudonymization history."""
        return self._load_history(self.paths.ra_lookup, "Original_SellingPoint", "Pseudonymized_Value")

    def save_sp_lookup(self, df: pd.DataFrame) -> None:
        """Persists the updated Selling Point mapping to disk."""
        df.to_csv(self.paths.ra_lookup, index=False, sep=";")
        self.logger.log(f"💾 Saved SP lookup to {self.paths.ra_lookup}")

    # ---- RequestID lookup ----
    def load_req_lookup(self) -> Dict[str, str]:
        """Retrieves the Request ID pseudonymization history."""
        return self._load_history(self.paths.req_lookup, "Original_RequestID", "Pseudonymized_RequestID")

    def save_req_lookup(self, df: pd.DataFrame) -> None:
        """Persists the updated Request ID mapping to disk."""
        df.to_csv(self.paths.req_lookup, index=False, sep=";")
        self.logger.log(f"💾 Saved RequestID lookup to {self.paths.req_lookup}")

    # ---- Device/IDP lookup ----
    def save_dev_idp_lookup(self, df: pd.DataFrame) -> None:
        """Saves Device and IDP normalization tables for audit purposes."""
        df.to_csv(self.paths.dev_idp_lookup, index=False, sep=";")
        self.logger.log(f"💾 Saved Device/IDP lookup to {self.paths.dev_idp_lookup}")

    # ---- Cloud Orders lookup ----
    def load_cloud_orders_lookup(self) -> Dict[str, str]:
        """Retrieves Cloud Identity Orders anonymization history."""
        return self._load_history(self.paths.cloud_orders_lookup, "Original_Order_ID", "Anonymized_Order_ID")

    def save_cloud_orders_lookup(self, df: pd.DataFrame) -> None:
        """Persists Cloud Orders mapping."""
        df.to_csv(self.paths.cloud_orders_lookup, index=False, sep=";")
        self.logger.log(f"💾 Saved Cloud Orders lookup to {self.paths.cloud_orders_lookup}")

    def next_cloud_order_counter(self, cloud_map: Dict[str, str]) -> int:
        """
        Calculates the next available integer for Cloud Order ID generation.
        Ensures the sequence continues from the last used ID.
        """
        max_num = 99
        for v in cloud_map.values():
            if not v:
                continue
            m = re.match(r"^(\d{6})$", str(v))
            if m:
                max_num = max(max_num, int(m.group(1)))
        return max_num

    # Request_Status lookup
    def save_status_lookup(self, df: pd.DataFrame) -> None:
        """Saves the status translation lookup to ensure end-to-end traceability."""
        df.to_csv(self.paths.status_lookup, index=False, sep=";")
        self.logger.log(f"💾 Saved RequestStatus lookup to {self.paths.status_lookup}")
    
    # Request_Mode lookup
    def save_mode_lookup(self, df: pd.DataFrame) -> None:
        """Saves the mode translation lookup for reporting consistency."""
        df.to_csv(self.paths.mode_lookup, index=False, sep=";")
        self.logger.log(f"💾 Saved RequestStatus lookup to {self.paths.mode_lookup}")


# ===============================================================
# 6. PSEUDONYMIZATION / NORMALIZATION (POLARS)
# ===============================================================
class Pseudonymizer:
    """
    Core engine for data transformation, normalization, and security masking.

    This class leverages the Polars library for high-performance vectorized operations. 
    It handles complex business logic to categorize selling points, normalize 
    device strings, and generate GDPR-compliant pseudonymized identifiers.
    """
    SP_PREFIXES = ("Seller A", "PA", "Seller B", "Seller C", "Seller D")
    REQ_PREFIXES = ("SR", "S", "DR", "R", "DEV")

    def __init__(self, logger: AppLogger, lookups: LookupStore):
        """
        Initializes the Pseudonymizer with specialized logging and lookup persistence.
        """
        self.logger = logger
        self.lookups = lookups

    # 6.1 Selling Point
    def pseudonymize_selling_point_pl(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, pd.DataFrame]:
        """
        Categorizes and pseudonymizes the 'Selling_Point' column.

        Logic:
        - Assigns a category (Seller A-D, PA) based on business rules (e.g., GOV_LIST).
        - Generates a unique, persistent ID for each Selling Point.
        - Updates the historical lookup table to ensure cross-run consistency.
        """
        self.logger.log("🔒 [POLARS] Pseudonymizing Selling_Point...")

        if "Selling_Point" not in df.columns:
            raise RuntimeError("Column 'Selling_Point' is missing in REGISTRATIONS.")

        df = df.with_columns(pl.col("Selling_Point").fill_null("Unknown").alias("SP_Orig"))

        sp_map = self.lookups.load_sp_lookup()
        sp_counters = {p: self.lookups._max_counter_for_prefix(sp_map, p + "-") for p in self.SP_PREFIXES}

        unique_vals: List[str] = df.get_column("Selling_Point").fill_null("").unique().to_list()

        for auth in unique_vals:
            if auth == "" or auth in sp_map:
                continue
            u = str(auth).upper()
            if u and u[0].isdigit():
                p = "Seller A"
            elif any(w in u for w in GOV_LIST):
                p = "PA"
            elif any(w in u for w in SELLER_B_LIST):
                p = "Seller B"
            elif u.startswith("DAT"):
                p = "Seller C"
            else:
                p = "Seller D"
            sp_counters[p] += 1
            sp_map[auth] = f"{p}-{sp_counters[p]}"

        df = df.with_columns(pl.col("Selling_Point").replace(sp_map).alias("Selling_Point"))

        df_sp_lookup = (
            df.select(["SP_Orig", "Selling_Point"])
              .unique()
              .rename({"SP_Orig": "Original_SellingPoint", "Selling_Point": "Pseudonymized_Value"})
              .to_pandas()
        )
        self.lookups.save_sp_lookup(df_sp_lookup)
        return df, df_sp_lookup

    # 6.2 Device & IDP normalization
    def normalize_device_and_idp_pl(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, pd.DataFrame]:
        """
        Performs advanced string cleanup and normalization for Device and IDP types.

        This method uses a priority-based Regex replacement strategy (longer patterns first) 
        to ensure that technical strings are mapped to clean, report-ready categories.
        """
        if "Device_Type" not in df.columns:
            self.logger.log("ℹ️ [POLARS] 'Device_Type' not present: skipping.")
            empty = pd.DataFrame(columns=["Original_String", "Final_Device_IDP_Value"])
            return df, empty

        self.logger.log("📱 [POLARS] Normalizing Device_Type and IDP...")

        df = df.with_columns(
            pl.col("Device_Type")
              .fill_null("")
              .cast(pl.Utf8, strict=False)
              .str.to_uppercase()
              .str.strip_chars()
              .alias("Device_Orig_Temp")
        ).with_columns(pl.col("Device_Orig_Temp").alias("Device_Type"))

        sorted_devices = dict(sorted(DEVICE_REPLACEMENTS.items(), key=lambda x: len(x[0]), reverse=True))
        sorted_idps = dict(sorted(IDP_REPLACEMENTS.items(), key=lambda x: len(x[0]), reverse=True))

        for old_val, new_val in sorted_devices.items():
            pattern = re.escape(str(old_val).upper())
            df = df.with_columns(pl.col("Device_Type").str.replace_all(pattern, str(new_val)))

        for idp_key, idp_alias in sorted_idps.items():
            pattern = re.escape(str(idp_key).upper())
            df = df.with_columns(pl.col("Device_Type").str.replace_all(pattern, str(idp_alias)))

        df = df.with_columns(
            pl.when(~pl.col("Device_Type").str.contains("IDP"))
              .then(pl.col("Device_Type") + pl.lit(" IDP1"))
              .otherwise(pl.col("Device_Type"))
              .alias("Device_Type")
        )

        df = df.with_columns(pl.col("Device_Type").str.strip_chars())

        df_dev_idp_lookup = (
            df.select(["Device_Orig_Temp", "Device_Type"])
              .unique()
              .rename({"Device_Orig_Temp": "Original_String", "Device_Type": "Final_Device_IDP_Value"})
              .to_pandas()
        )
        self.lookups.save_dev_idp_lookup(df_dev_idp_lookup)
        return df, df_dev_idp_lookup

    # 6.3 Request_Status normalization
    def normalize_request_status_pl(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, pd.DataFrame]:
        """
        Translates raw system statuses into human-readable, normalized categories.

        Uses external mapping (STATUS_TRANS) to standardize workflow states for 
        accurate funnel analysis in Power BI.
        """
        if "Request_Status" not in df.columns:
            self.logger.log("ℹ️ [POLARS] 'Request_Status' not present: skipping.")
            empty = pd.DataFrame(columns=["Original_Status", "Normalized_Status"])
            return df, empty

        self.logger.log("📌 [POLARS] Normalizing Request_Status...")

        # Keep original (for lookup)
        df = df.with_columns(
            pl.col("Request_Status")
              .fill_null("")
              .cast(pl.Utf8, strict=False)
              .str.strip_chars()
              .alias("Request_Status_Orig")
        )

        # Normalize current value too (strip) then apply mapping
        df = df.with_columns(
            pl.col("Request_Status")
              .fill_null("")
              .cast(pl.Utf8, strict=False)
              .str.strip_chars()
              .replace(STATUS_TRANS)
              .alias("Request_Status")
        )

        df_status_lookup = (
            df.select(["Request_Status_Orig", "Request_Status"])
              .unique()
              .rename({"Request_Status_Orig": "Original_Status", "Request_Status": "Normalized_Status"})
              .to_pandas()
        )

        # Save CSV lookup for traceability
        self.lookups.save_status_lookup(df_status_lookup)

        return df, df_status_lookup

# 6.4 Mode normalization
    def normalize_mode_pl(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, pd.DataFrame]:
        """
        Standardizes the registration 'Mode' with an intelligent fallback mechanism.

        If a mode is missing or unrecognized, it defaults to 'Self Enroll' to 
        maintain data integrity for trend analysis.
        """
        if "Mode" not in df.columns:
            self.logger.log("ℹ️ [POLARS] 'Mode' not present: skipping.")
            empty = pd.DataFrame(columns=["Original_Mode", "Translated_Mode"])
            return df, empty

        self.logger.log("🔄 [POLARS] Translating Mode column (Fallback: Self Enroll)...")

        # 1. Initial cleanup and original backup for lookup
        df = df.with_columns(
            pl.col("Mode")
              .fill_null("Self Enroll")
              .cast(pl.Utf8)
              .str.strip_chars()
              .alias("Mode_Orig")
        )

        # 2. Translation with Fallback
        df = df.with_columns(
            pl.col("Mode_Orig")
              .replace(MODE_TRANS, default="Self Enroll") 
              .alias("Mode")
        )

        # 3. Creating a DataFrame for SQL/CSV Lookup
        df_mode_lookup = (
            df.select(["Mode_Orig", "Mode"])
              .unique()
              .rename({"Mode_Orig": "Original_Mode", "Mode": "Translated_Mode"})
              .to_pandas()
        )

        # Lookup saving
        self.lookups.save_mode_lookup(df_mode_lookup)
        
        return df, df_mode_lookup

    # 6.5 Request_ID pseudonymization
    def pseudonymize_request_id_pl(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, pd.DataFrame]:
        """
        Generates structured, pseudonymized Request IDs based on device categories.

        The process involves:
        1. Deriving a category (SR, S, DR, R, DEV) from device metadata.
        2. Assigning a zero-padded incremental ID (e.g., SR00000001).
        3. Maintaining strict 1:1 mapping with original IDs for data security.
        """
        self.logger.log("🧩 [POLARS] Pseudonymizing Request_ID with categories...")

        if "Request_ID" not in df.columns:
            raise RuntimeError("Column 'Request_ID' missing in REGISTRATIONS.")
        if "Device_Type" not in df.columns:
            raise RuntimeError("Column 'Device_Type' missing; cannot derive category.")

        df = df.with_columns(
            pl.when(pl.col("Device_Type").str.contains("RINNOVO SPID")).then(pl.lit("SR"))
             .when(pl.col("Device_Type").str.contains("SPID")).then(pl.lit("S"))
             .when(pl.col("Device_Type").str.contains("RINNOVO")).then(pl.lit("DR"))
             .when(pl.col("Device_Type").str.contains("MOBILE")).then(pl.lit("R"))
             .otherwise(pl.lit("DEV"))
             .alias("Id/Dev Category")
        )

        req_map = self.lookups.load_req_lookup()
        counters = {p: 0 for p in self.REQ_PREFIXES}

        # compute existing max per prefix
        for v in req_map.values():
            if not isinstance(v, str):
                continue
            for p in self.REQ_PREFIXES:
                m = re.match(rf"^{re.escape(p)}(\d+)$", v)
                if m:
                    counters[p] = max(counters[p], int(m.group(1)))

        new_ids: List[str] = []
        for rid, cat in df.select(["Request_ID", "Id/Dev Category"]).iter_rows():
            rid = "" if rid is None else str(rid)
            cat = "DEV" if cat is None else str(cat)
            if rid not in req_map:
                counters[cat] += 1
                req_map[rid] = f"{cat}{counters[cat]:08d}"
            new_ids.append(req_map[rid])

        df = df.with_columns(pl.Series("Request_ID", new_ids))

        df_req_lookup = pd.DataFrame(list(req_map.items()), columns=["Original_RequestID", "Pseudonymized_RequestID"])
        self.lookups.save_req_lookup(df_req_lookup)
        return df, df_req_lookup


# ===============================================================
# 7. ETL ORCHESTRATOR ENGINE
# ===============================================================
class ETLOrchestrator:
    """
    Main controller for the end-to-end Data Engineering pipeline.

    The Orchestrator coordinates environment setup, multi-source ingestion,
    business logic application (Pseudonymization), and high-performance loading 
    into Azure SQL and Parquet Data Lakes.
    """
    def __init__(self, paths: PathConfig, logger: AppLogger, db: Database, lookups: LookupStore):
        """
        Initializes the pipeline with configuration, logging, and database services.
        """
        self.paths = paths
        self.logger = logger
        self.db = db
        self.lookups = lookups
        self.pseudo = Pseudonymizer(logger, lookups)

    # ---------- Internal Utilities ----------
    def _read_csv_pl(self, path: str, default_sep: str = ";") -> pl.DataFrame:
        """
        Robust CSV ingestion using Polars with an adaptive separator fallback mechanism.
        Ensures all columns are read as Utf8 to prevent schema mismatch during initial load.
        """
        try:
            df = pl.read_csv(path, separator=default_sep, infer_schema_length=2000, ignore_errors=True)
        except Exception:
            df = pl.read_csv(path, separator=",", infer_schema_length=2000, ignore_errors=True)
        return df.with_columns([pl.all().cast(pl.Utf8, strict=False)])

    def _write_parquet_pl(self, df: pl.DataFrame, subdir: str, base_name: str) -> str:
        """
        Exports data to a Parquet Data Lake using Snappy compression for optimized storage, 
        providing a locally stored redundancy layer for data recovery and offline analysis.
        """
        out_dir = os.path.join(self.paths.parquet_dir, subdir)
        os.makedirs(out_dir, exist_ok=True)
        parquet_path = os.path.join(out_dir, f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        df.write_parquet(parquet_path, compression="snappy")
        return parquet_path

    # ---------- Time Intelligence Phase ----------
    def phase_retail_calendar(
        self,
        df_reg_pl: pl.DataFrame,
        df_prod_pl: Optional[pl.DataFrame],
    ) -> None:
        """
        Dynamically generates and updates the Retail Calendar dimension.

        Algorithm:
        1. Adaptive parsing: Handles multiple date formats (ISO, EU, SQL) across sources.
        2. Range calculation: Automatically detects date boundaries and extends them by 5 years.
        3. SQL MERGE: Uses a staging-to-production MERGE pattern to avoid duplicate entries.
        4. Localization: Maps Italian public holidays and generates weekend flags.
        """
        self.logger.log("🗓️ Creating / Updating Retail_Calendar")

        def parse_any_date(expr: pl.Expr) -> pl.Expr:
            """Multi-format date parsing utility for heterogeneous data sources."""
            return pl.coalesce([
                expr.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S,%3f", strict=False).dt.date(),
                expr.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).dt.date(),
                expr.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                expr.str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            ])

        collected_mins, collected_maxs = [], []

        for col in ["Compilation_Date", "Production_Date"]:
            if col in df_reg_pl.columns:
                res = df_reg_pl.select([
                    parse_any_date(pl.col(col)).min().alias("min"),
                    parse_any_date(pl.col(col)).max().alias("max"),
                ])
                if res["min"][0] is not None:
                    collected_mins.append(res["min"][0])
                if res["max"][0] is not None:
                    collected_maxs.append(res["max"][0])

        if df_prod_pl is not None and "Production_Date" in df_prod_pl.columns:
            res = df_prod_pl.select([
                parse_any_date(pl.col("Production_Date")).min().alias("min"),
                parse_any_date(pl.col("Production_Date")).max().alias("max"),
            ])
            if res["min"][0] is not None:
                collected_mins.append(res["min"][0])
            if res["max"][0] is not None:
                collected_maxs.append(res["max"][0])

        if not collected_mins:
            self.logger.log("⚠️ Retail_Calendar skipped: no valid dates found.")
            return

        # Logic: Ensure a 5-year forecast horizon for Power BI predictive modeling
        desired_min = pd.to_datetime(min(collected_mins)).replace(month=1, day=1)
        desired_max = (
            pd.to_datetime(max(collected_maxs)) + pd.DateOffset(years=5)
        ).replace(month=12, day=31)

        exists_sql = self.db.query_scalar("""
            SELECT CASE WHEN EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='Retail_Calendar'
            ) THEN 1 ELSE 0 END
        """)

        def build_calendar_df(start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> pd.DataFrame:
            df = pd.DataFrame({"Date": pd.date_range(start_dt, end_dt, freq="D")})
            df["Year"] = df["Date"].dt.year
            df["Month_Number"] = df["Date"].dt.month
            df["Month_Name"] = df["Date"].dt.month_name()
            df["Quarter"] = "Q" + df["Date"].dt.quarter.astype(str)
            df["Day_Number"] = df["Date"].dt.dayofweek + 1
            df["Day_Name"] = df["Date"].dt.day_name()
            df["ISO_Week"] = df["Date"].dt.isocalendar().week.astype(int)
            df["Is_Weekend"] = df["Day_Number"].isin([6, 7])
            df["_MM_DD"] = df["Date"].dt.strftime("%m-%d")
            df["Holiday_Name"] = df["_MM_DD"].map(ITALIAN_PUBLIC_HOLIDAYS).fillna("")
            df["Is_Public_Holiday"] = df["Holiday_Name"] != ""
            df.drop(columns="_MM_DD", inplace=True)
            return df

        if not exists_sql:
            df_full = build_calendar_df(desired_min, desired_max)
            self.db.to_sql(df_full, "Retail_Calendar", if_exists="replace", index=False)
            self.logger.log("✅ Retail_Calendar created")
            return

        min_sql = pd.to_datetime(
            self.db.query_scalar("SELECT MIN(CONVERT(date,[Date])) FROM dbo.Retail_Calendar")
        )
        max_sql = pd.to_datetime(
            self.db.query_scalar("SELECT MAX(CONVERT(date,[Date])) FROM dbo.Retail_Calendar")
        )

        parts = []
        if desired_min < min_sql:
            parts.append((desired_min, min_sql - pd.Timedelta(days=1)))
        if desired_max > max_sql:
            parts.append((max_sql + pd.Timedelta(days=1), desired_max))

        if not parts:
            self.logger.log("✅ Retail_Calendar already up to date")
            return

        df_new = pd.concat([build_calendar_df(s, e) for s, e in parts])
        self.db.to_sql(df_new, "STAGING_Retail_Calendar", if_exists="replace", index=False)

        self.db.execute("""       
            MERGE dbo.Retail_Calendar AS T
            USING dbo.STAGING_Retail_Calendar AS S
                ON CONVERT(date, T.[Date]) = CONVERT(date, S.[Date])
            WHEN NOT MATCHED THEN
                INSERT (
                    [Date],
                    [Year],
                    [Month_Number],
                    [Month_Name],
                    [Quarter],
                    [Day_Number],
                    [Day_Name],
                    [ISO_Week],
                    [Is_Weekend],
                    [Holiday_Name],
                    [Is_Public_Holiday]
                )
                VALUES (
                    S.[Date],
                    S.[Year],
                    S.[Month_Number],
                    S.[Month_Name],
                    S.[Quarter],
                    S.[Day_Number],
                    S.[Day_Name],
                    S.[ISO_Week],
                    S.[Is_Weekend],
                    S.[Holiday_Name],
                    S.[Is_Public_Holiday]
                );

            DROP TABLE dbo.STAGING_Retail_Calendar;
            """)


        self.logger.log("✅ Retail_Calendar increment merged")

    # ---------- System Cleanup ----------
    def maybe_wipe(self, mode_choice: str) -> None:
        """
        Security utility to reset the database and local lookups for 'Restore' operations.
        Ensures a clean slate for full re-runs while maintaining audit logs.
        """
        if mode_choice != "r":
            return
        ask = input("⚠️ Wipe SQL tables and local lookups? (y/n): ").strip().lower()
        if ask != "y":
            return

        self.logger.log("🧹 Dropping SQL tables and deleting local lookup files...")
        if self.db.engine is None:
            raise RuntimeError("DB Engine not initialized.")

        with self.db.engine.connect() as conn:
            tables_to_drop = [
                "Pseudonymized_db",
                "Cloud_Identity_Orders",
                "Italian_Province",
                "Retail_Calendar",
                "Lookup_SellingPoint",
                "Lookup_RequestID",
                "Lookup_Device_IDP",
                "Lookup_CloudOrders",
                "Lookup_RequestStatus",
            ]
            for t in tables_to_drop:
                conn.execute(text(f"DROP TABLE IF EXISTS dbo.[{t}]"))
            conn.commit()

        for p in [
            self.paths.ra_lookup,
            self.paths.req_lookup,
            self.paths.dev_idp_lookup,
            self.paths.cloud_orders_lookup,
            self.paths.status_lookup, 
        ]:
            if os.path.exists(p):
                os.remove(p)

        self.logger.log("✅ Cleanup complete.")

    # ===========================================================
    # PHASE 1: DATA INGESTION & CROSS-SOURCE MERGING
    # ===========================================================
    def phase_registrations(self, path_reg: str, path_prod: Optional[str], mode_choice: str) -> None:
        """
        Orchestrates Phase 1: Registration processing and Production data reconciliation.

        Key features:
        - Pre-anonymization merge: Joins Production dates to Registrations using a left-join.
        - Date precedence logic: Prioritizes verified Production data over Registration timestamps.
        - Full ETL stack: Ingests, Pseudonymizes, Normalizes, and Upserts data to SQL.
        """
        self.logger.log("⚙️ Phase 1: Registrations (merge Productions BEFORE anonymization)...")

        df_pl = self._read_csv_pl(path_reg, default_sep=";")
        self.logger.log(f"📥 Registrations rows after read: {df_pl.height}")

        # Normalize Request_ID
        if "Request_ID" in df_pl.columns:
            df_pl = df_pl.with_columns(pl.col("Request_ID").str.strip_chars().alias("Request_ID"))

        # Preserve original Production_Date from Registrations for comparison
        if "Production_Date" in df_pl.columns:
            df_pl = df_pl.rename({"Production_Date": "Production_Date_Reg"})
        else:
            df_pl = df_pl.with_columns(pl.lit(None).cast(pl.Utf8).alias("Production_Date_Reg"))

        df_prod_pl = None  # will hold grouped productions (Request_ID -> Production_Date_Prod)

        # Reconciliation Logic: Aligning production dates from multiple systems
        if path_prod:
            self.logger.log("🔗 Merging Productions on Request_ID (left join)...")
            df_prod = self._read_csv_pl(path_prod, default_sep=";")

            if "Request_ID" in df_prod.columns:
                df_prod = df_prod.with_columns(pl.col("Request_ID").str.strip_chars().alias("Request_ID"))

            missing = [c for c in ["Request_ID", "Production_Date"] if c not in df_prod.columns]
            if missing:
                raise RuntimeError(f"Productions file is missing columns: {missing}")

            # keep max Production_Date per Request_ID (as you already do)
            df_prod_pl = (
                df_prod.select(["Request_ID", "Production_Date"])
                    .group_by("Request_ID")
                    .agg(pl.col("Production_Date").max().alias("Production_Date_Prod"))
            )

            df_pl = df_pl.join(df_prod_pl, on="Request_ID", how="left")
            self.logger.log(f"✅ Rows after merge: {df_pl.height}")
        else:
            self.logger.log("ℹ️ No Productions selected: Production_Date_Prod will be null.")
            df_pl = df_pl.with_columns(pl.lit(None).cast(pl.Utf8).alias("Production_Date_Prod"))

        # --------- Adaptive parsing for DATE-level comparison ----------
        def parse_any_date(expr: pl.Expr) -> pl.Expr:
            e = expr.cast(pl.Utf8, strict=False).str.strip_chars()
            return pl.coalesce([
                e.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S,%3f", strict=False).dt.date(),
                e.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False).dt.date(),
                e.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
                e.str.strptime(pl.Date, "%d/%m/%Y", strict=False),
            ])

# -----------------------------------------------------------
        # BUSINESS RULE: DATA PRECEDENCE & RECONCILIATION
        # -----------------------------------------------------------
        # We prioritize Production_Date from the production system (Source of Truth)
        # over the registration system to ensure the highest accuracy for KPI calculations.
        # This handles cases where dates might differ between legacy systems.
        df_pl = df_pl.with_columns(
            pl.when(
                pl.col("Production_Date_Reg").is_null() |
                (pl.col("Production_Date_Reg").str.strip_chars() == "")
            )
            .then(pl.col("Production_Date_Prod"))

            .when(
                pl.col("Production_Date_Prod").is_null() |
                (pl.col("Production_Date_Prod").str.strip_chars() == "")
            )
            .then(pl.col("Production_Date_Reg"))

            .when(
                parse_any_date(pl.col("Production_Date_Reg")) != parse_any_date(pl.col("Production_Date_Prod"))
            )
            .then(pl.col("Production_Date_Prod"))

            .otherwise(pl.col("Production_Date_Reg"))
            .alias("Production_Date")
        )

        # Drop temp columns
        df_pl = df_pl.drop(["Production_Date_Reg", "Production_Date_Prod"])

        self.phase_retail_calendar(df_pl, df_prod)

        # Pseudonymize + Normalize
        df_pl, df_sp_lookup = self.pseudo.pseudonymize_selling_point_pl(df_pl)
        df_pl, df_dev_idp_lookup = self.pseudo.normalize_device_and_idp_pl(df_pl)

        # ✅ NEW: normalize Request_Status
        df_pl, df_status_lookup = self.pseudo.normalize_request_status_pl(df_pl)
        df_pl, df_mode_lookup = self.pseudo.normalize_mode_pl(df_pl)

        df_pl, df_req_lookup = self.pseudo.pseudonymize_request_id_pl(df_pl)

        # Preview
        preview_cols = [c for c in ["Selling_Point", "Request_ID", "Mode", "Device_Type", "Request_Status", "Production_Date"] if c in df_pl.columns]
        print("\n🔍 REGISTRATIONS PREVIEW:\n", df_pl.select(preview_cols).head(5).to_pandas())

        # Remove service columns
        drop_cols = [c for c in ["SP_Orig", "Device_Orig_Temp", "Request_Status_Orig", "Mode_Orig"] if c in df_pl.columns]
        df_final_pl = df_pl.drop(drop_cols)

        # Parquet
        parquet_path = self._write_parquet_pl(df_final_pl, "registrations", "Pseudonymized_db")
        self.logger.log(f"📦 Parquet exported: {parquet_path}")

        # SQL upload (pandas)
        if input("\n❓ Upload Registrations to SQL? (y/n): ").strip().lower() == "y":
            self.logger.log("📤 Uploading Registrations to SQL (PANDAS)...")
            df_final_pd = df_final_pl.to_pandas()

            if mode_choice == "r":
                self.db.to_sql(df_final_pd, "Pseudonymized_db", if_exists="replace", index=False, chunksize=10000)
            else:
                self.db.to_sql(df_final_pd, "STAGING_Reg", if_exists="replace", index=False)
                self.db.execute(
                    "DELETE T FROM dbo.[Pseudonymized_db] AS T "
                    "INNER JOIN dbo.[STAGING_Reg] AS S ON T.[Request_ID] = S.[Request_ID]"
                )
                self.db.execute("INSERT INTO dbo.[Pseudonymized_db] SELECT * FROM dbo.[STAGING_Reg]")
                self.db.execute("DROP TABLE dbo.[STAGING_Reg]")

            # Lookup tables
            self.db.to_sql(df_sp_lookup, "Lookup_SellingPoint", if_exists="replace", index=False)
            self.db.to_sql(df_req_lookup, "Lookup_RequestID", if_exists="replace", index=False)
            self.db.to_sql(df_dev_idp_lookup, "Lookup_Device_IDP", if_exists="replace", index=False)
            self.db.to_sql(df_status_lookup, "Lookup_RequestStatus", if_exists="replace", index=False)
            self.db.to_sql(df_mode_lookup, "Lookup_Mode", if_exists="replace", index=False)

            self.logger.log("✅ Registrations + lookups uploaded successfully.")

    # ===========================================================
    # PHASE 3: CLOUD IDENTITY ORDERS (ANON + LOOKUP)
    # ===========================================================
    def phase_cloud_identity_orders(self, path_cloud: str) -> None:
        self.logger.log("⚙️ Phase 3: Cloud Identity Orders (ANONYMIZATION + LOOKUP)...")
        """
        Processes Cloud Identity Orders using strict Anonymization protocols.
        Generates random noise for sensitive numerical columns and persists mappings.
        """
        df_pl = self._read_csv_pl(path_cloud, default_sep=";")
        n = df_pl.height
        self.logger.log(f"📥 Cloud Orders rows after read: {n}")
        if n == 0:
            self.logger.log("⚠️ Cloud Orders file empty. Skipping.")
            return

        if "Order_ID" not in df_pl.columns:
            raise RuntimeError("Cloud Orders: missing 'Order_ID' column.")

        cloud_map = self.lookups.load_cloud_orders_lookup()
        max_seen = self.lookups.next_cloud_order_counter(cloud_map)

        originals = df_pl.get_column("Order_ID").fill_null("").to_list()

        anon_order_ids: List[str] = []
        for oid in originals:
            key = str(oid)
            if key not in cloud_map:
                max_seen += 1
                cloud_map[key] = f"{max_seen:06d}"
            anon_order_ids.append(cloud_map[key])

        anon_col2 = [random.randint(1000, 9999) for _ in range(n)]

        start_d = datetime(2024, 5, 30)
        end_d = datetime(2025, 1, 31)
        delta_days = (end_d - start_d).days
        anon_dates = [(start_d + timedelta(days=random.randint(0, delta_days))).strftime("%d/%m/%Y") for _ in range(n)]

        df_pl = df_pl.with_columns([
            pl.Series("Order_ID", anon_order_ids),
            pl.Series("Column2", anon_col2),
            pl.Series("Order_Date", anon_dates),
        ])

        df_lookup = pd.DataFrame(
            [{"Original_Order_ID": k, "Anonymized_Order_ID": v} for k, v in cloud_map.items()]
        )
        self.lookups.save_cloud_orders_lookup(df_lookup)

        parquet_path = self._write_parquet_pl(df_pl, "cloud_identity", "Cloud_Identity_Orders_Anon")
        self.logger.log(f"📦 Parquet exported: {parquet_path}")

        self.logger.log("📤 Uploading Cloud Identity Orders to SQL (PANDAS)...")
        self.db.to_sql(df_pl.to_pandas(), "Cloud_Identity_Orders", if_exists="replace", index=False)

        self.db.to_sql(df_lookup, "Lookup_CloudOrders", if_exists="replace", index=False)
        self.logger.log("✅ Cloud Identity Orders + lookup uploaded successfully.")

    # ===========================================================
    # PHASE 4: GEO-DIMENSIONS (Italian Provinces)
    # ===========================================================
    def phase_provinces(self, path_prov: str) -> None:
        """
        Uploads and validates Italian Provinces geo-dimension table.
        Ensures consistent regional reporting in Power BI.
        """
        self.logger.log("⚙️ Phase 4: Italian Provinces (ALWAYS upload to SQL)...")

        df_pl = self._read_csv_pl(path_prov, default_sep=",")
        self.logger.log(f"📥 Provinces rows after read: {df_pl.height}")
        self.logger.log(f"📑 Provinces columns: {df_pl.columns}")

        if df_pl.height == 0 or len(df_pl.columns) == 0:
            raise RuntimeError("Italian Provinces: empty dataframe or no columns. Check CSV separator/content.")

        parquet_path = self._write_parquet_pl(df_pl, "provinces", "Italian_Province")
        self.logger.log(f"📦 Parquet exported: {parquet_path}")

        self.logger.log("📤 Uploading Italian_Province to SQL (PANDAS)...")
        self.db.to_sql(df_pl.to_pandas(), "Italian_Province", if_exists="replace", index=False)
        self.logger.log("✅ Italian_Province uploaded successfully.")

    # ===========================================================
    # MAIN EXECUTION RUNTIME
    # ===========================================================
    def run(self) -> None:
        """
        Launches the interactive ETL pipeline orchestration.
        
        This runtime manager handles:
        1. User Input: Mode selection (Full Restore vs Smart Append).
        2. Security: Dynamic injection of encrypted database credentials.
        3. Logic Flow: Triggering sequential phases for Registrations, Anonymized Orders, and Geo-Dimensions.
        """
        self.logger.log("🚀 STARTING ETL - POLARS (ANON + PARQUET) + PANDAS (SQL)")
        
        # Mode Selection: Full Restore (Rebuilds DB) vs Smart Append (Incremental Update)
        print("CHOOSE MODE:\n [r] Full Restore (Local Merge)\n [a] Smart Append (Azure Update)")
        mode_choice = input("Choice: ").lower().strip()

        # Secure Credential Management: Loading environment variables dynamically
        path_env = select_file_optional("Select Credentials (.env)", os.path.join(self.paths.input_dir, "Secrets"))
        if not path_env:
            self.logger.log("No .env selected. Exiting.")
            return

        self.db.load_env_from_file(path_env)
        self.db.connect_from_env()

        # Database Guard: Handles cleanup if 'Full Restore' is selected
        self.maybe_wipe(mode_choice)

        # Interactive Data Ingestion: User selects source files for the current run
        path_reg = select_file_optional("Select REGISTRATIONS", self.paths.input_dir)
        path_prod = select_file_optional("Select PRODUCTIONS (Request_ID -> Production_Date)", self.paths.input_dir)
        path_cloud = select_file_optional("Select CLOUD_IDENTITY_ORDERS", self.paths.input_dir)
        path_prov = select_file_optional("Select ITALIAN_PROVINCE", self.paths.input_dir)

        # Sequential Phases Execution with dependency checks
        # Each phase is executed only if the corresponding source file has been successfully selected, ensuring pipeline modularity.
        if path_reg:
            self.phase_registrations(path_reg, path_prod, mode_choice)
        if path_cloud:
            self.phase_cloud_identity_orders(path_cloud)
        if path_prov:
            self.phase_provinces(path_prov)

        self.logger.log("🎯 PIPELINE FINISHED SUCCESSFULLY.")


# ===============================================================
# 8. PIPELINE ENTRY POINT (MAIN)
# ===============================================================
def main():
    """
    Bootstrap and execution of the ETL application.
    
    This function initializes the core services (Paths, Logging, Database, 
    and Lookup persistence) and injects them into the Orchestrator. 
    It ensures the environment is correctly mapped before starting the runtime.
    """
    # A. Path Resolution: Identify project root and structure
    project_root = PathConfig.detect_project_root()
    paths = PathConfig(project_root=project_root)

    # B. Service Initialization: Logging and dynamic path injection
    logger = AppLogger(paths.logs_dir)

    if paths.input_dir not in sys.path:
        sys.path.append(paths.input_dir)

    # C. Data Infrastructure: Database handler and persistence store
    db = Database(logger)
    lookups = LookupStore(logger, paths)

    # D. Orchestration: Injecting dependencies and launching the process
    orchestrator = ETLOrchestrator(paths, logger, db, lookups)
    orchestrator.run()

if __name__ == "__main__":
    # Standard Python guard to prevent unintended execution during imports
    main()