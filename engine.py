"""
CastingIQ Adaptive Data Engine
==============================
Production-grade manufacturing data ingestion that handles real-world chaos:
messy CSVs, inconsistent Excel exports, mixed formats, encoding issues, and
column names that change between every MES export.

Built by Brian Crusoe | github.com/brcrusoe72

Usage:
    from engine import AdaptiveDataEngine
    engine = AdaptiveDataEngine()
    result = engine.ingest("messy_data.xlsx")
    print(result.quality_report)
    clean_df = result.dataframe
"""

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd

# Optional imports — graceful degradation
try:
    import chardet
except ImportError:
    chardet = None

try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None

try:
    import openpyxl  # noqa: F401 — needed by pandas for xlsx
except ImportError:
    pass


# ---------------------------------------------------------------------------
# Standard Schema — the canonical column names we map everything onto
# ---------------------------------------------------------------------------

STANDARD_SCHEMA: dict[str, dict[str, Any]] = {
    "timestamp":          {"type": "datetime", "aliases": [
        "date", "time", "datetime", "dt", "event_date", "event_time",
        "record_date", "record_time", "log_date", "log_time", "ts",
        "created_at", "created", "occurred_at", "event_timestamp",
        "production_date", "prod_date", "shift_date",
    ]},
    "equipment_id":       {"type": "categorical", "aliases": [
        "machine", "machine_id", "equipment", "equip_id", "equip",
        "furnace", "furnace_id", "line", "line_id", "station",
        "station_id", "asset", "asset_id", "resource", "workcenter",
        "work_center", "cell", "cell_id", "device", "device_id",
    ]},
    "part_id":            {"type": "categorical", "aliases": [
        "part", "part_number", "part_no", "part_num", "pn",
        "product", "product_id", "product_code", "item", "item_id",
        "item_number", "sku", "material", "material_id", "alloy",
        "casting", "casting_id", "mold", "mold_id", "job", "job_id",
        "work_order", "wo", "order_id",
    ]},
    "shift":              {"type": "categorical", "aliases": [
        "shift_name", "shift_id", "shift_num", "shift_number",
        "work_shift", "crew", "crew_id", "team", "team_id",
    ]},
    "operator":           {"type": "categorical", "aliases": [
        "operator_id", "operator_name", "op", "op_id", "worker",
        "worker_id", "employee", "employee_id", "emp_id", "user",
        "user_id", "technician", "tech", "personnel",
    ]},
    "quantity_produced":  {"type": "numeric", "aliases": [
        "qty_produced", "qty_prod", "parts_produced", "parts_cast",
        "production_count", "prod_count", "count", "total_parts",
        "total_produced", "output", "output_count", "pieces",
        "units", "units_produced", "quantity", "qty",
    ]},
    "quantity_good":      {"type": "numeric", "aliases": [
        "qty_good", "good_parts", "good_count", "parts_good",
        "passed", "pass_count", "accepted", "conforming",
        "first_pass", "good_units", "good",
    ]},
    "quantity_scrap":     {"type": "numeric", "aliases": [
        "qty_scrap", "scrap", "scrap_count", "parts_scrap",
        "rejected", "reject_count", "defective", "nonconforming",
        "nc_count", "bad_parts", "bad_count", "waste",
        "rework", "rework_count",
    ]},
    "downtime_minutes":   {"type": "numeric", "aliases": [
        "downtime_min", "dt_min", "downtime", "down_time",
        "dwn_time_min", "stop_time", "stop_duration",
        "duration_min", "duration_minutes", "lost_time",
        "idle_time", "idle_min", "outage_min",
        "downtime_hours", "dt_hours", "downtime_hrs",  # handled via unit detection
    ]},
    "downtime_reason":    {"type": "categorical", "aliases": [
        "dt_reason", "reason", "reason_code", "stop_reason",
        "failure_mode", "failure_reason", "cause", "root_cause",
        "downtime_category", "dt_category", "category",
        "fault", "fault_code", "alarm", "alarm_code",
        "event_type", "event_code",
    ]},
    "defect_type":        {"type": "categorical", "aliases": [
        "defect", "defect_code", "defect_category", "defect_class",
        "nc_type", "nonconformance", "rejection_reason",
        "reject_reason", "quality_issue", "issue_type",
    ]},
    "temperature":        {"type": "numeric", "aliases": [
        "temp", "temp_c", "temp_f", "temperature_c", "temperature_f",
        "pour_temp", "pour_temp_c", "pour_temperature",
        "mold_temp", "mold_preheat", "mold_preheat_c",
        "furnace_temp", "process_temp", "metal_temp",
    ]},
    "pressure":           {"type": "numeric", "aliases": [
        "press", "pressure_psi", "pressure_bar", "pressure_mpa",
        "vacuum", "vacuum_level", "chamber_pressure",
    ]},
    "cycle_time":         {"type": "numeric", "aliases": [
        "cycle_time_min", "cycle_min", "ct", "ct_min",
        "pour_duration", "pour_duration_min", "process_time",
        "run_time", "run_time_min", "takt", "takt_time",
    ]},
    "oee":                {"type": "numeric", "aliases": [
        "oee_pct", "oee_percent", "overall_equipment_effectiveness",
    ]},
    "availability":       {"type": "numeric", "aliases": [
        "avail", "avail_pct", "availability_pct", "uptime",
        "uptime_pct",
    ]},
    "performance":        {"type": "numeric", "aliases": [
        "perf", "perf_pct", "performance_pct", "efficiency",
        "speed_rate",
    ]},
    "quality_rate":       {"type": "numeric", "aliases": [
        "qual", "qual_pct", "quality_pct", "yield", "fty",
        "first_time_yield", "pass_rate",
    ]},
}

# Null-like values to normalize
NULL_VALUES = {
    "", "null", "none", "n/a", "#n/a", "na", "nan", "-", "--",
    ".", "missing", "undefined", "not available", "not applicable",
    "#value!", "#ref!", "#div/0!", "#name?", "inf", "-inf",
}


# ---------------------------------------------------------------------------
# Data classes for results
# ---------------------------------------------------------------------------

@dataclass
class ColumnMapping:
    """Result of mapping a raw column to the standard schema."""
    raw_name: str
    mapped_name: Optional[str]
    confidence: float  # 0.0 – 1.0
    detected_type: str  # datetime, numeric, categorical
    alternatives: list[tuple[str, float]] = field(default_factory=list)


@dataclass
class QualityScore:
    """Data quality assessment."""
    completeness: float      # 0-100
    consistency: float       # 0-100
    timeliness: float        # 0-100
    accuracy: float          # 0-100
    overall: float           # 0-100
    grade: str               # A-F
    details: dict = field(default_factory=dict)


@dataclass
class IngestResult:
    """Complete result of data ingestion."""
    dataframe: pd.DataFrame
    raw_dataframe: pd.DataFrame
    column_mappings: list[ColumnMapping]
    quality_score: QualityScore
    cleaning_log: list[str]
    outliers: dict[str, list[int]]  # column → row indices
    schema_fingerprint: str
    row_count_raw: int
    row_count_clean: int
    duplicates_removed: int


# ---------------------------------------------------------------------------
# Core Engine
# ---------------------------------------------------------------------------

class AdaptiveDataEngine:
    """
    Adaptive manufacturing data ingestion engine.
    
    Reads any CSV/Excel file, auto-detects column meanings via fuzzy matching,
    scores data quality, cleans and normalizes, and learns schema mappings
    for future files from the same source.
    """

    def __init__(self, schema_dir: Optional[str] = None):
        self.schema_dir = Path(schema_dir) if schema_dir else Path(__file__).parent / "data" / "schemas"
        self.schema_dir.mkdir(parents=True, exist_ok=True)
        self._cleaning_log: list[str] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(
        self,
        source,  # file path (str/Path), file-like object, or DataFrame
        schema_name: Optional[str] = None,
        sheet_name: Optional[str] = None,
    ) -> IngestResult:
        """
        Ingest a data file and return cleaned, mapped data.
        
        Args:
            source: Path to CSV/Excel, file-like upload, or existing DataFrame.
            schema_name: Name of a saved schema to apply (skips auto-detect).
            sheet_name: For Excel files, which sheet to read.
        
        Returns:
            IngestResult with cleaned DataFrame, mappings, and quality score.
        """
        self._cleaning_log = []

        # 1. Read raw data
        raw_df = self._read_source(source, sheet_name)
        self._log(f"Read {len(raw_df)} rows × {len(raw_df.columns)} columns")

        # 2. Normalize nulls across the entire frame
        raw_df = self._normalize_nulls(raw_df)

        # 3. Try saved schema or auto-detect
        fingerprint = self._fingerprint(raw_df)
        saved = self._load_schema(schema_name or fingerprint)

        if saved:
            self._log(f"Applied saved schema: {schema_name or fingerprint}")
            mappings = self._apply_saved_mappings(raw_df, saved)
        else:
            mappings = self._auto_detect_columns(raw_df)

        # 4. Build mapped DataFrame
        mapped_df = self._build_mapped_df(raw_df, mappings)

        # 5. Clean
        clean_df, dupes = self._clean(mapped_df)

        # 6. Detect outliers (on numeric cols)
        outliers = self._detect_outliers(clean_df)

        # 7. Quality scoring
        quality = self._score_quality(raw_df, clean_df, mappings)

        # 8. Save schema for future reuse
        self._save_schema(fingerprint, mappings)

        return IngestResult(
            dataframe=clean_df,
            raw_dataframe=raw_df,
            column_mappings=mappings,
            quality_score=quality,
            cleaning_log=list(self._cleaning_log),
            outliers=outliers,
            schema_fingerprint=fingerprint,
            row_count_raw=len(raw_df),
            row_count_clean=len(clean_df),
            duplicates_removed=dupes,
        )

    def ingest_batch(self, directory: str | Path) -> list[IngestResult]:
        """Ingest all CSV/Excel files in a directory."""
        directory = Path(directory)
        results = []
        for f in sorted(directory.iterdir()):
            if f.suffix.lower() in (".csv", ".xlsx", ".xls"):
                try:
                    results.append(self.ingest(f))
                    self._log(f"✓ Processed {f.name}")
                except Exception as e:
                    self._log(f"✗ Failed {f.name}: {e}")
        return results

    # ------------------------------------------------------------------
    # File reading with encoding detection
    # ------------------------------------------------------------------

    def _read_source(self, source, sheet_name=None) -> pd.DataFrame:
        # Already a DataFrame
        if isinstance(source, pd.DataFrame):
            return source.copy()

        # File-like (Streamlit upload)
        if hasattr(source, "read"):
            name = getattr(source, "name", "upload.csv")
            if name.lower().endswith((".xlsx", ".xls")):
                return self._read_excel(source, sheet_name)
            data = source.read()
            if isinstance(data, bytes):
                encoding = self._detect_encoding(data)
                data = data.decode(encoding, errors="replace")
                self._log(f"Detected encoding: {encoding}")
            from io import StringIO
            return pd.read_csv(StringIO(data), dtype=str, on_bad_lines="skip")

        # File path
        path = Path(source)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        if path.suffix.lower() in (".xlsx", ".xls"):
            return self._read_excel(path, sheet_name)

        # CSV with encoding detection
        raw_bytes = path.read_bytes()
        encoding = self._detect_encoding(raw_bytes)
        self._log(f"Detected encoding: {encoding}")

        return pd.read_csv(
            path, encoding=encoding, dtype=str, on_bad_lines="skip"
        )

    def _read_excel(self, source, sheet_name=None) -> pd.DataFrame:
        try:
            return pd.read_excel(
                source,
                sheet_name=sheet_name or 0,
                dtype=str,
                engine="openpyxl",
            )
        except Exception:
            # Fallback: try without openpyxl engine specification
            return pd.read_excel(source, sheet_name=sheet_name or 0, dtype=str)

    def _detect_encoding(self, raw_bytes: bytes) -> str:
        if chardet:
            result = chardet.detect(raw_bytes[:50000])
            enc = result.get("encoding", "utf-8") or "utf-8"
            # chardet sometimes returns odd names
            return enc.lower().replace("-", "").replace("_", "").\
                replace("iso8859", "iso-8859-").replace("windows", "cp").\
                replace("ascii", "utf-8") if "utf" not in enc.lower() else enc
        # Heuristic fallback
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                raw_bytes[:10000].decode(enc)
                return enc
            except (UnicodeDecodeError, LookupError):
                continue
        return "utf-8"

    # ------------------------------------------------------------------
    # Null normalization
    # ------------------------------------------------------------------

    def _normalize_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        count = 0
        for col in df.columns:
            mask = df[col].astype(str).str.strip().str.lower().isin(NULL_VALUES)
            n = mask.sum()
            if n > 0:
                df.loc[mask, col] = np.nan
                count += n
        if count:
            self._log(f"Normalized {count} null-like values to NaN")
        return df

    # ------------------------------------------------------------------
    # Column auto-detection & fuzzy matching
    # ------------------------------------------------------------------

    def _auto_detect_columns(self, df: pd.DataFrame) -> list[ColumnMapping]:
        mappings = []
        used_standards = set()

        for raw_col in df.columns:
            best_match, best_score, alternatives = self._fuzzy_match_column(
                raw_col, df[raw_col], used_standards
            )
            detected_type = self._detect_column_type(df[raw_col])

            if best_match and best_score >= 0.45:
                used_standards.add(best_match)

            mappings.append(ColumnMapping(
                raw_name=raw_col,
                mapped_name=best_match if best_score >= 0.45 else None,
                confidence=best_score,
                detected_type=detected_type,
                alternatives=alternatives[:5],
            ))

        self._log(f"Mapped {sum(1 for m in mappings if m.mapped_name)} of {len(mappings)} columns")
        low_conf = [m for m in mappings if m.mapped_name and m.confidence < 0.70]
        if low_conf:
            self._log(f"⚠ {len(low_conf)} columns mapped with <70% confidence — review recommended")

        return mappings

    def _fuzzy_match_column(
        self, raw_name: str, series: pd.Series, used: set
    ) -> tuple[Optional[str], float, list[tuple[str, float]]]:
        """Match a raw column name against the standard schema using fuzzy matching."""
        normalized = self._normalize_col_name(raw_name)
        scores: list[tuple[str, float]] = []

        for std_name, info in STANDARD_SCHEMA.items():
            if std_name in used:
                continue

            # Direct match
            if normalized == std_name:
                return std_name, 1.0, []

            # Alias match
            best_alias_score = 0.0
            for alias in info["aliases"]:
                norm_alias = self._normalize_col_name(alias)
                if normalized == norm_alias:
                    return std_name, 0.98, []
                # Subsequence / contains
                if norm_alias in normalized or normalized in norm_alias:
                    best_alias_score = max(best_alias_score, 0.85)
                # SequenceMatcher
                ratio = SequenceMatcher(None, normalized, norm_alias).ratio()
                best_alias_score = max(best_alias_score, ratio)

            # Also match against the standard name itself
            ratio = SequenceMatcher(None, normalized, std_name).ratio()
            best_alias_score = max(best_alias_score, ratio)

            # Boost score based on data content heuristics
            content_boost = self._content_type_boost(series, info["type"])
            final_score = min(best_alias_score + content_boost * 0.15, 1.0)

            scores.append((std_name, final_score))

        scores.sort(key=lambda x: x[1], reverse=True)
        if scores:
            return scores[0][0], scores[0][1], scores
        return None, 0.0, []

    @staticmethod
    def _normalize_col_name(name: str) -> str:
        """Normalize column name for comparison."""
        name = str(name).lower().strip()
        name = re.sub(r"[^a-z0-9]", "_", name)
        name = re.sub(r"_+", "_", name).strip("_")
        return name

    def _content_type_boost(self, series: pd.Series, expected_type: str) -> float:
        """Give a small confidence boost if column content matches expected type."""
        sample = series.dropna().head(100)
        if len(sample) == 0:
            return 0.0

        if expected_type == "datetime":
            return 1.0 if self._looks_like_dates(sample) else -0.3

        if expected_type == "numeric":
            numeric = pd.to_numeric(sample, errors="coerce")
            pct = numeric.notna().mean()
            return 0.5 if pct > 0.8 else (-0.2 if pct < 0.3 else 0.0)

        if expected_type == "categorical":
            nunique = sample.nunique()
            ratio = nunique / len(sample) if len(sample) > 0 else 1
            return 0.3 if ratio < 0.5 else 0.0

        return 0.0

    def _detect_column_type(self, series: pd.Series) -> str:
        """Detect whether a column is datetime, numeric, or categorical."""
        sample = series.dropna().astype(str).head(200)
        if len(sample) == 0:
            return "empty"

        if self._looks_like_dates(sample):
            return "datetime"

        numeric = pd.to_numeric(sample.str.replace(",", ""), errors="coerce")
        if numeric.notna().mean() > 0.7:
            return "numeric"

        return "categorical"

    @staticmethod
    def _looks_like_dates(sample: pd.Series) -> bool:
        """Test if a sample of values look like dates/timestamps.
        
        Uses regex patterns rather than dateutil.parse(fuzzy=True) to avoid
        false positives on equipment IDs like 'FURN-01' or part numbers.
        """
        # Strict regex: require patterns that unambiguously look like dates
        date_patterns = [
            r"\d{4}[-/.]\d{1,2}[-/.]\d{1,2}",          # 2025-01-15, 2025/01/15
            r"\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}",        # 01/15/2025, 15-01-25
            r"\d{1,2}\s+\w{3,9}\s+\d{2,4}",            # 18 January 2025
            r"\w{3,9}\s+\d{1,2},?\s+\d{2,4}",          # January 23 2025, Jan 16 2025
            r"\d{1,2}-\w{3}-\d{2,4}",                   # 17-Jan-2025
        ]
        combined = re.compile("|".join(date_patterns), re.IGNORECASE)
        hits = sum(1 for v in sample.astype(str) if combined.search(str(v)))
        return hits / len(sample) > 0.6

    # ------------------------------------------------------------------
    # Schema persistence
    # ------------------------------------------------------------------

    def _fingerprint(self, df: pd.DataFrame) -> str:
        """Create a fingerprint from column names to identify similar files."""
        cols = sorted(self._normalize_col_name(c) for c in df.columns)
        return hashlib.md5("|".join(cols).encode()).hexdigest()[:12]

    def _save_schema(self, fingerprint: str, mappings: list[ColumnMapping]):
        path = self.schema_dir / f"{fingerprint}.json"
        data = {
            "fingerprint": fingerprint,
            "created": datetime.now().isoformat(),
            "mappings": {
                m.raw_name: {
                    "mapped_name": m.mapped_name,
                    "confidence": m.confidence,
                    "detected_type": m.detected_type,
                }
                for m in mappings
            },
        }
        path.write_text(json.dumps(data, indent=2))

    def _load_schema(self, name: str) -> Optional[dict]:
        if not name:
            return None
        # Try exact filename
        path = self.schema_dir / f"{name}.json"
        if path.exists():
            return json.loads(path.read_text())
        # Try as a schema name (check all files)
        for f in self.schema_dir.glob("*.json"):
            try:
                data = json.loads(f.read_text())
                if data.get("fingerprint") == name:
                    return data
            except (json.JSONDecodeError, KeyError):
                continue
        return None

    def _apply_saved_mappings(self, df: pd.DataFrame, saved: dict) -> list[ColumnMapping]:
        mappings_data = saved.get("mappings", {})
        result = []
        for col in df.columns:
            if col in mappings_data:
                m = mappings_data[col]
                result.append(ColumnMapping(
                    raw_name=col,
                    mapped_name=m.get("mapped_name"),
                    confidence=m.get("confidence", 1.0),
                    detected_type=m.get("detected_type", "unknown"),
                ))
            else:
                # New column not in saved schema — auto-detect
                detected_type = self._detect_column_type(df[col])
                result.append(ColumnMapping(
                    raw_name=col,
                    mapped_name=None,
                    confidence=0.0,
                    detected_type=detected_type,
                ))
        return result

    def update_mapping(self, mappings: list[ColumnMapping], raw_name: str,
                       new_mapped_name: str) -> list[ColumnMapping]:
        """Update a column mapping (user correction). Returns updated list."""
        for m in mappings:
            if m.raw_name == raw_name:
                m.mapped_name = new_mapped_name
                m.confidence = 1.0  # User-confirmed
                break
        return mappings

    # ------------------------------------------------------------------
    # Build mapped DataFrame
    # ------------------------------------------------------------------

    def _build_mapped_df(self, raw_df: pd.DataFrame,
                         mappings: list[ColumnMapping]) -> pd.DataFrame:
        df = raw_df.copy()
        rename_map = {}
        for m in mappings:
            if m.mapped_name:
                rename_map[m.raw_name] = m.mapped_name
        df = df.rename(columns=rename_map)
        return df

    # ------------------------------------------------------------------
    # Cleaning pipeline
    # ------------------------------------------------------------------

    def _clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        df = df.copy()

        # 1. Parse datetime columns (only mapped timestamp cols or strong date detections)
        datetime_schema_cols = {k for k, v in STANDARD_SCHEMA.items() if v["type"] == "datetime"}
        for col in df.columns:
            if col in datetime_schema_cols:
                df[col] = self._parse_dates(df[col])
                self._log(f"Parsed dates in '{col}'")

        # 2. Convert numeric columns
        numeric_cols = [c for c in STANDARD_SCHEMA
                        if STANDARD_SCHEMA[c]["type"] == "numeric" and c in df.columns]
        for col in numeric_cols:
            df[col] = self._to_numeric(df[col])

        # Also convert any remaining columns that look numeric
        for col in df.columns:
            if col not in numeric_cols and col != "timestamp":
                sample = df[col].dropna().head(100)
                test = pd.to_numeric(sample.astype(str).str.replace(",", ""), errors="coerce")
                if len(sample) > 0 and test.notna().mean() > 0.8:
                    df[col] = self._to_numeric(df[col])

        # 3. Remove duplicates
        before = len(df)
        df = df.drop_duplicates()
        dupes = before - len(df)
        if dupes:
            self._log(f"Removed {dupes} duplicate rows")

        # 4. Unit detection / conversion
        df = self._detect_and_convert_units(df)

        # 5. Strip whitespace from string columns
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("nan", np.nan)

        return df, dupes

    def _parse_dates(self, series: pd.Series) -> pd.Series:
        """Parse dates trying multiple strategies."""
        import warnings
        # Strategy 1: pandas built-in (fast)
        for fmt in (None, "mixed"):
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    parsed = pd.to_datetime(series, format=fmt, errors="coerce", dayfirst=False)
                if parsed.notna().mean() > 0.7:
                    return parsed
            except (ValueError, TypeError):
                continue

        # Strategy 2: dateutil row-by-row (slow but thorough)
        if dateutil_parser:
            def safe_parse(val):
                try:
                    return dateutil_parser.parse(str(val), fuzzy=True)
                except (ValueError, OverflowError, TypeError):
                    return pd.NaT
            return series.apply(safe_parse)

        return pd.to_datetime(series, errors="coerce")

    @staticmethod
    def _to_numeric(series: pd.Series) -> pd.Series:
        """Convert to numeric, handling commas and European decimals."""
        s = series.astype(str).str.strip()
        # Remove currency symbols
        s = s.str.replace(r"[$€£¥]", "", regex=True)
        # Handle European decimal (comma) — but only if no period present
        # "1.234,56" → "1234.56" and "12,5" → "12.5"
        has_both = s.str.contains(r"\.", na=False) & s.str.contains(",", na=False)
        s = s.where(~has_both, s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False))
        s = s.where(has_both | ~s.str.contains(",", na=False),
                    s.str.replace(",", ".", regex=False))
        return pd.to_numeric(s, errors="coerce")

    def _detect_and_convert_units(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect likely unit mismatches and flag them."""
        # Temperature: if values > 500, likely °F — convert to °C
        if "temperature" in df.columns:
            temps = df["temperature"].dropna()
            if len(temps) > 0:
                median = temps.median()
                if median > 500:
                    self._log("Temperature values appear to be °F — converting to °C")
                    df["temperature"] = (df["temperature"] - 32) * 5 / 9
                    df["temperature_unit_converted"] = True

        # Downtime: if values suggest hours (all < 24 with decimals), convert to minutes
        if "downtime_minutes" in df.columns:
            dt_vals = df["downtime_minutes"].dropna()
            if len(dt_vals) > 10:
                if dt_vals.max() < 25 and dt_vals.mean() < 8:
                    self._log("Downtime values appear to be hours — converting to minutes")
                    df["downtime_minutes"] = df["downtime_minutes"] * 60

        return df

    # ------------------------------------------------------------------
    # Outlier detection (IQR)
    # ------------------------------------------------------------------

    def _detect_outliers(self, df: pd.DataFrame) -> dict[str, list[int]]:
        outliers = {}
        for col in df.select_dtypes(include=[np.number]).columns:
            vals = df[col].dropna()
            if len(vals) < 10:
                continue
            q1 = vals.quantile(0.25)
            q3 = vals.quantile(0.75)
            iqr = q3 - q1
            if iqr == 0:
                continue
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            mask = (df[col] < lower) | (df[col] > upper)
            flagged = df.index[mask & df[col].notna()].tolist()
            if flagged:
                outliers[col] = flagged
                self._log(f"Flagged {len(flagged)} outliers in '{col}'")
        return outliers

    # ------------------------------------------------------------------
    # Quality scoring
    # ------------------------------------------------------------------

    def _score_quality(self, raw_df: pd.DataFrame, clean_df: pd.DataFrame,
                       mappings: list[ColumnMapping]) -> QualityScore:
        details = {}

        # Completeness: average non-null rate
        completeness_per_col = {}
        for col in clean_df.columns:
            pct = clean_df[col].notna().mean() * 100
            completeness_per_col[col] = round(pct, 1)
        completeness = np.mean(list(completeness_per_col.values())) if completeness_per_col else 0
        details["completeness_by_column"] = completeness_per_col

        # Consistency: data type uniformity
        consistency_scores = []
        for col in clean_df.columns:
            sample = clean_df[col].dropna()
            if len(sample) == 0:
                continue
            if clean_df[col].dtype in (np.float64, np.int64, "datetime64[ns]"):
                consistency_scores.append(100)
            else:
                # Check if values are consistently formatted
                types = sample.apply(type).nunique()
                consistency_scores.append(100 if types == 1 else max(0, 100 - types * 20))
        consistency = np.mean(consistency_scores) if consistency_scores else 50

        # Timeliness: date coverage
        timeliness = 50  # default
        ts_cols = [c for c in clean_df.columns if clean_df[c].dtype == "datetime64[ns]"]
        if ts_cols:
            ts = clean_df[ts_cols[0]].dropna()
            if len(ts) > 1:
                span = (ts.max() - ts.min()).days
                if span > 0:
                    expected_rows = span  # rough: 1 row/day minimum
                    actual = len(ts)
                    coverage = min(actual / max(expected_rows, 1), 1.0)
                    timeliness = coverage * 100
                    details["date_range"] = f"{ts.min().date()} to {ts.max().date()}"
                    details["date_span_days"] = span

        # Accuracy: based on mapping confidence + outlier ratio
        avg_confidence = np.mean([m.confidence for m in mappings if m.mapped_name]) * 100 \
            if any(m.mapped_name for m in mappings) else 0
        mapped_ratio = sum(1 for m in mappings if m.mapped_name) / max(len(mappings), 1) * 100
        accuracy = (avg_confidence * 0.6 + mapped_ratio * 0.4)
        details["avg_mapping_confidence"] = round(avg_confidence, 1)
        details["columns_mapped_pct"] = round(mapped_ratio, 1)

        overall = (completeness * 0.3 + consistency * 0.25 +
                   timeliness * 0.2 + accuracy * 0.25)

        grade = (
            "A" if overall >= 90 else
            "B" if overall >= 75 else
            "C" if overall >= 60 else
            "D" if overall >= 45 else "F"
        )

        return QualityScore(
            completeness=round(completeness, 1),
            consistency=round(consistency, 1),
            timeliness=round(timeliness, 1),
            accuracy=round(accuracy, 1),
            overall=round(overall, 1),
            grade=grade,
            details=details,
        )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _log(self, msg: str):
        self._cleaning_log.append(msg)

    def rebuild_mapped_df(self, raw_df: pd.DataFrame,
                          mappings: list[ColumnMapping]) -> pd.DataFrame:
        """Rebuild mapped + cleaned DataFrame after user edits mappings."""
        mapped = self._build_mapped_df(raw_df, mappings)
        clean, _ = self._clean(mapped)
        return clean
