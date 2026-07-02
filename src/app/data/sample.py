"""
Deterministic sample data generator for DataGuardian demo mode.

All data is generated with a fixed random seed for reproducibility.
Dates are relative to 2026-06-26 (project reference date).
"""

from __future__ import annotations

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

_RNG = random.Random(42)

# ── Reference date ────────────────────────────────────────────────────────────
_BASE_DATE = datetime(2026, 6, 26)

# ── Domain constants ──────────────────────────────────────────────────────────
_STEWARDS = [
    "Sarah Mitchell",
    "James Chen",
    "Emma Davis",
    "Oliver Brown",
    "",  # unassigned
]

_SOURCES = ["customers", "orders", "products", "order_items"]

_BATCHES = [
    f"batch_2026{((_BASE_DATE - timedelta(days=d)).strftime('%m%d'))}"
    for d in [0, 3, 7, 14, 21, 28]
]

_STATUS_WEIGHTS = {
    "PENDING": 0.38,
    "APPROVED": 0.35,
    "REJECTED": 0.14,
    "CORRECTION_REQUESTED": 0.13,
}

_RULE_CATALOG: list[dict[str, Any]] = [
    {
        "rule_name": "not_null",
        "severity": "error",
        "template": "Value is required for column '{col}' but was NULL",
        "expected": "non-null",
        "actual_fn": lambda: "NULL",
    },
    {
        "rule_name": "unique",
        "severity": "error",
        "template": "Duplicate value detected in column '{col}'",
        "expected": "unique",
        "actual_fn": lambda: f"found {_RNG.randint(2, 8)} duplicates",
    },
    {
        "rule_name": "email",
        "severity": "error",
        "template": "Invalid email format in column '{col}'",
        "expected": "valid email address",
        "actual_fn": lambda: _RNG.choice(["john.doe@", "@@invalid.com", "nodomain", "spaces here@x.com"]),
    },
    {
        "rule_name": "positive_number",
        "severity": "error",
        "template": "Column '{col}' must be a positive number",
        "expected": "> 0",
        "actual_fn": lambda: str(round(_RNG.uniform(-500, 0), 2)),
    },
    {
        "rule_name": "future_date",
        "severity": "warning",
        "template": "Date in column '{col}' is in the future",
        "expected": f"<= {_BASE_DATE.strftime('%Y-%m-%d')}",
        "actual_fn": lambda: (_BASE_DATE + timedelta(days=_RNG.randint(1, 365))).strftime("%Y-%m-%d"),
    },
    {
        "rule_name": "allowed_values",
        "severity": "error",
        "template": "Value in column '{col}' is not in the allowed set",
        "expected": "one of the allowed values",
        "actual_fn": lambda: _RNG.choice(["UNKNOWN", "DRAFT", "ENTERPRISE_PLUS", "N/A", "TBD"]),
    },
    {
        "rule_name": "min_length",
        "severity": "warning",
        "template": "Column '{col}' value is shorter than the minimum required length",
        "expected": "min_length=3",
        "actual_fn": lambda: f"length={_RNG.randint(0, 2)}",
    },
    {
        "rule_name": "referential_integrity",
        "severity": "error",
        "template": "Foreign key in column '{col}' does not reference an existing record",
        "expected": "existing parent record",
        "actual_fn": lambda: f"ID {_RNG.randint(90000, 99999)} not found",
    },
    {
        "rule_name": "sql_expression",
        "severity": "warning",
        "template": "Custom expression failed for column '{col}'",
        "expected": "expression evaluates to TRUE",
        "actual_fn": lambda: "FALSE",
    },
]

_SOURCE_COLUMNS: dict[str, list[str]] = {
    "customers": ["email", "customer_id", "birth_date", "customer_segment", "annual_revenue", "phone_number", "country_code"],
    "orders": ["total_amount", "customer_id", "order_status", "shipping_address", "order_date", "payment_method"],
    "products": ["product_name", "unit_price", "product_id", "category", "sku", "weight_kg"],
    "order_items": ["quantity", "order_id", "product_id", "discount_pct", "unit_cost", "line_total"],
}

_RAW_RECORD_TEMPLATES: dict[str, list[dict[str, Any]]] = {
    "customers": [
        {"customer_id": "C{n:06d}", "first_name": "John", "last_name": "Smith", "email": "john.smith@", "country_code": "GB", "customer_segment": "Enterprise", "annual_revenue": 125000.0, "is_active": True},
        {"customer_id": "C{n:06d}", "first_name": "Maria", "last_name": "Garcia", "email": None, "country_code": "ES", "customer_segment": "SMB", "annual_revenue": 42000.0, "is_active": True},
        {"customer_id": "C{n:06d}", "first_name": "Wei", "last_name": "Zhang", "email": "w.zhang@@company.com", "country_code": "CN", "customer_segment": "ENTERPRISE_PLUS", "annual_revenue": -500.0, "is_active": False},
        {"customer_id": "C{n:06d}", "first_name": "Anna", "last_name": "Müller", "email": "anna.muller@corp.de", "country_code": "DE", "customer_segment": "Partner", "birth_date": "2030-01-15", "annual_revenue": 88000.0, "is_active": True},
        {"customer_id": None, "first_name": "Robert", "last_name": "Johnson", "email": "r.johnson@email.com", "country_code": "US", "customer_segment": "Enterprise", "annual_revenue": 220000.0, "is_active": True},
    ],
    "orders": [
        {"order_id": "ORD{n:08d}", "customer_id": "C{c:06d}", "order_date": "2026-06-15", "total_amount": -145.50, "order_status": "DRAFT", "payment_method": "CREDIT_CARD", "shipping_address": "123 Main St"},
        {"order_id": "ORD{n:08d}", "customer_id": None, "order_date": "2026-06-18", "total_amount": 289.99, "order_status": "PENDING", "payment_method": "WIRE_TRANSFER", "shipping_address": "45 Oak Ave"},
        {"order_id": "ORD{n:08d}", "customer_id": "C{c:06d}", "order_date": "2030-12-01", "total_amount": 1200.00, "order_status": "CONFIRMED", "payment_method": None, "shipping_address": ""},
        {"order_id": "ORD{n:08d}", "customer_id": "C{c:06d}", "order_date": "2026-06-20", "total_amount": 0.00, "order_status": "SHIPPED", "payment_method": "PAYPAL", "shipping_address": "78 River Rd"},
        {"order_id": "ORD{n:08d}", "customer_id": "C{c:06d}", "order_date": "2026-06-22", "total_amount": 4500.00, "order_status": "UNKNOWN", "payment_method": "CREDIT_CARD", "shipping_address": "22 Park Blvd"},
    ],
    "products": [
        {"product_id": "PRD{n:06d}", "product_name": None, "sku": "SKU-{n:05d}", "category": "Electronics", "unit_price": 299.99, "weight_kg": 0.85, "is_active": True},
        {"product_id": "PRD{n:06d}", "product_name": "A", "sku": "SKU-{n:05d}", "category": "TBD", "unit_price": -19.99, "weight_kg": 0.12, "is_active": True},
        {"product_id": None, "product_name": "Premium Widget Pro", "sku": "SKU-{n:05d}", "category": "Hardware", "unit_price": 89.00, "weight_kg": 1.5, "is_active": False},
        {"product_id": "PRD{n:06d}", "product_name": "DataSync Adapter", "sku": None, "category": "Software", "unit_price": 0.0, "weight_kg": None, "is_active": True},
        {"product_id": "PRD{n:06d}", "product_name": "Cloud Monitor X", "sku": "SKU-{n:05d}", "category": "N/A", "unit_price": 1499.00, "weight_kg": 3.2, "is_active": True},
    ],
    "order_items": [
        {"line_id": "LN{n:08d}", "order_id": "ORD{o:08d}", "product_id": "PRD{p:06d}", "quantity": -1, "unit_cost": 25.99, "discount_pct": 10.0, "line_total": -25.99},
        {"line_id": "LN{n:08d}", "order_id": None, "product_id": "PRD{p:06d}", "quantity": 5, "unit_cost": 149.00, "discount_pct": 5.0, "line_total": 707.75},
        {"line_id": "LN{n:08d}", "order_id": "ORD{o:08d}", "product_id": "PRD{p:06d}", "quantity": 0, "unit_cost": 0.0, "discount_pct": 150.0, "line_total": 0.0},
        {"line_id": "LN{n:08d}", "order_id": "ORD{o:08d}", "product_id": None, "quantity": 2, "unit_cost": 99.50, "discount_pct": 0.0, "line_total": 199.00},
    ],
}

_SOURCE_COUNTS = {"customers": 62, "orders": 78, "products": 38, "order_items": 22}

_COMMENT_TEMPLATES = [
    "Reviewed the raw record — the email format issue appears to be a data entry error from the source system.",
    "Confirmed with the upstream team. This record will be corrected in the next batch.",
    "DQ violation is expected for legacy records migrated before 2025. Approving with documented exception.",
    "The customer_id is missing because this is a guest checkout. This is a known business rule.",
    "Flagged for source system correction. The vendor has been notified.",
    "Cross-referenced with CRM system — this record appears to be a test entry. Rejecting.",
    "Annual revenue field is populated from a different currency. Conversion factor not applied at source.",
    "Duplicate detected — investigating whether this is a legitimate re-order or a system glitch.",
    "Assigned to data engineering team for investigation. The NULL value appears systemic.",
    "The future date in birth_date is clearly a data entry error (2030 vs 2003). Correcting.",
    "Source system API returned this record with truncated email. Infrastructure team notified.",
    "Batch reprocessing scheduled for tomorrow. Hold approval until then.",
    "Verified against paper records — the value is correct. Overriding the DQ rule for this record.",
    "This is the third consecutive batch with this violation. Escalating to data owner.",
    "Approved after manual verification with the regional operations team.",
]

_REPLY_TEMPLATES = [
    "Thanks for the context. Proceeding with your recommendation.",
    "Confirmed — I've updated the source record and it will be corrected in the next run.",
    "Agreed. Marking as resolved on our end.",
    "Can you provide the CRM reference number for audit purposes?",
    "Got it. I'll notify the compliance team as well.",
    "The engineering ticket has been raised: DG-4821.",
    "Understood. Documenting this as a known exception.",
]


# ── Internal helpers ──────────────────────────────────────────────────────────

def _uid() -> str:
    return str(uuid.UUID(int=_RNG.getrandbits(128)))


def _random_status() -> str:
    statuses = list(_STATUS_WEIGHTS.keys())
    weights = list(_STATUS_WEIGHTS.values())
    return _RNG.choices(statuses, weights=weights, k=1)[0]


def _random_steward(exclude_empty_prob: float = 0.15) -> str:
    if _RNG.random() < exclude_empty_prob:
        return ""
    return _RNG.choice(_STEWARDS[:4])


def _random_days_ago(min_days: int = 0, max_days: int = 28) -> datetime:
    return _BASE_DATE - timedelta(days=_RNG.randint(min_days, max_days), hours=_RNG.randint(0, 23), minutes=_RNG.randint(0, 59))


def _build_failed_rules(source: str, count: int) -> list[dict[str, Any]]:
    cols = _SOURCE_COLUMNS.get(source, ["value"])
    selected_rules = _RNG.sample(_RULE_CATALOG, min(count, len(_RULE_CATALOG)))
    result = []
    for rule_def in selected_rules:
        col = _RNG.choice(cols)
        result.append({
            "rule_name": rule_def["rule_name"],
            "column_name": col,
            "severity": rule_def["severity"],
            "message": rule_def["template"].replace("{col}", col),
            "expected": rule_def["expected"],
            "actual": rule_def["actual_fn"](),
        })
    return result


def _build_raw_record(source: str, seq: int) -> dict[str, Any]:
    templates = _RAW_RECORD_TEMPLATES.get(source, [{"id": seq}])
    template = _RNG.choice(templates)
    record: dict[str, Any] = {}
    for k, v in template.items():
        if isinstance(v, str):
            record[k] = v.format(n=seq, c=_RNG.randint(1, 5000), o=_RNG.randint(1, 9999), p=_RNG.randint(1, 999))
        else:
            record[k] = v
    record["_batch_id"] = _RNG.choice(_BATCHES)
    record["_load_date"] = _random_days_ago(0, 28).strftime("%Y-%m-%d")
    return record


# ── Public generators ─────────────────────────────────────────────────────────

def _gen_pipeline_runs() -> pd.DataFrame:
    rows = []
    run_counter = 1
    for source in _SOURCES:
        for batch in _BATCHES:
            batch_date_str = batch.replace("batch_2026", "")
            month = int(batch_date_str[:2])
            day = int(batch_date_str[2:])
            start = datetime(2026, month, day, _RNG.randint(1, 5), _RNG.randint(0, 59))
            duration = _RNG.uniform(45, 420)
            end = start + timedelta(seconds=duration)
            bronze = _RNG.randint(800, 12000)
            failed = _RNG.randint(10, int(bronze * 0.12))
            silver = bronze - failed
            has_error = _RNG.random() < 0.07
            status = "FAILED" if has_error else "SUCCESS"
            rows.append({
                "run_id": f"run_{run_counter:05d}",
                "source_name": source,
                "batch_id": batch,
                "status": status,
                "start_time": start,
                "end_time": end if not has_error else start + timedelta(seconds=_RNG.uniform(5, 30)),
                "duration_seconds": round(duration, 1),
                "bronze_rows_read": bronze,
                "silver_rows_written": 0 if has_error else silver,
                "failed_rows": 0 if has_error else failed,
                "dq_score": None if has_error else round(silver / bronze, 4),
                "schema_violations": _RNG.randint(0, 3),
                "contract_violations": _RNG.randint(0, 2),
                "error_message": "SparkException: Task failed after 3 retries" if has_error else None,
            })
            run_counter += 1
    return pd.DataFrame(rows)


def _gen_stewardship_records() -> pd.DataFrame:
    rows = []
    seq = 1000
    for source, total in _SOURCE_COUNTS.items():
        run_pool = [f"run_{i:05d}" for i in _RNG.sample(range(1, 25), min(6, 24))]
        for i in range(total):
            status = _random_status()
            violation_count = _RNG.randint(1, 4)
            created_at = _random_days_ago(1, 28)
            reviewed_at = None
            reviewed_by = None
            if status != "PENDING":
                reviewed_at = created_at + timedelta(hours=_RNG.randint(2, 72))
                reviewed_by = _RNG.choice(_STEWARDS[:4])
            batch = _RNG.choice(_BATCHES)
            rows.append({
                "record_id": _uid(),
                "run_id": _RNG.choice(run_pool),
                "source_name": source,
                "batch_id": batch,
                "table_name": f"silver.{source}",
                "dq_score": round(_RNG.uniform(0.40, 0.82), 4),
                "status": status,
                "assigned_to": _random_steward() if status in ("PENDING", "CORRECTION_REQUESTED") else (reviewed_by or ""),
                "violation_count": violation_count,
                "failed_rules": json.dumps(_build_failed_rules(source, violation_count)),
                "raw_record": json.dumps(_build_raw_record(source, seq)),
                "ingested_at": created_at - timedelta(minutes=_RNG.randint(1, 60)),
                "created_at": created_at,
                "reviewed_at": reviewed_at,
                "reviewed_by": reviewed_by,
                "updated_at": reviewed_at or created_at,
            })
            seq += 1
    return pd.DataFrame(rows)


def _gen_stewardship_actions(records: pd.DataFrame) -> pd.DataFrame:
    rows = []
    action_types_for_status = {
        "APPROVED": ["APPROVE"],
        "REJECTED": ["REJECT"],
        "CORRECTION_REQUESTED": ["REQUEST_CORRECTION"],
        "PENDING": [],
    }
    for _, rec in records.iterrows():
        status = rec["status"]
        actions_for_rec = action_types_for_status.get(status, [])
        if not actions_for_rec:
            continue
        reviewed_at = rec["reviewed_at"] or rec["created_at"]
        steward = rec["reviewed_by"] or _RNG.choice(_STEWARDS[:4])
        # Optional: assign action before the final action
        if _RNG.random() < 0.45:
            rows.append({
                "action_id": _uid(),
                "record_id": rec["record_id"],
                "action_type": "ASSIGN",
                "performed_by": _RNG.choice(_STEWARDS[:4]),
                "comment": f"Assigned to {steward} for review",
                "assigned_to": steward,
                "previous_status": "PENDING",
                "new_status": "PENDING",
                "action_timestamp": rec["created_at"] + timedelta(minutes=_RNG.randint(5, 120)),
                "metadata": json.dumps({}),
            })
        # The primary action
        action_type = actions_for_rec[0]
        prev = "PENDING"
        new_status = status
        comment_pool = {
            "APPROVE": [
                "Record verified against source system. Values are correct.",
                "Manual review complete — approving with documented DQ exception.",
                "Confirmed with data owner. Approved.",
                "All violations are within acceptable tolerance. Approved.",
            ],
            "REJECT": [
                "Record contains critical data integrity violations. Rejected.",
                "Duplicate detected — rejecting to avoid downstream corruption.",
                "Cross-referenced with CRM — this is a test record. Rejected.",
                "Source system error confirmed. Rejecting pending system fix.",
            ],
            "REQUEST_CORRECTION": [
                "Email format violation requires correction at source system.",
                "Requested upstream team to correct the NULL customer_id.",
                "Foreign key violation — please re-submit with valid reference.",
                "Date field appears to be a data entry error. Requesting correction.",
            ],
        }
        rows.append({
            "action_id": _uid(),
            "record_id": rec["record_id"],
            "action_type": action_type,
            "performed_by": steward,
            "comment": _RNG.choice(comment_pool.get(action_type, ["Action taken."])),
            "assigned_to": steward,
            "previous_status": prev,
            "new_status": new_status,
            "action_timestamp": reviewed_at,
            "metadata": json.dumps({"dq_score": rec["dq_score"]}),
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "action_id", "record_id", "action_type", "performed_by", "comment",
        "assigned_to", "previous_status", "new_status", "action_timestamp", "metadata",
    ])


def _gen_comments(records: pd.DataFrame) -> pd.DataFrame:
    rows = []
    eligible = records[records["status"].isin(["APPROVED", "REJECTED", "CORRECTION_REQUESTED"])]
    sampled = eligible.sample(frac=0.35, random_state=42)
    for _, rec in sampled.iterrows():
        steward = rec["reviewed_by"] or _RNG.choice(_STEWARDS[:4])
        ts = rec["created_at"] + timedelta(hours=_RNG.randint(1, 48))
        cid = _uid()
        rows.append({
            "comment_id": cid,
            "record_id": rec["record_id"],
            "parent_comment_id": None,
            "author": steward,
            "message": _RNG.choice(_COMMENT_TEMPLATES),
            "status": "ACTIVE",
            "created_at": ts,
        })
        # 40% chance of a reply
        if _RNG.random() < 0.4:
            rows.append({
                "comment_id": _uid(),
                "record_id": rec["record_id"],
                "parent_comment_id": cid,
                "author": _RNG.choice([s for s in _STEWARDS[:4] if s != steward]),
                "message": _RNG.choice(_REPLY_TEMPLATES),
                "status": "ACTIVE",
                "created_at": ts + timedelta(hours=_RNG.randint(1, 24)),
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "comment_id", "record_id", "parent_comment_id", "author", "message", "status", "created_at",
    ])


def _gen_audit_log(actions: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, action in actions.iterrows():
        rows.append({
            "audit_id": _uid(),
            "entity_type": "stewardship_record",
            "entity_id": action["record_id"],
            "operation": action["action_type"],
            "performed_by": action["performed_by"],
            "details": json.dumps({
                "action_id": action["action_id"],
                "previous_status": action["previous_status"],
                "new_status": action["new_status"],
                "comment": action["comment"],
            }),
            "audit_timestamp": action["action_timestamp"],
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "audit_id", "entity_type", "entity_id", "operation", "performed_by", "details", "audit_timestamp",
    ])


def generate_sample_data() -> dict[str, pd.DataFrame]:
    """Build all in-memory DataFrames for demo mode. Deterministic (seed=42)."""
    # Reset seed so every call produces identical data regardless of import order
    _RNG.seed(42)
    records = _gen_stewardship_records()
    pipeline_runs = _gen_pipeline_runs()
    actions = _gen_stewardship_actions(records)
    comments = _gen_comments(records)
    audit_log = _gen_audit_log(actions)
    return {
        "stewardship_records": records,
        "stewardship_actions": actions,
        "comments": comments,
        "audit_log": audit_log,
        "pipeline_runs": pipeline_runs,
    }
