"""
CDM Filter Transformer
=======================
Parses a raw CDM JSON payload, filters the records under a given object_type
using a list of filter predicates (dot-notation fields, operators, values),
and returns a new CDM structure.  The original metadata and audit objects are
inherited untouched; only metadata.batch and metadata.lineage are updated.

Input parameters
----------------
raw_cdm        : str   – JSON string of the full CDM payload
Filters        : list  – each element is a dict with {field, value, operator}
"""

import json
import hashlib
import copy
from typing import Any


# ---------------------------------------------------------------------------
# 1) Dot-notation resolver
# ---------------------------------------------------------------------------
def _resolve_dot_path(record: dict, path: str) -> Any:
    """Traverse *record* following a dot-separated path.

    Example
    -------
    >>> _resolve_dot_path({"core": {"status": "active"}}, "core.status")
    'active'
    """
    current = record
    for segment in path.split("."):
        if isinstance(current, dict) and segment in current:
            current = current[segment]
        else:
            return None          # path does not exist → treat as None
    return current


# ---------------------------------------------------------------------------
# 2) Single-predicate evaluator
# ---------------------------------------------------------------------------
def _evaluate_filter(record: dict, flt: dict) -> bool:
    """Return True when *record* satisfies the filter predicate *flt*.

    Supported operators
    -------------------
    =  !=  >  >=  <  <=  is in  not in  is null  is not null
    """
    raw_field: str = flt["field"]
    operator: str  = flt["operator"].strip().lower()
    filter_value   = flt.get("value")

    # Strip the first segment (array key like "departments") since we're
    # resolving against individual records, not the full CDM.
    parts = raw_field.split(".", 1)
    field = parts[1] if len(parts) > 1 else parts[0]

    actual_value = _resolve_dot_path(record, field)

    # --- nullity / emptiness checks (value-independent) ---------------------
    # Considers a field empty when it is None, an empty string, an empty
    # list, or an empty dict.
    if operator == "is null":
        if actual_value is None:
            return True
        if isinstance(actual_value, (str, list, dict)) and len(actual_value) == 0:
            return True
        return False
    if operator == "is not null":
        if actual_value is None:
            return False
        if isinstance(actual_value, (str, list, dict)) and len(actual_value) == 0:
            return False
        return True

    # If the resolved value is None for non-null operators → no match
    if actual_value is None:
        return False

    # --- membership checks --------------------------------------------------
    if operator == "is in":
        # filter_value should be a list (or comma-separated string)
        collection = filter_value if isinstance(filter_value, list) else [
            v.strip() for v in str(filter_value).split(",")
        ]
        return str(actual_value) in [str(v) for v in collection]

    if operator == "not in":
        collection = filter_value if isinstance(filter_value, list) else [
            v.strip() for v in str(filter_value).split(",")
        ]
        return str(actual_value) not in [str(v) for v in collection]

    # --- comparison / equality operators ------------------------------------
    # Attempt numeric coercion so that ">" works on numbers stored as strings
    def _coerce(a, b):
        try:
            return float(a), float(b)
        except (ValueError, TypeError):
            return str(a), str(b)

    cmp_actual, cmp_filter = _coerce(actual_value, filter_value)

    if operator == "=" or operator == "" or not operator:
        return cmp_actual == cmp_filter
    if operator == "!=":
        return cmp_actual != cmp_filter
    if operator == ">":
        return cmp_actual > cmp_filter
    if operator == ">=":
        return cmp_actual >= cmp_filter
    if operator == "<":
        return cmp_actual < cmp_filter
    if operator == "<=":
        return cmp_actual <= cmp_filter

    raise ValueError(f"Unsupported operator: '{flt['operator']}'")


# ---------------------------------------------------------------------------
# 3) Main transformer
# ---------------------------------------------------------------------------
def main(
    input
) -> dict:
    """Apply *filters* to the *object_type* array inside *raw_cdm* and return
    a new CDM payload wrapped in ``{"filtered_cdm": "<json string>"}``.
    raw_cdm: str,
    recipe_id: str,
    job_id: str,
    workato_url: str,
    source_event: str,
    source_system: str,
    object_type: str,
    Filters: list[dict]
    """
    raw_cdm = input.get("raw_cdm", "")
    Filters = input.get("Filters", [])

    # ---- Step 1: Parse the raw CDM JSON string ----------------------------
    cdm: dict = json.loads(raw_cdm)

    # ---- Step 2: Extract the data array using object_type -----------------
    object_type = Filters[0].get('field').split(".")[0]
    if object_type not in cdm:
        raise KeyError(
            f"object_type '{object_type}' not found in CDM payload. "
            f"Available keys: {list(cdm.keys())}"
        )

    source_records: list[dict] = cdm[object_type]

    # ---- Step 3 & 4: Apply every filter sequentially ----------------------
    filtered_records = list(source_records)  # shallow copy of the list
    for flt in Filters:
        filtered_records = [
            rec for rec in filtered_records if _evaluate_filter(rec, flt)
        ]

    # ---- Build the new CDM shell ------------------------------------------
    # Inherit the entire metadata and audit from the original CDM untouched;
    # only metadata.batch and metadata.lineage are updated below.
    new_cdm: dict = {
        "metadata": copy.deepcopy(cdm.get("metadata", {})),
        object_type: filtered_records,
        "audit": copy.deepcopy(cdm.get("audit", {})),
    }

    # Convenience aliases
    old_meta = cdm.get("metadata", {})
    new_meta = new_cdm["metadata"]

    # ---- Step 5: Update metadata.lineage ----------------------------------
    old_msg_id   = old_meta.get("message", {}).get("msg_id", "")
    old_batch_id = old_meta.get("batch", {}).get("batch_id", "")

    # Build the existing lineage parent_batch_ids chain (append new parent)
    existing_parent_batches = (
        old_meta.get("lineage", {}).get("parent_batch_ids", "")
    )
    if existing_parent_batches:
        parent_batch_ids = f"{existing_parent_batches},{old_batch_id}"
    else:
        parent_batch_ids = old_batch_id

    # Build the transformation_step description
    transformation_steps = []
    for flt in Filters:
        field    = flt["field"]
        operator = flt["operator"]
        value    = flt.get("value", "")
        transformation_steps.append(
            f'filter => "{object_type}[{field}] {operator} {value}"'
        )
    transformation_step_str = ", ".join(transformation_steps)

    new_meta["lineage"] = {
        "parent_message_id":  old_msg_id,
        "parent_batch_ids":   parent_batch_ids,
        "transformation_step": transformation_step_str,
    }

    # ---- Step 6: Update metadata.batch ------------------------------------
    filtered_json_bytes = json.dumps(
        filtered_records, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    batch_hash = hashlib.sha256(filtered_json_bytes).hexdigest()

    new_meta["batch"] = {
        "batch_id":     batch_hash,
        "record_count": int(len(filtered_records)),
    }

    # ---- Step 8: Serialize and return -------------------------------------
    filtered_cdm_string = json.dumps(new_cdm, ensure_ascii=False)

    return {"filtered_cdm": filtered_cdm_string}


# ---------------------------------------------------------------------------
# Quick self-test / demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    sample_cdm = {
        "metadata": {
            "message": {
                "msg_id": "aaa-bbb-ccc",
                "correlation_id": "job-001",
                "timestamp_utc": "2025-06-01T12:00:00.000000Z",
            },
            "event": {
                "version": "v1.0",
                "source_system": "Odoo",
                "object_type": "employees",
                "source_event": "employee.sync",
            },
            "processing": {
                "workflow_id": "recipe-100",
                "workato_url": "https://workato.com/recipes/100",
            },
            "batch": {
                "batch_id": "prev-batch-hash",
                "batch_size": 3,
            },
            "lineage": {
                "parent_message_id": "",
                "parent_batch_ids": "",
                "transformation_step": "",
            },
        },
        "employees": [
            {
                "identity": {
                    "canonical_id": "EMP-001",
                    "external_keys": {"odoo_id": "10", "bigquery_id": "bq-10"},
                },
                "core": {
                    "primary_name": "Alice",
                    "email": "alice@example.com",
                    "work_phone": "+1234",
                    "gender": "female",
                    "status": "active",
                    "birth_date": "1990-03-15",
                },
                "employment": {
                    "manager": "Bob",
                    "job_title": "Engineer",
                    "department": "Engineering",
                    "employment_type": "full-time",
                    "employment_start_date": "2020-01-10T00:00:00Z",
                    "employment_end_date": None,
                },
                "user": {
                    "login": "alice",
                    "status": "enabled",
                    "sales_name": "",
                    "google_workspace_user_id": "gw-alice",
                },
            },
            {
                "identity": {
                    "canonical_id": "EMP-002",
                    "external_keys": {"odoo_id": "20", "bigquery_id": "bq-20"},
                },
                "core": {
                    "primary_name": "Bob",
                    "email": "bob@example.com",
                    "work_phone": "+5678",
                    "gender": "male",
                    "status": "inactive",
                    "birth_date": "1985-07-22",
                },
                "employment": {
                    "manager": "Carol",
                    "job_title": "Manager",
                    "department": "Engineering",
                    "employment_type": "full-time",
                    "employment_start_date": "2018-06-01T00:00:00Z",
                    "employment_end_date": "2024-12-31T00:00:00Z",
                },
                "user": {
                    "login": "bob",
                    "status": "disabled",
                    "sales_name": "",
                    "google_workspace_user_id": "gw-bob",
                },
            },
            {
                "identity": {
                    "canonical_id": "EMP-003",
                    "external_keys": {"odoo_id": "30", "bigquery_id": "bq-30"},
                },
                "core": {
                    "primary_name": "Carol",
                    "email": "carol@example.com",
                    "work_phone": None,
                    "gender": "female",
                    "status": "active",
                    "birth_date": "1992-11-05",
                },
                "employment": {
                    "manager": "Dave",
                    "job_title": "Director",
                    "department": "Sales",
                    "employment_type": "full-time",
                    "employment_start_date": "2019-03-20T00:00:00Z",
                    "employment_end_date": None,
                },
                "user": {
                    "login": "carol",
                    "status": "enabled",
                    "sales_name": "Carol S.",
                    "google_workspace_user_id": "gw-carol",
                },
            },
        ],
        "audit": {
            "last_modified_by": "Odoo Connector",
            "change_log": "Initial sync",
        },
    }

    path = 'recipes/cdm_transformers/sample_cdm.json'
    with open(path, "r") as file:
        cdm = json.load(file)
    cdm = cdm if cdm else sample_cdm
    raw = json.dumps(cdm)

    # ----- Test 1: equality on nested dot-path ----------------------------
    in_payload = {
        "raw_cdm": raw,
		"source_system": "BigQuery",
		"source_event": "Scheduler",
		"Filters": [
			{
				"field": "departments.identity.canonical_id",
				"operator": "not in",
				"value": "4898652453064971392"
			}
		],
		"object_type": "Department"
	}
    result = main(in_payload)
    parsed = json.loads(result["filtered_cdm"])
    print("=== Test 1: core.status = active AND employment.department = Engineering ===")
    print(f"Filtered count : {len(parsed['employees'])}")
    print(f"Names          : {[e['core']['primary_name'] for e in parsed['employees']]}")
    print(f"Lineage        : {json.dumps(parsed['metadata']['lineage'], indent=2)}")
    print(f"Batch          : {json.dumps(parsed['metadata']['batch'], indent=2)}")
    print(f"Audit          : {json.dumps(parsed['audit'], indent=2)}")
    print()

    # ----- Test 2: is null -------------------------------------------------
    result2 = main(
        recipe_id="recipe-200",
        job_id="job-556",
        source_system="Odoo",
        source_event="employee.filtered",
        object_type="employees",
        Filters=[
            {"field": "core.work_phone", "operator": "is null", "value": ""},
        ],
        raw_cdm=raw,
        workato_url="https://workato.com/recipes/200",
    )
    parsed2 = json.loads(result2["filtered_cdm"])
    print("=== Test 2: core.work_phone is null ===")
    print(f"Filtered count : {len(parsed2['employees'])}")
    print(f"Names          : {[e['core']['primary_name'] for e in parsed2['employees']]}")
    print()

    # ----- Test 3: is in ---------------------------------------------------
    result3 = main(
        recipe_id="recipe-200",
        job_id="job-557",
        source_system="Odoo",
        source_event="employee.filtered",
        object_type="employees",
        Filters=[
            {"field": "employment.job_title", "operator": "is in", "value": "Engineer,Director"},
        ],
        raw_cdm=raw,
        workato_url="https://workato.com/recipes/200",
    )
    parsed3 = json.loads(result3["filtered_cdm"])
    print("=== Test 3: employment.job_title is in [Engineer, Director] ===")
    print(f"Filtered count : {len(parsed3['employees'])}")
    print(f"Names          : {[e['core']['primary_name'] for e in parsed3['employees']]}")
    print()

    # ----- Test 4: != operator ---------------------------------------------
    result4 = main(
        recipe_id="recipe-200",
        job_id="job-558",
        source_system="Odoo",
        source_event="employee.filtered",
        object_type="employees",
        Filters=[
            {"field": "core.gender", "operator": "!=", "value": "male"},
        ],
        raw_cdm=raw,
        workato_url="https://workato.com/recipes/200",
    )
    parsed4 = json.loads(result4["filtered_cdm"])
    print("=== Test 4: core.gender != male ===")
    print(f"Filtered count : {len(parsed4['employees'])}")
    print(f"Names          : {[e['core']['primary_name'] for e in parsed4['employees']]}")