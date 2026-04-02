"""
CDM Diff Transformer
====================
Computes the set difference A - B between two CDM payloads.
Returns records present in CDM A but not in CDM B, matched by
identity.canonical_id under the specified object_type.

Input parameters
----------------
raw_cdm_a       : str   – JSON string of CDM payload A
raw_cdm_b       : str   – JSON string of CDM payload B
recipe_id       : str   – workflow / recipe identifier
job_id          : str   – correlation / job identifier
workato_url     : str   – (optional) Workato recipe URL for processing metadata
source_event    : str   – originating event name
source_system   : str   – originating system name
object_type     : str   – key in the CDM that holds the data array (e.g. "departments")
"""

import json
import uuid
import hashlib
import copy
from datetime import datetime, timezone


def main(input: dict) -> dict:
    """Compute A - B on the *object_type* array and return a new CDM payload
    wrapped in ``{"diff_cdm": "<json string>"}``.
    """
    cdm_a_raw = input.get("raw_cdm_a", "")
    cdm_b_raw = input.get("raw_cdm_b", "")
    recipe_id = input.get("recipe_id", "")
    job_id = input.get("job_id", "")
    workato_url = input.get("workato_url", "")
    source_event = input.get("source_event", "")
    source_system = input.get("source_system", "")
    object_type = input.get("object_type", "")

    # ---- Step 1: Parse both CDM JSON strings ---------------------------------
    cdm_a: dict = json.loads(cdm_a_raw)
    cdm_b: dict = json.loads(cdm_b_raw)

    # ---- Step 2: Validate object_type exists in both payloads ----------------
    if object_type not in cdm_a:
        raise KeyError(
            f"object_type '{object_type}' not found in CDM A. "
            f"Available keys: {list(cdm_a.keys())}"
        )
    if object_type not in cdm_b:
        raise KeyError(
            f"object_type '{object_type}' not found in CDM B. "
            f"Available keys: {list(cdm_b.keys())}"
        )

    records_a: list[dict] = cdm_a[object_type]
    records_b: list[dict] = cdm_b[object_type]

    # ---- Step 3: Build set of canonical IDs from B ---------------------------
    b_ids: set[str] = set()
    for rec in records_b:
        canonical_id = rec.get("identity", {}).get("canonical_id")
        if canonical_id is not None:
            b_ids.add(str(canonical_id))

    # ---- Step 4: Filter A - B (records in A whose ID is not in B) ------------
    diff_records = [
        rec for rec in records_a
        if str(rec.get("identity", {}).get("canonical_id", "")) not in b_ids
    ]

    # ---- Build the new CDM shell ---------------------------------------------
    new_cdm: dict = {
        "metadata": copy.deepcopy(cdm_a.get("metadata", {})),
        object_type: diff_records,
        "audit": {},
    }

    old_meta = cdm_a.get("metadata", {})
    new_meta = new_cdm["metadata"]
    original_object_type = old_meta.get("event", {}).get("object_type", "")

    # ---- Step 5: Populate metadata.lineage -----------------------------------
    old_msg_id = old_meta.get("message", {}).get("msg_id", "")
    old_batch_id = old_meta.get("batch", {}).get("batch_id", "")

    existing_parent_batches = (
        old_meta.get("lineage", {}).get("parent_batch_ids", "")
    )
    if existing_parent_batches:
        parent_batch_ids = f"{existing_parent_batches},{old_batch_id}"
    else:
        parent_batch_ids = old_batch_id

    b_batch_id = cdm_b.get("metadata", {}).get("batch", {}).get("batch_id", "")

    new_meta["lineage"] = {
        "parent_message_id": old_msg_id,
        "parent_batch_ids": parent_batch_ids,
        "transformation_step": (
            f'diff => "{object_type}: A({old_batch_id}) - B({b_batch_id})"'
        ),
    }

    # ---- Step 6: Fill metadata fields ----------------------------------------
    new_meta["message"] = {
        "msg_id": str(uuid.uuid4()),
        "correlation_id": job_id,
        "timestamp_utc": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ),
    }

    new_meta["event"] = {
        "source_system": source_system,
        "object_type": original_object_type,
        "source_event": source_event,
    }

    new_meta["processing"] = {
        "workflow_id": recipe_id,
        "workato_url": workato_url,
    }

    diff_json_bytes = json.dumps(
        diff_records, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    batch_hash = hashlib.sha256(diff_json_bytes).hexdigest()

    new_meta["batch"] = {
        "batch_id": batch_hash,
        "record_count": int(len(diff_records)),
    }

    # ---- Step 7: Fill audit section ------------------------------------------
    new_cdm["audit"] = {
        "last_modified_by": "Workato Transformer",
        "change_log": (
            f"Diff Transformer: {object_type} A - B "
            f"(A: {len(records_a)} records, B: {len(records_b)} records, "
            f"result: {len(diff_records)} records)"
        ),
    }

    # ---- Step 8: Serialize and return ----------------------------------------
    diff_cdm_string = json.dumps(new_cdm, ensure_ascii=False)
    return {"diff_cdm": diff_cdm_string}


# ---------------------------------------------------------------------------
# Quick self-test / demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    cdm_a = {
        "metadata": {
            "message": {"msg_id": "aaa-111", "correlation_id": "job-001",
                        "timestamp_utc": "2026-04-01T12:00:00.000000Z"},
            "event": {"version": "v1.0", "source_system": "BigQuery",
                      "object_type": "Department", "source_event": "Scheduler"},
            "processing": {"workflow_id": "recipe-100",
                           "workato_url": "https://workato.com/recipes/100"},
            "batch": {"batch_id": "batch-a-hash", "batch_size": 3},
        },
        "departments": [
            {"identity": {"canonical_id": "1001",
                          "external_keys": {"bigquery_id": "1001"}},
             "core": {"department_name": "Engineering", "parent": "", "dealer_key": ""}},
            {"identity": {"canonical_id": "1002",
                          "external_keys": {"bigquery_id": "1002"}},
             "core": {"department_name": "Sales", "parent": "", "dealer_key": ""}},
            {"identity": {"canonical_id": "1003",
                          "external_keys": {"bigquery_id": "1003"}},
             "core": {"department_name": "HR", "parent": "", "dealer_key": ""}},
        ],
    }

    cdm_b = {
        "metadata": {
            "message": {"msg_id": "bbb-222", "correlation_id": "job-002",
                        "timestamp_utc": "2026-04-01T13:00:00.000000Z"},
            "event": {"version": "v1.0", "source_system": "BigQuery",
                      "object_type": "Department", "source_event": "Scheduler"},
            "processing": {"workflow_id": "recipe-100",
                           "workato_url": "https://workato.com/recipes/100"},
            "batch": {"batch_id": "batch-b-hash", "batch_size": 1},
        },
        "departments": [
            {"identity": {"canonical_id": "1002",
                          "external_keys": {"bigquery_id": "1002"}},
             "core": {"department_name": "Sales", "parent": "", "dealer_key": ""}},
        ],
    }

    result = main({
        "cdm_a": json.dumps(cdm_a),
        "cdm_b": json.dumps(cdm_b),
        "recipe_id": "recipe-300",
        "job_id": "job-900",
        "workato_url": "https://workato.com/recipes/300",
        "source_event": "Scheduler",
        "source_system": "BigQuery",
        "object_type": "departments",
    })

    parsed = json.loads(result["diff_cdm"])
    print("=== Diff: A - B on departments ===")
    print(f"A had       : 3 records (Engineering, Sales, HR)")
    print(f"B had       : 1 record  (Sales)")
    print(f"Result count: {len(parsed['departments'])}")
    print(f"Result depts: {[d['core']['department_name'] for d in parsed['departments']]}")
    print(f"Lineage     : {json.dumps(parsed['metadata']['lineage'], indent=2)}")
    print(f"Batch       : {json.dumps(parsed['metadata']['batch'], indent=2)}")
    print(f"Audit       : {json.dumps(parsed['audit'], indent=2)}")
