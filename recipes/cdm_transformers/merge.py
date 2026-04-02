"""
CDM Merge Transformer
=====================
Computes the set union A ∪ B between two CDM payloads.
Combines all records from CDM A and CDM B under the specified object_type,
deduplicating by identity.canonical_id. When a record exists in both A and B,
the version from A takes precedence (A wins).  The original metadata and
audit objects from CDM A are inherited untouched; only metadata.batch and
metadata.lineage are updated.

Input parameters
----------------
raw_cdm_a       : str   – JSON string of CDM payload A
raw_cdm_b       : str   – JSON string of CDM payload B
object_type     : str   – key in the CDM that holds the data array (e.g. "departments")
"""

import json
import hashlib
import copy


def main(input: dict) -> dict:
    """Compute A ∪ B on the *object_type* array and return a new CDM payload
    wrapped in ``{"merged_cdm": "<json string>"}``.

    Deduplication strategy: records from A take precedence over B when both
    share the same identity.canonical_id.
    """
    cdm_a_raw = input.get("raw_cdm_a", "")
    cdm_b_raw = input.get("raw_cdm_b", "")
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

    # ---- Step 3: Build an ordered dict of records from A (keyed by ID) -------
    seen_ids: dict[str, dict] = {}
    merged_records: list[dict] = []

    # A records go in first — they take precedence
    for rec in records_a:
        canonical_id = str(rec.get("identity", {}).get("canonical_id", ""))
        if canonical_id not in seen_ids:
            seen_ids[canonical_id] = rec
            merged_records.append(rec)

    # ---- Step 4: Append B records that are not already in A ------------------
    for rec in records_b:
        canonical_id = str(rec.get("identity", {}).get("canonical_id", ""))
        if canonical_id not in seen_ids:
            seen_ids[canonical_id] = rec
            merged_records.append(rec)

    # ---- Build the new CDM shell ---------------------------------------------
    # Inherit the entire metadata and audit from CDM A untouched;
    # only metadata.batch and metadata.lineage are updated below.
    new_cdm: dict = {
        "metadata": copy.deepcopy(cdm_a.get("metadata", {})),
        object_type: merged_records,
        "audit": copy.deepcopy(cdm_a.get("audit", {})),
    }

    old_meta = cdm_a.get("metadata", {})
    new_meta = new_cdm["metadata"]

    # ---- Step 5: Update metadata.lineage -------------------------------------
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
            f'merge => "{object_type}: A({old_batch_id}) ∪ B({b_batch_id})"'
        ),
    }

    # ---- Step 6: Update metadata.batch ---------------------------------------
    merged_json_bytes = json.dumps(
        merged_records, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    batch_hash = hashlib.sha256(merged_json_bytes).hexdigest()

    new_meta["batch"] = {
        "batch_id": batch_hash,
        "record_count": int(len(merged_records)),
    }

    # ---- Step 8: Serialize and return ----------------------------------------
    merged_cdm_string = json.dumps(new_cdm, ensure_ascii=False)
    return {"merged_cdm": merged_cdm_string}


# ---------------------------------------------------------------------------
# Quick self-test / demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # CDM A has: Engineering (1001), Sales (1002), HR (1003)
    cdm_a = {
        "metadata": {
            "message": {"msg_id": "aaa-111", "correlation_id": "job-001",
                        "timestamp_utc": "2026-04-01T12:00:00.000000Z"},
            "event": {"version": "v1.0", "source_system": "BigQuery",
                      "object_type": "Department", "source_event": "Scheduler"},
            "processing": {"workflow_id": "recipe-100",
                           "workato_url": "https://workato.com/recipes/100"},
            "batch": {"batch_id": "batch-a-hash", "record_count": 3},
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

    # CDM B has: Sales (1002 - duplicate), Marketing (1004 - new), Finance (1005 - new)
    cdm_b = {
        "metadata": {
            "message": {"msg_id": "bbb-222", "correlation_id": "job-002",
                        "timestamp_utc": "2026-04-01T13:00:00.000000Z"},
            "event": {"version": "v1.0", "source_system": "BigQuery",
                      "object_type": "Department", "source_event": "Scheduler"},
            "processing": {"workflow_id": "recipe-100",
                           "workato_url": "https://workato.com/recipes/100"},
            "batch": {"batch_id": "batch-b-hash", "record_count": 3},
        },
        "departments": [
            {"identity": {"canonical_id": "1002",
                          "external_keys": {"bigquery_id": "1002"}},
             "core": {"department_name": "Sales (updated)", "parent": "", "dealer_key": ""}},
            {"identity": {"canonical_id": "1004",
                          "external_keys": {"bigquery_id": "1004"}},
             "core": {"department_name": "Marketing", "parent": "", "dealer_key": ""}},
            {"identity": {"canonical_id": "1005",
                          "external_keys": {"bigquery_id": "1005"}},
             "core": {"department_name": "Finance", "parent": "", "dealer_key": ""}},
        ],
    }

    result = main({
        "raw_cdm_a": json.dumps(cdm_a),
        "raw_cdm_b": json.dumps(cdm_b),
        "recipe_id": "recipe-400",
        "job_id": "job-950",
        "workato_url": "https://workato.com/recipes/400",
        "source_event": "Scheduler",
        "source_system": "BigQuery",
        "object_type": "departments",
    })

    parsed = json.loads(result["merged_cdm"])
    print("=== Merge: A ∪ B on departments ===")
    print(f"A had       : 3 records (Engineering, Sales, HR)")
    print(f"B had       : 3 records (Sales [dup], Marketing, Finance)")
    print(f"Result count: {len(parsed['departments'])}")
    print(f"Result depts: {[d['core']['department_name'] for d in parsed['departments']]}")
    print(f"Version     : {parsed['metadata']['event']['version']}")
    print(f"Lineage     : {json.dumps(parsed['metadata']['lineage'], indent=2)}")
    print(f"Batch       : {json.dumps(parsed['metadata']['batch'], indent=2)}")
    print(f"Audit       : {json.dumps(parsed['audit'], indent=2)}")
    print()

    # Verify: Sales should be A's version (not "Sales (updated)") since A wins
    sales_rec = [d for d in parsed["departments"]
                 if d["identity"]["canonical_id"] == "1002"][0]
    print(f"Sales name  : '{sales_rec['core']['department_name']}' (should be 'Sales', A wins)")
