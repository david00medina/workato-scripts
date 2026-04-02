"""
CDM Merge Transformer
=====================
Computes the set union A ∪ B between two CDM payloads.
Combines all records from CDM A and CDM B under the specified object_type,
deduplicating by **content equality** (every field except the ``identity``
block).  The identity block is excluded from comparison because IDs may
differ across source systems while the business data is the same.
When a duplicate is found, the version from A takes precedence (A wins).

Matching is done via SHA-256 hashes of the non-identity content, giving
O(n) set construction and O(1) per-record lookups.

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


# ---------------------------------------------------------------------------
# Helper – content fingerprint (excludes the "identity" block)
# ---------------------------------------------------------------------------
def _content_hash(record: dict) -> str:
    """Return a deterministic SHA-256 hex digest of *record* with the
    ``identity`` key stripped out.  Uses canonical JSON serialisation
    (sorted keys, compact separators) so that logically equal dicts
    always produce the same hash.
    """
    content = {k: v for k, v in record.items() if k != "identity"}
    raw = json.dumps(content, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def main(input: dict) -> dict:
    """Compute A ∪ B on the *object_type* array and return a new CDM payload
    wrapped in ``{"merged_cdm": "<json string>"}``.

    Deduplication strategy: two records are duplicates when all fields
    **except** ``identity`` are equal.  A's version wins on duplicates.
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

    # ---- Step 3: Build a set of content hashes from A (O(|A|)) ---------------
    seen_hashes: set[str] = set()
    merged_records: list[dict] = []

    # A records go in first — they take precedence
    for rec in records_a:
        h = _content_hash(rec)
        if h not in seen_hashes:
            seen_hashes.add(h)
            merged_records.append(rec)

    # ---- Step 4: Append B records whose content is not already in A (O(|B|)) -
    for rec in records_b:
        h = _content_hash(rec)
        if h not in seen_hashes:
            seen_hashes.add(h)
            merged_records.append(rec)

    # ---- Build the new CDM shell ---------------------------------------------
    new_cdm: dict = {
        "metadata": copy.deepcopy(cdm_a.get("metadata", {})),
        object_type: merged_records,
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
            f'merge => "{object_type}: A({old_batch_id}) ∪ B({b_batch_id})"'
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

    merged_json_bytes = json.dumps(
        merged_records, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    batch_hash = hashlib.sha256(merged_json_bytes).hexdigest()

    new_meta["batch"] = {
        "batch_id": batch_hash,
        "record_count": int(len(merged_records)),
    }

    # ---- Step 7: Fill audit section ------------------------------------------
    # Count how many records came exclusively from B (new additions)
    added_from_b = int(len(merged_records)) - int(len(records_a))

    new_cdm["audit"] = {
        "last_modified_by": "Workato Transformer",
        "change_log": (
            f"Merge Transformer: {object_type} A ∪ B "
            f"(A: {len(records_a)} records, B: {len(records_b)} records, "
            f"added from B: {added_from_b}, "
            f"result: {len(merged_records)} records)"
        ),
    }

    # ---- Step 8: Serialize and return ----------------------------------------
    merged_cdm_string = json.dumps(new_cdm, ensure_ascii=False)
    return {"merged_cdm": merged_cdm_string}


# ---------------------------------------------------------------------------
# Quick self-test / demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # CDM A: Engineering, Sales, HR
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

    # --- Test 1: B has same content with different IDs → deduplicated ---------
    # B: Sales (different ID, same content = dup), Marketing (new), Finance (new)
    cdm_b_1 = {
        "metadata": {
            "message": {"msg_id": "bbb-222", "correlation_id": "job-002",
                        "timestamp_utc": "2026-04-01T13:00:00.000000Z"},
            "event": {"version": "v1.0", "source_system": "BigQuery",
                      "object_type": "Department", "source_event": "Scheduler"},
            "processing": {"workflow_id": "recipe-100",
                           "workato_url": "https://workato.com/recipes/100"},
            "batch": {"batch_id": "batch-b1-hash", "record_count": 3},
        },
        "departments": [
            # Same core as A's "Sales" but with a completely different identity
            {"identity": {"canonical_id": "9999",
                          "external_keys": {"odoo_id": "OD-50"}},
             "core": {"department_name": "Sales", "parent": "", "dealer_key": ""}},
            {"identity": {"canonical_id": "1004",
                          "external_keys": {"bigquery_id": "1004"}},
             "core": {"department_name": "Marketing", "parent": "", "dealer_key": ""}},
            {"identity": {"canonical_id": "1005",
                          "external_keys": {"bigquery_id": "1005"}},
             "core": {"department_name": "Finance", "parent": "", "dealer_key": ""}},
        ],
    }

    result1 = main({
        "raw_cdm_a": json.dumps(cdm_a),
        "raw_cdm_b": json.dumps(cdm_b_1),
        "recipe_id": "recipe-400", "job_id": "job-950",
        "workato_url": "https://workato.com/recipes/400",
        "source_event": "Scheduler", "source_system": "BigQuery",
        "object_type": "departments",
    })
    parsed1 = json.loads(result1["merged_cdm"])
    print("=== Test 1: B has Sales with different ID but same content (dup) ===")
    print(f"Result count: {len(parsed1['departments'])}  (expected 5: Eng, Sales, HR, Marketing, Finance)")
    print(f"Result depts: {[d['core']['department_name'] for d in parsed1['departments']]}")
    print()

    # --- Test 2: B has same ID but different content → both kept --------------
    cdm_b_2 = copy.deepcopy(cdm_b_1)
    cdm_b_2["departments"][0]["identity"]["canonical_id"] = "1002"
    cdm_b_2["departments"][0]["core"]["department_name"] = "Sales (updated)"

    result2 = main({
        "raw_cdm_a": json.dumps(cdm_a),
        "raw_cdm_b": json.dumps(cdm_b_2),
        "recipe_id": "recipe-400", "job_id": "job-951",
        "workato_url": "https://workato.com/recipes/400",
        "source_event": "Scheduler", "source_system": "BigQuery",
        "object_type": "departments",
    })
    parsed2 = json.loads(result2["merged_cdm"])
    print("=== Test 2: B has Sales (updated) with same ID but different content ===")
    print(f"Result count: {len(parsed2['departments'])}  (expected 6: all unique content)")
    print(f"Result depts: {[d['core']['department_name'] for d in parsed2['departments']]}")
