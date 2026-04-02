"""
CDM Diff Transformer
====================
Computes the set difference A - B between two CDM payloads, matched by
identity.canonical_id under the specified object_type.

Resolution strategy for matching records (same canonical_id in both):
  • If A's version has strictly more information → kept in the result
    (A carries data that B does not).
  • If B's version has equal or more information → excluded from the
    result (B already covers everything A has).

Records only in A are always included.  The original metadata and audit
objects from CDM A are inherited untouched; only metadata.batch and
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


# ---------------------------------------------------------------------------
# Helper – recursive information score
# ---------------------------------------------------------------------------
def _info_score(obj) -> int:
    """Count non-null, non-empty leaf values recursively.

    This gives a single integer that represents how much actual data a
    record (or any nested structure) carries.  Used to decide which of
    two records with the same canonical_id is "richer".

    Scoring rules
    -------------
    * ``None``          → 0
    * ``""`` (empty str) → 0
    * ``[]`` (empty list) → 0
    * ``{}`` (empty dict) → 0
    * non-empty str     → len(str)   (longer strings = more info)
    * number / bool     → 1
    * dict              → sum of children scores
    * list              → sum of element scores
    """
    if obj is None:
        return 0
    if isinstance(obj, dict):
        return sum(_info_score(v) for v in obj.values())
    if isinstance(obj, list):
        return sum(_info_score(v) for v in obj) if obj else 0
    if isinstance(obj, str):
        return len(obj) if obj else 0
    return 1  # int, float, bool


def main(input: dict) -> dict:
    """Compute A - B on the *object_type* array and return a new CDM payload
    wrapped in ``{"diff_cdm": "<json string>"}``.

    For records that share a canonical_id, the version with strictly more
    information is kept; otherwise the record is dropped.
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

    # ---- Step 3: Index B by canonical_id (O(|B|)) ----------------------------
    b_index: dict[str, dict] = {}
    for rec in records_b:
        cid = str(rec.get("identity", {}).get("canonical_id", ""))
        b_index[cid] = rec  # last-write wins if B has dupes

    # ---- Step 4: Build diff list (O(|A|)) ------------------------------------
    # • A-only records    → always included
    # • Matched records   → included only if A's version is strictly richer
    diff_records: list[dict] = []
    for rec_a in records_a:
        cid = str(rec_a.get("identity", {}).get("canonical_id", ""))
        rec_b = b_index.get(cid)
        if rec_b is None:
            # A-only → keep
            diff_records.append(rec_a)
        elif _info_score(rec_a) > _info_score(rec_b):
            # A is richer → keep A's version
            diff_records.append(rec_a)
        # else: B has equal or more info → drop

    # ---- Build the new CDM shell ---------------------------------------------
    # Inherit the entire metadata and audit from CDM A untouched;
    # only metadata.batch and metadata.lineage are updated below.
    new_cdm: dict = {
        "metadata": copy.deepcopy(cdm_a.get("metadata", {})),
        object_type: diff_records,
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
            f'diff => "{object_type}: A({old_batch_id}) - B({b_batch_id})"'
        ),
    }

    # ---- Step 6: Update metadata.batch ---------------------------------------
    diff_json_bytes = json.dumps(
        diff_records, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    batch_hash = hashlib.sha256(diff_json_bytes).hexdigest()

    new_meta["batch"] = {
        "batch_id": batch_hash,
        "record_count": int(len(diff_records)),
    }

    # ---- Serialize and return ------------------------------------------------
    diff_cdm_string = json.dumps(new_cdm, ensure_ascii=False)
    return {"diff_cdm": diff_cdm_string}


# ---------------------------------------------------------------------------
# Quick self-test / demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # A: Engineering (full), Sales (sparse), HR (full)
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
             "core": {"department_name": "Engineering", "parent": "CTO", "dealer_key": "DK-1"}},
            {"identity": {"canonical_id": "1002",
                          "external_keys": {"bigquery_id": "1002"}},
             "core": {"department_name": "Sales", "parent": "", "dealer_key": ""}},
            {"identity": {"canonical_id": "1003",
                          "external_keys": {"bigquery_id": "1003"}},
             "core": {"department_name": "HR", "parent": "COO", "dealer_key": "DK-3"}},
        ],
        "audit": {"last_modified_by": "BigQuery Sync", "change_log": "Initial"},
    }

    # B: Sales (richer than A), Engineering (sparser than A)
    cdm_b = {
        "metadata": {
            "message": {"msg_id": "bbb-222", "correlation_id": "job-002",
                        "timestamp_utc": "2026-04-01T13:00:00.000000Z"},
            "event": {"version": "v1.0", "source_system": "BigQuery",
                      "object_type": "Department", "source_event": "Scheduler"},
            "processing": {"workflow_id": "recipe-100",
                           "workato_url": "https://workato.com/recipes/100"},
            "batch": {"batch_id": "batch-b-hash", "record_count": 2},
        },
        "departments": [
            {"identity": {"canonical_id": "1001",
                          "external_keys": {"bigquery_id": "1001"}},
             "core": {"department_name": "Engineering", "parent": "", "dealer_key": ""}},
            {"identity": {"canonical_id": "1002",
                          "external_keys": {"bigquery_id": "1002"}},
             "core": {"department_name": "Sales", "parent": "CRO", "dealer_key": "DK-2"}},
        ],
        "audit": {"last_modified_by": "BigQuery Sync", "change_log": "Initial"},
    }

    result = main({
        "raw_cdm_a": json.dumps(cdm_a),
        "raw_cdm_b": json.dumps(cdm_b),
        "object_type": "departments",
    })

    parsed = json.loads(result["diff_cdm"])
    names = [d["core"]["department_name"] for d in parsed["departments"]]
    print("=== Diff: A - B on departments ===")
    print(f"A had       : Engineering (rich), Sales (sparse), HR (A-only)")
    print(f"B had       : Engineering (sparse), Sales (rich)")
    print(f"Result count: {len(parsed['departments'])}")
    print(f"Result depts: {names}")
    print(f"  → Engineering kept (A richer than B)")
    print(f"  → Sales dropped   (B richer than A)")
    print(f"  → HR kept         (A-only)")
    print(f"Batch       : {json.dumps(parsed['metadata']['batch'], indent=2)}")
