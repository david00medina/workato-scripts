"""
CDM Merge Transformer
=====================
Computes the set union A ∪ B between two CDM payloads, matched by
identity.canonical_id under the specified object_type.

Resolution strategy for matching records (same canonical_id in both):
  • A deep merge is performed across every field of the two records.
  • At each leaf, the value with more information wins (non-null over
    null, longer string over shorter, populated dict over empty, etc.).
  • New keys that only exist in one side are always carried over.

Records only in A or only in B are included as-is.  The original metadata
and audit objects from CDM A are inherited untouched; only metadata.batch
and metadata.lineage are updated.

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


# ---------------------------------------------------------------------------
# Helper – recursive deep merge (keeps the most info at every level)
# ---------------------------------------------------------------------------
def _deep_merge(a, b):
    """Merge two values, preferring the richer one at every nesting level.

    * Both dicts   → recurse key-by-key; union of all keys.
    * Both lists   → keep the longer list.
    * Otherwise    → keep whichever side has the higher _info_score.
    """
    # Fast path: identical references or one side is None
    if a is b:
        return a
    if a is None:
        return b
    if b is None:
        return a

    # Dict + Dict → recurse
    if isinstance(a, dict) and isinstance(b, dict):
        merged: dict = {}
        for key in a.keys() | b.keys():          # union of keys – O(|keys|)
            if key in a and key in b:
                merged[key] = _deep_merge(a[key], b[key])
            elif key in a:
                merged[key] = a[key]
            else:
                merged[key] = b[key]
        return merged

    # List + List → keep the longer one
    if isinstance(a, list) and isinstance(b, list):
        return a if len(a) >= len(b) else b

    # Leaf vs leaf → higher score wins; tie → prefer a (left bias)
    return a if _info_score(a) >= _info_score(b) else b


def main(input: dict) -> dict:
    """Compute A ∪ B on the *object_type* array and return a new CDM payload
    wrapped in ``{"merged_cdm": "<json string>"}``.

    For records that share a canonical_id, both sides are deep-merged
    field-by-field to retain the maximum amount of information.
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

    # ---- Step 3: Index A by canonical_id (O(|A|)) ----------------------------
    # Preserves insertion order so result ordering matches A first, then B-only.
    a_index: dict[str, int] = {}       # cid → index in merged_records
    merged_records: list[dict] = []

    for rec in records_a:
        cid = str(rec.get("identity", {}).get("canonical_id", ""))
        if cid not in a_index:
            a_index[cid] = len(merged_records)
            merged_records.append(rec)

    # ---- Step 4: Merge / append B records (O(|B|)) ---------------------------
    for rec_b in records_b:
        cid = str(rec_b.get("identity", {}).get("canonical_id", ""))
        if cid in a_index:
            # Deep-merge into the existing A record in-place
            idx = a_index[cid]
            merged_records[idx] = _deep_merge(merged_records[idx], rec_b)
        else:
            a_index[cid] = len(merged_records)
            merged_records.append(rec_b)

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

    # ---- Serialize and return ------------------------------------------------
    merged_cdm_string = json.dumps(new_cdm, ensure_ascii=False)
    return {"merged_cdm": merged_cdm_string}


# ---------------------------------------------------------------------------
# Quick self-test / demo
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # A: Engineering (has parent, no dealer_key), Sales (sparse), HR (A-only)
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
             "core": {"department_name": "Engineering", "parent": "CTO", "dealer_key": ""}},
            {"identity": {"canonical_id": "1002",
                          "external_keys": {"bigquery_id": "1002"}},
             "core": {"department_name": "Sales", "parent": "", "dealer_key": ""}},
            {"identity": {"canonical_id": "1003",
                          "external_keys": {"bigquery_id": "1003"}},
             "core": {"department_name": "HR", "parent": "COO", "dealer_key": "DK-3"}},
        ],
        "audit": {"last_modified_by": "BigQuery Sync", "change_log": "Initial"},
    }

    # B: Engineering (has dealer_key, no parent), Sales (rich), Finance (B-only)
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
            {"identity": {"canonical_id": "1001",
                          "external_keys": {"bigquery_id": "1001", "odoo_id": "OD-1"}},
             "core": {"department_name": "Engineering", "parent": "", "dealer_key": "DK-1"}},
            {"identity": {"canonical_id": "1002",
                          "external_keys": {"bigquery_id": "1002"}},
             "core": {"department_name": "Sales", "parent": "CRO", "dealer_key": "DK-2"}},
            {"identity": {"canonical_id": "1005",
                          "external_keys": {"bigquery_id": "1005"}},
             "core": {"department_name": "Finance", "parent": "CFO", "dealer_key": "DK-5"}},
        ],
        "audit": {"last_modified_by": "BigQuery Sync", "change_log": "Initial"},
    }

    result = main({
        "raw_cdm_a": json.dumps(cdm_a),
        "raw_cdm_b": json.dumps(cdm_b),
        "object_type": "departments",
    })

    parsed = json.loads(result["merged_cdm"])
    print("=== Merge: A ∪ B on departments (deep-merge) ===")
    print(f"Result count: {len(parsed['departments'])}")
    for dept in parsed["departments"]:
        cid = dept["identity"]["canonical_id"]
        name = dept["core"]["department_name"]
        parent = dept["core"]["parent"]
        dk = dept["core"]["dealer_key"]
        ext = dept["identity"].get("external_keys", {})
        print(f"  {cid} {name:15s} parent={parent!r:6s} dealer_key={dk!r:6s} ext_keys={ext}")
    print()
    print("Expected:")
    print("  1001 Engineering   parent='CTO'  dealer_key='DK-1' ext_keys={bigquery+odoo}")
    print("  1002 Sales         parent='CRO'  dealer_key='DK-2'")
    print("  1003 HR            parent='COO'  dealer_key='DK-3' (A-only)")
    print("  1005 Finance       parent='CFO'  dealer_key='DK-5' (B-only)")
