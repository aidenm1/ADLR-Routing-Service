"""
travel_time_cache_utils.py
--------------------------
Utilities for loading and saving cached travel time matrices.

Imported by route-generation.py. Three public functions:
    get_cached_matrix_if_valid  — load cache if nodes match
    save_cached_matrix          — write matrix + metadata to disk
    is_cache_recent             — age check (hours)
"""

import hashlib
import json
import os
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_nodes_hash(nodes):
    entries = []
    for node in nodes:
        entries.append((
            node.get("type", ""),
            str(node.get("id", "")),
            float(node["lat"]),
            float(node["lng"]),
        ))
    entries.sort(key=lambda e: (0 if e[0] == "hub" else 1, e[0], e[1]))
    return hashlib.sha256(
        json.dumps(entries, separators=(",", ":")).encode()
    ).hexdigest()


def _load_raw(cache_path):
    """Return (matrix, metadata) or (None, None)."""
    if not os.path.exists(cache_path):
        return None, None
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "matrix" not in data or "metadata" not in data:
            return None, None
        return data["matrix"], data["metadata"]
    except (json.JSONDecodeError, IOError, KeyError):
        return None, None


def _is_valid_for_nodes(metadata, nodes):
    if metadata.get("num_nodes") != len(nodes):
        return False
    return metadata.get("nodes_hash") == _compute_nodes_hash(nodes)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_cached_matrix_if_valid(cache_path, nodes, verbose=False):
    """
    Return (matrix, metadata) if the cache exists and matches current nodes,
    otherwise return (None, None).
    """
    matrix, metadata = _load_raw(cache_path)
    if matrix is None:
        if verbose:
            print(f"Cache not found or unreadable: {cache_path}")
        return None, None

    if not _is_valid_for_nodes(metadata, nodes):
        if verbose:
            print(f"Cache invalid for current nodes: {cache_path}")
        return None, None

    if verbose:
        print(
            f"Loaded cached travel time matrix ({len(nodes)} nodes), "
            f"created {metadata.get('created_at')}"
        )
    return matrix, metadata


def save_cached_matrix(cache_path, matrix, nodes, hub=None):
    """Write matrix + metadata to cache_path."""
    cache_data = {
        "metadata": {
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
            "num_nodes": len(nodes),
            "nodes_hash": _compute_nodes_hash(nodes),
            "hub_lat": hub["lat"] if hub else None,
            "hub_lng": hub["lng"] if hub else None,
        },
        "matrix": matrix,
    }
    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache_data, f, indent=2)


def is_cache_recent(cache_path, max_age_hours=24):
    """Return True if the cache exists and was written within max_age_hours."""
    _, metadata = _load_raw(cache_path)
    if metadata is None:
        return False
    try:
        created = datetime.fromisoformat(metadata["created_at"])
    except (KeyError, ValueError):
        return False
    age = datetime.now(tz=timezone.utc) - created
    return age.total_seconds() <= max_age_hours * 3600