#!/usr/bin/env python3
"""
Lightweight indexer for shrimantasatpati/Saree-NIFT-Style dataset.

Does NOT require the `datasets` library.
Uses Hugging Face Datasets Server HTTP API + ERP REST API.

Usage:
    python3 tools/index_nift_style_references.py \
        --factory-node-id FACT-BLR-01 \
        --api-base-url http://localhost:5003/api/v1 \
        --token <ERP_JWT_TOKEN>
"""

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional


DEFAULT_DATASET = "shrimantasatpati/Saree-NIFT-Style"
DEFAULT_SPLIT = "train"
DEFAULT_CONFIG = "default"
HF_DATASETS_SERVER_BASE = "https://datasets-server.huggingface.co"


def api_get(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "erp-style-indexer/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        print(f"[ERROR] HTTP {exc.code} for {url}: {exc.reason}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"[ERROR] Request failed for {url}: {exc}", file=sys.stderr)
        sys.exit(1)


def fetch_rows(dataset: str, config: str, split: str, limit: int = 200) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    page_size = min(limit, 100)
    while offset < limit:
        url = (
            f"{HF_DATASETS_SERVER_BASE}/rows"
            f"?dataset={urllib.parse.quote(dataset, safe='')}"
            f"&config={urllib.parse.quote(config, safe='')}"
            f"&split={urllib.parse.quote(split, safe='')}"
            f"&offset={offset}&limit={page_size}"
        )
        payload = api_get(url)
        page_rows = payload.get("rows", [])
        if not page_rows:
            break
        rows.extend(page_rows)
        offset += len(page_rows)
        if offset >= payload.get("num_rows_total", 0):
            break
    return rows[:limit]


def post_json(
    base_url: str,
    path: str,
    payload: Dict[str, Any],
    token: Optional[str] = None,
) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(f"{base_url}{path}", data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"[ERROR] HTTP {exc.code} for {path}: {body}", file=sys.stderr)
        return {"error": {"status": exc.code, "message": body}}
    except Exception as exc:
        print(f"[ERROR] Request failed for {path}: {exc}", file=sys.stderr)
        return {"error": {"status": 500, "message": str(exc)}}


def extract_image_info(row: Dict[str, Any], row_index: int) -> Optional[Dict[str, Any]]:
    """Return normalized image metadata from a HF datasets-server row."""
    row_data = row.get("row", {})
    image_obj = row_data.get("image")
    if not image_obj:
        return None

    src = image_obj.get("src") or image_obj.get("url")
    if not src:
        return None

    width = image_obj.get("width")
    height = image_obj.get("height")
    if width is None or height is None:
        # Some server responses embed bytes; skip those here.
        return None

    image_id = f"SR-NIFT-{row_index:03d}"
    return {
        "image_id": image_id,
        "factory_node_id": None,
        "source_dataset": DEFAULT_DATASET,
        "hf_split": DEFAULT_SPLIT,
        "hf_row_index": row_index,
        "image_url": src,
        "image_width": width,
        "image_height": height,
        "storage_path": None,
        "mime_type": "image/png",
        "file_size_bytes": None,
        "dominant_colors": [],
        "pattern_hash": None,
        "ai_style_tag": None,
        "ai_confidence_score": None,
        "is_active": True,
        "metadata": {
            "source": "huggingface",
            "dataset": DEFAULT_DATASET,
            "row_index": row_index,
            "indexed_at": datetime.utcnow().isoformat() + "Z",
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Index NIFT Saree Style dataset into ERP")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="HuggingFace dataset id")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help="Dataset config / subset")
    parser.add_argument("--split", default=DEFAULT_SPLIT, help="Dataset split")
    parser.add_argument("--limit", type=int, default=200, help="Max rows to index")
    parser.add_argument("--factory-node-id", default="FACT-BLR-01", help="Factory node id")
    parser.add_argument("--api-base-url", default="http://localhost:5003/api/v1", help="ERP API base URL")
    parser.add_argument("--token", default=os.environ.get("ERP_JWT_TOKEN"), help="JWT token for ERP API")
    parser.add_argument("--dry-run", action="store_true", help="Print JSON without POSTing to API")
    args = parser.parse_args()

    print(f"[INFO] Fetching dataset rows: {args.dataset} [{args.config}] {args.split}")
    rows = fetch_rows(args.dataset, args.config, args.split, limit=args.limit)
    print(f"[INFO] Fetched {len(rows)} rows")

    indexed = 0
    skipped = 0
    for idx, row in enumerate(rows):
        info = extract_image_info(row, row_index=idx)
        if not info:
            skipped += 1
            continue

        if args.dry_run:
            print(json.dumps({"image_id": info["image_id"], "image_url": info["image_url"]}))
            indexed += 1
            continue

        payload = dict(info)
        payload["factory_node_id"] = args.factory_node_id
        result = post_json(
            base_url=args.api_base_url,
            path="/enterprise/style-references",
            payload=payload,
            token=args.token,
        )
        if result.get("error"):
            print(f"[ERROR] Failed to index {info['image_id']}: {result['error']}", file=sys.stderr)
            skipped += 1
        else:
            print(f"[OK] Indexed {info['image_id']} -> {result.get('id')}")
            indexed += 1

    print(f"[SUMMARY] indexed={indexed} skipped={skipped} total={len(rows)}")
    return 0 if skipped == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
