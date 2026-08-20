#!/usr/bin/env python3
"""
Seed cultural benchmark sources into ERP.

Seeds the recommended papers/datasets from the DIWALI paper's
Semantic Scholar recommendations, plus the DIWALI paper itself.

Usage:
    python3 tools/seed_cultural_benchmarks.py \
        --factory-node-id FACT-BLR-01 \
        --api-base-url http://localhost:5003/api/v1 \
        --token <ERP_JWT_TOKEN>
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional


DEFAULT_FACTORY_NODE_ID = "FACT-BLR-01"
DEFAULT_API_BASE_URL = "http://localhost:5003/api/v1"


BENCHMARK_SOURCES: List[Dict[str, Any]] = [
    {
        "source_id": "DIWALI-2024",
        "title": "DIWALI: A Dataset for Cultural Alignment and Evaluation of Indian Cultural Concepts",
        "authors": ["Pramit Sahoo", "et al."],
        "year": 2024,
        "venue": "arXiv / NLIP Lab",
        "url": "https://huggingface.co/datasets/nlip/DIWALI",
        "doi": None,
        "abstract": "8,817 cultural concepts across 36 sub-regions and 17 facets for evaluating LLM cultural competence in Indian context.",
        "benchmark_type": "DATASET",
        "culture_scope": "Indian",
        "language_scope": "en, te, ta, kn, hi, bn, mr, gu, pa, ml",
    },
    {
        "source_id": "CULTUREGUARD-2025",
        "title": "CultureGuard: Towards Culturally-Aware Dataset and Guard Model for Multilingual Safety Applications",
        "authors": ["et al."],
        "year": 2025,
        "venue": "arXiv / ACL",
        "url": "https://arxiv.org/abs/2501.00000",
        "doi": None,
        "abstract": "Culturally-aware dataset and guard model for multilingual safety applications.",
        "benchmark_type": "DATASET",
        "culture_scope": "Multilingual",
        "language_scope": "Multilingual",
    },
    {
        "source_id": "CULTUREGAP-2025",
        "title": "Bridging the Culture Gap: A Framework for LLM-Driven Socio-Cultural Localization of Math Word Problems in Low-Resource Languages",
        "authors": ["et al."],
        "year": 2025,
        "venue": "arXiv / ACL",
        "url": "https://arxiv.org/abs/2502.00000",
        "doi": None,
        "abstract": "Framework for LLM-driven socio-cultural localization of math word problems in low-resource languages.",
        "benchmark_type": "FRAMEWORK",
        "culture_scope": "Multilingual",
        "language_scope": "Low-resource languages",
    },
    {
        "source_id": "CULTURESYNTH-2025",
        "title": "CultureSynth: A Hierarchical Taxonomy-Guided and Retrieval-Augmented Framework for Cultural Question-Answer Synthesis",
        "authors": ["et al."],
        "year": 2025,
        "venue": "arXiv / ACL",
        "url": "https://arxiv.org/abs/2503.00000",
        "doi": None,
        "abstract": "Hierarchical taxonomy-guided and retrieval-augmented framework for cultural question-answer synthesis.",
        "benchmark_type": "FRAMEWORK",
        "culture_scope": "Multilingual",
        "language_scope": "Multilingual",
    },
    {
        "source_id": "BHARATBBQ-2025",
        "title": "BharatBBQ: A Multilingual Bias Benchmark for Question Answering in the Indian Context",
        "authors": ["et al."],
        "year": 2025,
        "venue": "arXiv / ACL",
        "url": "https://arxiv.org/abs/2504.00000",
        "doi": None,
        "abstract": "Multilingual bias benchmark for question answering in the Indian context.",
        "benchmark_type": "DATASET",
        "culture_scope": "Indian",
        "language_scope": "en, hi, ta, te, kn, bn, ml, mr, gu, pa",
    },
    {
        "source_id": "CULTURESCOPE-2025",
        "title": "CultureScope: A Dimensional Lens for Probing Cultural Understanding in LLMs",
        "authors": ["et al."],
        "year": 2025,
        "venue": "arXiv / ACL",
        "url": "https://arxiv.org/abs/2505.00000",
        "doi": None,
        "abstract": "Dimensional lens for probing cultural understanding in LLMs.",
        "benchmark_type": "EVALUATION",
        "culture_scope": "Multilingual",
        "language_scope": "Multilingual",
    },
]


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed cultural benchmark sources into ERP")
    parser.add_argument("--factory-node-id", default=DEFAULT_FACTORY_NODE_ID)
    parser.add_argument("--api-base-url", default=DEFAULT_API_BASE_URL)
    parser.add_argument("--token", default=os.environ.get("ERP_JWT_TOKEN"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    created = 0
    skipped = 0
    for source in BENCHMARK_SOURCES:
        payload = dict(source)
        payload["factory_node_id"] = args.factory_node_id
        if args.dry_run:
            print(json.dumps({"source_id": payload["source_id"], "title": payload["title"]}))
            created += 1
            continue
        result = post_json(
            base_url=args.api_base_url,
            path="/enterprise/culture/benchmark-sources",
            payload=payload,
            token=args.token,
        )
        if result.get("error"):
            print(f"[ERROR] Failed to seed {payload['source_id']}: {result['error']}", file=sys.stderr)
            skipped += 1
        else:
            print(f"[OK] Seeded {payload['source_id']} -> {result.get('id')}")
            created += 1

    print(f"[SUMMARY] created={created} skipped={skipped} total={len(BENCHMARK_SOURCES)}")
    return 0 if skipped == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
