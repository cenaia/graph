from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_REPO_ROOT = Path("/Users/ciaccy/Desktop/0all/obj/contest/Kiveiv-loukie7-0320")
DEFAULT_WORKSPACE_ROOT = Path("/Users/ciaccy/Desktop/0all/obj/contest/graph")


def main() -> None:
    asyncio.run(_async_main())


async def _async_main() -> None:
    args = _parse_args()
    repo_root = args.repo_root.resolve()
    workspace_root = args.workspace_root.resolve()

    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from sqlalchemy import exists, func, or_, select

    from backend.domains.extraction.infrastructure.graph.neo4j_export import build_neo4j_export_bundle
    from backend.infrastructure.db import DatabaseSessionManager
    from backend.infrastructure.models import TaskOutput, TaskRun
    from backend.infrastructure.oss_client import OSSClient
    from backend.shared.config import load_config
    from backend.shared.env_loader import load_runtime_env
    from backend.shared.enums import TaskStatus, TaskType

    os.environ["KIVEIV_ENV"] = args.runtime_env
    load_runtime_env(runtime_env=args.runtime_env, explicit_env_file=args.env_file, override=True)
    config = load_config(env=os.environ)
    _assert_safe_database(config.database.url, allow_nonstandard=args.allow_nonstandard_db)

    db = DatabaseSessionManager(config.database)
    oss = OSSClient(config.oss, env=config.runtime.env)

    output_context: dict[str, Any]
    outputs: list[dict[str, Any]]
    task_snapshot: dict[str, Any]

    async def _load() -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
        async with db.session_factory() as session:
            graph_output_exists = exists(
                select(TaskOutput.id).where(
                    TaskOutput.task_id == TaskRun.id,
                    or_(
                        TaskOutput.output_type == "graph_json",
                        func.lower(TaskOutput.output_uri).like("%graph.json"),
                    ),
                )
            )
            stmt = (
                select(TaskRun)
                .where(
                    TaskRun.task_type == TaskType.KNOWLEDGE_EXTRACT.value,
                    TaskRun.status == TaskStatus.COMPLETED.value,
                    graph_output_exists,
                )
                .order_by(TaskRun.completed_at.desc(), TaskRun.created_at.desc())
                .limit(200)
            )
            if args.task_id:
                stmt = stmt.where(TaskRun.id == args.task_id)
            tasks = (await session.execute(stmt)).scalars().all()

            selected_task = None
            for candidate in tasks:
                snapshot = dict(candidate.input_snapshot or {})
                payload = dict(snapshot.get("payload") or {}) if isinstance(snapshot.get("payload"), dict) else {}
                if args.doc_id is not None and int(payload.get("doc_id") or -1) != int(args.doc_id):
                    continue
                selected_task = candidate
                break
            if selected_task is None:
                raise SystemExit("No completed graph extraction task matched the current selector.")

            output_rows = (
                await session.execute(
                    select(TaskOutput)
                    .where(TaskOutput.task_id == selected_task.id)
                    .order_by(TaskOutput.created_at.asc())
                )
            ).scalars().all()

            normalized_outputs = [
                {
                    "id": row.id,
                    "task_id": row.task_id,
                    "output_type": str(row.output_type or "").strip().lower(),
                    "output_uri": str(row.output_uri or "").strip(),
                    "metadata": dict(row.output_metadata or {}),
                    "created_at": row.created_at.isoformat() if row.created_at is not None else None,
                }
                for row in output_rows
            ]
            return (
                {
                    "task_id": selected_task.id,
                    "status": selected_task.status,
                    "completed_at": selected_task.completed_at.isoformat() if selected_task.completed_at is not None else None,
                    "created_at": selected_task.created_at.isoformat() if selected_task.created_at is not None else None,
                },
                normalized_outputs,
                dict(selected_task.input_snapshot or {}),
            )

    try:
        output_context, outputs, task_snapshot = await _load()
        asset_outputs = _select_graph_assets(outputs)
        if "graph.json" not in asset_outputs:
            raise SystemExit("Selected task does not expose graph.json.")

        context = _build_context(output_context=output_context, outputs=outputs, task_snapshot=task_snapshot)
        bundle_dir = _make_bundle_dir(workspace_root=workspace_root, context=context)
        raw_dir = bundle_dir / "raw"
        neo4j_dir = bundle_dir / "neo4j"
        raw_dir.mkdir(parents=True, exist_ok=False)
        neo4j_dir.mkdir(parents=True, exist_ok=True)

        downloaded_assets: dict[str, dict[str, Any]] = {}
        missing_assets: list[str] = []
        for asset_name in ("graph.json", "graph.graphml", "entity_observations.json", "viewer.html"):
            selected_output = asset_outputs.get(asset_name)
            if selected_output is None:
                missing_assets.append(asset_name)
                continue
            payload = await oss.download_bytes(uri_or_key=selected_output["output_uri"])
            target_path = raw_dir / asset_name
            target_path.write_bytes(payload)
            downloaded_assets[asset_name] = {
                "path": str(target_path),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "size_bytes": len(payload),
                "output_id": selected_output["id"],
                "output_type": selected_output["output_type"],
                "output_uri": selected_output["output_uri"],
            }

        graph_payload = json.loads((raw_dir / "graph.json").read_text(encoding="utf-8"))
        observations_payload = None
        observation_path = raw_dir / "entity_observations.json"
        if observation_path.exists():
            observations_payload = json.loads(observation_path.read_text(encoding="utf-8"))

        if not args.skip_neo4j:
            bundle = build_neo4j_export_bundle(
                graph_payload=graph_payload,
                observations_payload=observations_payload,
                mode=args.mode,
            )
            _write_neo4j_files(neo4j_dir=neo4j_dir, bundle=bundle)
            type_mappings = bundle.type_mappings
            neo4j_stats = bundle.stats
        else:
            type_mappings = {}
            neo4j_stats = {}

        manifest = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": args.mode,
            "skip_neo4j": bool(args.skip_neo4j),
            "task": context,
            "raw_assets": downloaded_assets,
            "missing_assets": missing_assets,
            "graph_stats": {
                "schema_version": int(graph_payload.get("schema_version") or 0),
                "entity_count": int(graph_payload.get("stats", {}).get("entity_count") or len(graph_payload.get("entities", []))),
                "relation_count": int(graph_payload.get("stats", {}).get("relation_count") or len(graph_payload.get("relations", []))),
            },
            "neo4j_stats": neo4j_stats,
            "type_mappings": type_mappings,
        }
        (bundle_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        print(str(bundle_dir))
    finally:
        await db.dispose()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backup the latest graph extraction bundle and export Neo4j import assets.")
    parser.add_argument("--repo-root", type=Path, default=DEFAULT_REPO_ROOT)
    parser.add_argument("--workspace-root", type=Path, default=DEFAULT_WORKSPACE_ROOT)
    parser.add_argument("--runtime-env", default="dev")
    parser.add_argument("--env-file")
    parser.add_argument("--task-id")
    parser.add_argument("--doc-id", type=int)
    parser.add_argument("--mode", choices=("cypher", "admin", "both"), default="both")
    parser.add_argument("--skip-neo4j", action="store_true")
    parser.add_argument("--allow-nonstandard-db", action="store_true")
    return parser.parse_args()


def _assert_safe_database(database_url: str, *, allow_nonstandard: bool) -> None:
    parsed = urlparse(database_url)
    database_name = parsed.path.lstrip("/")
    if allow_nonstandard:
        return
    if database_name != "kiveiv_dev":
        raise SystemExit(f"Refusing to run against database '{database_name}'. Expected 'kiveiv_dev'.")


def _select_graph_assets(outputs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for item in outputs:
        output_uri = str(item.get("output_uri") or "")
        filename = Path(output_uri).name.lower()
        output_type = str(item.get("output_type") or "").lower()
        if filename in {"graph.json", "graph.graphml", "entity_observations.json", "viewer.html"}:
            selected.setdefault(filename, item)
            continue
        if output_type == "graph_json":
            selected.setdefault("graph.json", item)
        elif output_type == "graphml":
            selected.setdefault("graph.graphml", item)
        elif output_type == "entity_observations_json":
            selected.setdefault("entity_observations.json", item)
        elif output_type == "viewer_html":
            selected.setdefault("viewer.html", item)
    return selected


def _build_context(
    *,
    output_context: dict[str, Any],
    outputs: list[dict[str, Any]],
    task_snapshot: dict[str, Any],
) -> dict[str, Any]:
    payload = dict(task_snapshot.get("payload") or {}) if isinstance(task_snapshot.get("payload"), dict) else {}
    metadata = {}
    for item in outputs:
        candidate = item.get("metadata")
        if isinstance(candidate, dict) and candidate:
            metadata = candidate
            break
    return {
        "task_id": output_context["task_id"],
        "kb_id": _int_or_none(metadata.get("kb_id") or payload.get("kb_id")),
        "doc_id": _int_or_none(metadata.get("doc_id") or payload.get("doc_id")),
        "version_id": _int_or_none(metadata.get("version_id") or payload.get("version_id")),
        "version_no": _int_or_none(metadata.get("version_no") or payload.get("version_no")),
        "lineage_id": str(metadata.get("lineage_id") or payload.get("lineage_id") or "").strip() or None,
        "root_task_id": str(metadata.get("root_task_id") or payload.get("root_task_id") or "").strip() or None,
        "task_status": output_context["status"],
        "task_created_at": output_context["created_at"],
        "task_completed_at": output_context["completed_at"],
    }


def _make_bundle_dir(*, workspace_root: Path, context: dict[str, Any]) -> Path:
    workspace_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bundle_name = (
        f"{timestamp}__task_{context['task_id']}__doc_{context.get('doc_id') or 'unknown'}__v_{context.get('version_no') or 'unknown'}"
    )
    return workspace_root / bundle_name


def _write_neo4j_files(*, neo4j_dir: Path, bundle: Any) -> None:
    (neo4j_dir / "type_mappings.json").write_text(
        json.dumps(bundle.type_mappings, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    if bundle.cypher_files:
        cypher_dir = neo4j_dir / "cypher"
        cypher_dir.mkdir(parents=True, exist_ok=True)
        for filename, content in bundle.cypher_files.items():
            (cypher_dir / filename).write_text(content, encoding="utf-8")
    if bundle.admin_files:
        admin_dir = neo4j_dir / "admin"
        admin_dir.mkdir(parents=True, exist_ok=True)
        for filename, content in bundle.admin_files.items():
            (admin_dir / filename).write_text(content, encoding="utf-8")


def _int_or_none(value: Any) -> int | None:
    try:
        current = int(value)
    except (TypeError, ValueError):
        return None
    return current if current > 0 else None


if __name__ == "__main__":
    main()
