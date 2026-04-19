from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import io
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


DEFAULT_REPO_ROOT = Path("/Users/ciaccy/Desktop/0all/obj/contest/Kiveiv-loukie7-0320")
DEFAULT_WORKSPACE_ROOT = Path("/Users/ciaccy/Desktop/0all/obj/contest/graph")
DEFAULT_RAW_BASE_URL = "https://raw.githubusercontent.com/cenaia/graph/main"
RELATION_TYPE_IDENT_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")

CANONICAL_RELATION_EN_MAP: dict[str, tuple[str, str]] = {
    "产出": ("PRODUCES", "Produces"),
    "供电": ("POWERS", "Powers"),
    "依据": ("BASED_ON", "Based On"),
    "前置于": ("PRECEDES", "Precedes"),
    "包含": ("CONTAINS", "Contains"),
    "原因是": ("CAUSED_BY", "Caused By"),
    "反映": ("REFLECTS", "Reflects"),
    "发现": ("DISCOVERS", "Discovers"),
    "发生": ("OCCURS", "Occurs"),
    "发生于": ("OCCURS_AT", "Occurs At"),
    "发生故障": ("EXPERIENCES_FAILURE", "Experiences Failure"),
    "发生部位": ("OCCURS_AT_COMPONENT", "Occurs At Component"),
    "处理": ("HANDLES", "Handles"),
    "处理对象": ("ACTS_ON", "Acts On"),
    "处置": ("REMEDIATES", "Remediates"),
    "处置于": ("REMEDIATES_AT", "Remediates At"),
    "审批": ("APPROVES", "Approves"),
    "审核": ("REVIEWS", "Reviews"),
    "对应": ("CORRESPONDS_TO", "Corresponds To"),
    "导致": ("CAUSES", "Causes"),
    "引用": ("REFERENCES", "References"),
    "归因于": ("ATTRIBUTED_TO", "Attributed To"),
    "归类为": ("CLASSIFIED_AS", "Classified As"),
    "形成": ("FORMS", "Forms"),
    "影响": ("AFFECTS", "Affects"),
    "执行": ("EXECUTES", "Executes"),
    "指示": ("INDICATES", "Indicates"),
    "教训为": ("YIELDS_LESSON", "Yields Lesson"),
    "未知": ("UNKNOWN_RELATION", "Unknown Relation"),
    "检测": ("INSPECTS", "Inspects"),
    "涉及": ("INVOLVES", "Involves"),
    "涉及设备": ("INVOLVES_EQUIPMENT", "Involves Equipment"),
    "用于判定": ("USED_FOR_EVALUATION", "Used For Evaluation"),
    "用于处理": ("USED_FOR_HANDLING", "Used For Handling"),
    "用于诊断": ("USED_FOR_DIAGNOSIS", "Used For Diagnosis"),
    "用于预防": ("USED_FOR_PREVENTION", "Used For Prevention"),
    "监控": ("MONITORS", "Monitors"),
    "监测": ("MEASURES", "Measures"),
    "监督": ("SUPERVISES", "Supervises"),
    "确认": ("CONFIRMS", "Confirms"),
    "符合": ("COMPLIES_WITH", "Complies With"),
    "约束": ("CONSTRAINS", "Constrains"),
    "组成": ("COMPOSES", "Composes"),
    "组成或隶属": ("PART_OF_OR_BELONGS_TO", "Part Of Or Belongs To"),
    "表现为": ("MANIFESTS_AS", "Manifests As"),
    "见证": ("WITNESSES", "Witnesses"),
    "触发": ("TRIGGERS", "Triggers"),
    "诊断指标": ("HAS_DIAGNOSTIC_INDICATOR", "Has Diagnostic Indicator"),
    "负责": ("RESPONSIBLE_FOR", "Responsible For"),
    "适用于": ("APPLIES_TO", "Applies To"),
    "遵循": ("FOLLOWS", "Follows"),
    "配备": ("EQUIPPED_WITH", "Equipped With"),
    "采取": ("ADOPTS", "Adopts"),
    "采用": ("USES", "Uses"),
    "采用检测方法": ("USES_TEST_METHOD", "Uses Test Method"),
    "采用维修工艺": ("USES_REPAIR_PROCESS", "Uses Repair Process"),
    "针对": ("TARGETS", "Targets"),
    "预防": ("PREVENTS", "Prevents"),
}


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

        import_targets: list[dict[str, Any]] = []
        relation_view_profiles: dict[str, Any] = {}
        if not args.skip_neo4j:
            bundle = build_neo4j_export_bundle(
                graph_payload=graph_payload,
                observations_payload=observations_payload,
                mode=args.mode,
            )
            enhancement = _enhance_neo4j_bundle(
                bundle=bundle,
                bundle_name=bundle_dir.name,
                raw_base_url=args.raw_base_url,
                emit_local_import=args.emit_local_import,
                relation_view_profile=args.relation_view_profile,
                emit_experimental_zh_import=args.emit_experimental_zh_import,
            )
            _write_neo4j_files(neo4j_dir=neo4j_dir, bundle=bundle)
            _write_neo4j_readme(
                neo4j_dir=neo4j_dir,
                import_targets=enhancement["import_targets"],
                relation_view_profile=args.relation_view_profile,
                emit_experimental_zh_import=args.emit_experimental_zh_import,
            )
            type_mappings = bundle.type_mappings
            neo4j_stats = bundle.stats
            import_targets = enhancement["import_targets"]
            relation_view_profiles = enhancement["relation_view_profiles"]
        else:
            type_mappings = {}
            neo4j_stats = {}

        manifest = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": args.mode,
            "skip_neo4j": bool(args.skip_neo4j),
            "raw_base_url": args.raw_base_url,
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
            "import_targets": import_targets,
            "relation_view_profiles": relation_view_profiles,
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
    parser.add_argument("--raw-base-url", default=DEFAULT_RAW_BASE_URL)
    parser.add_argument("--relation-view-profile", choices=("engineering", "teaching", "both"), default="both")
    parser.add_argument("--emit-local-import", dest="emit_local_import", action="store_true", default=True)
    parser.add_argument("--no-emit-local-import", dest="emit_local_import", action="store_false")
    parser.add_argument("--emit-experimental-zh-import", action="store_true")
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


def _enhance_neo4j_bundle(
    *,
    bundle: Any,
    bundle_name: str,
    raw_base_url: str,
    emit_local_import: bool,
    relation_view_profile: str,
    emit_experimental_zh_import: bool,
) -> dict[str, Any]:
    if not bundle.cypher_files:
        return {
            "import_targets": [],
            "relation_view_profiles": _build_relation_view_profiles(
                relation_view_profile=relation_view_profile,
                emit_experimental_zh_import=emit_experimental_zh_import,
            ),
        }

    relation_fieldnames, relation_rows = _read_csv_content(bundle.cypher_files["relationships.csv"])
    relation_mapping = _build_relation_display_mapping(
        relation_rows=relation_rows,
        relation_type_mappings=bundle.type_mappings.get("relation_types", {}),
    )
    next_type_mappings = _augment_type_mappings(
        type_mappings=dict(bundle.type_mappings),
        relation_mapping=relation_mapping,
    )
    bundle.type_mappings.clear()
    bundle.type_mappings.update(next_type_mappings)
    bundle.cypher_files["relationships.csv"] = _augment_relationship_csv_content(
        content=bundle.cypher_files["relationships.csv"],
        relation_mapping=relation_mapping,
        is_admin=False,
    )
    bundle.cypher_files["entity_has_observation.csv"] = _augment_observation_link_csv_content(
        content=bundle.cypher_files["entity_has_observation.csv"],
        is_admin=False,
    )
    if bundle.admin_files:
        if "relationships.csv" in bundle.admin_files:
            bundle.admin_files["relationships.csv"] = _augment_relationship_csv_content(
                content=bundle.admin_files["relationships.csv"],
                relation_mapping=relation_mapping,
                is_admin=True,
            )
        if "entity_has_observation.csv" in bundle.admin_files:
            bundle.admin_files["entity_has_observation.csv"] = _augment_observation_link_csv_content(
                content=bundle.admin_files["entity_has_observation.csv"],
                is_admin=True,
            )

    csv_url_base = _build_csv_url_base(raw_base_url=raw_base_url, bundle_name=bundle_name)
    import_targets: list[dict[str, Any]] = []
    bundle.cypher_files["import.cypher"] = _build_import_cypher(
        csv_base=csv_url_base,
        relation_mapping=relation_mapping,
        local=False,
        teaching=False,
    )
    import_targets.append(
        {
            "profile": "engineering",
            "variant": "cloud",
            "path": "neo4j/cypher/import.cypher",
            "csv_base": csv_url_base,
        }
    )

    if emit_local_import:
        bundle.cypher_files["import.local.cypher"] = _build_import_cypher(
            csv_base="file:///",
            relation_mapping=relation_mapping,
            local=True,
            teaching=False,
        )
        import_targets.append(
            {
                "profile": "engineering",
                "variant": "local",
                "path": "neo4j/cypher/import.local.cypher",
                "csv_base": "file:///",
            }
        )
    else:
        bundle.cypher_files.pop("import.local.cypher", None)

    if emit_experimental_zh_import:
        bundle.cypher_files["import.zh.cypher"] = _build_import_cypher(
            csv_base=csv_url_base,
            relation_mapping=relation_mapping,
            local=False,
            teaching=True,
        )
        import_targets.append(
            {
                "profile": "experimental_zh",
                "variant": "cloud",
                "path": "neo4j/cypher/import.zh.cypher",
                "csv_base": csv_url_base,
            }
        )
        if emit_local_import:
            bundle.cypher_files["import.zh.local.cypher"] = _build_import_cypher(
                csv_base="file:///",
                relation_mapping=relation_mapping,
                local=True,
                teaching=True,
            )
            import_targets.append(
                {
                    "profile": "experimental_zh",
                    "variant": "local",
                    "path": "neo4j/cypher/import.zh.local.cypher",
                    "csv_base": "file:///",
                }
            )
        else:
            bundle.cypher_files.pop("import.zh.local.cypher", None)
    else:
        bundle.cypher_files.pop("import.zh.cypher", None)
        bundle.cypher_files.pop("import.zh.local.cypher", None)

    return {
        "import_targets": import_targets,
        "relation_view_profiles": _build_relation_view_profiles(
            relation_view_profile=relation_view_profile,
            emit_experimental_zh_import=emit_experimental_zh_import,
        ),
    }


def _build_relation_display_mapping(
    *,
    relation_rows: list[dict[str, str]],
    relation_type_mappings: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    used_canonical_types: dict[str, str] = {}

    raw_keys = list(relation_type_mappings.keys())
    raw_keys.extend(_resolve_relation_raw_key(row) for row in relation_rows)

    for raw_key in raw_keys:
        key = str(raw_key or "").strip()
        if not key or key in mapping:
            continue
        relation_row = next((item for item in relation_rows if _resolve_relation_raw_key(item) == key), None)
        canonical_type_en, display_type_en = _resolve_canonical_relation_names(raw_key=key, relation_row=relation_row)
        canonical_type_en = _deduplicate_canonical_type_en(
            canonical_type_en=canonical_type_en,
            raw_key=key,
            used_canonical_types=used_canonical_types,
        )
        display_type_zh = _resolve_display_type_zh(raw_key=key, relation_row=relation_row)
        mapping[key] = {
            "raw_key": key,
            "canonical_type_en": canonical_type_en,
            "display_type_zh": display_type_zh,
            "display_type_en": display_type_en,
            "display_type_bilingual": f"{display_type_zh} / {display_type_en}",
            "legacy_token": str(relation_type_mappings.get(key, {}).get("token") or "").strip() or None,
        }

    mapping["HAS_OBSERVATION"] = {
        "raw_key": "HAS_OBSERVATION",
        "canonical_type_en": "HAS_OBSERVATION",
        "display_type_zh": "包含观测",
        "display_type_en": "Has Observation",
        "display_type_bilingual": "包含观测 / Has Observation",
        "legacy_token": "HAS_OBSERVATION",
    }

    zh_usage: dict[str, str] = {}
    for item in sorted(mapping.values(), key=lambda current: current["canonical_type_en"]):
        teaching_type_zh = item["display_type_zh"]
        existing = zh_usage.get(teaching_type_zh)
        if existing and existing != item["canonical_type_en"]:
            teaching_type_zh = f"{teaching_type_zh}【{item['canonical_type_en']}】"
        zh_usage[teaching_type_zh] = item["canonical_type_en"]
        item["teaching_type_zh"] = teaching_type_zh
    return mapping


def _resolve_relation_raw_key(row: dict[str, str]) -> str:
    for candidate in (
        row.get("canonical_type"),
        row.get("type"),
        row.get("relation_key"),
        row.get("raw_type"),
        row.get("neo4j_rel_type"),
    ):
        value = str(candidate or "").strip()
        if value:
            return value
    return ""


def _resolve_canonical_relation_names(
    *,
    raw_key: str,
    relation_row: dict[str, str] | None,
) -> tuple[str, str]:
    if raw_key in CANONICAL_RELATION_EN_MAP:
        return CANONICAL_RELATION_EN_MAP[raw_key]

    for candidate in (
        (relation_row or {}).get("canonical_type_en"),
        (relation_row or {}).get("relation_key"),
        (relation_row or {}).get("canonical_type"),
        (relation_row or {}).get("type"),
        (relation_row or {}).get("raw_type"),
    ):
        normalized = _normalize_canonical_type_en(candidate)
        if normalized:
            return normalized, _titleize_canonical_type(normalized)

    fallback = _fallback_canonical_type_en(raw_key)
    return fallback, _titleize_canonical_type(fallback)


def _resolve_display_type_zh(*, raw_key: str, relation_row: dict[str, str] | None) -> str:
    candidates: list[str] = []
    if relation_row is not None:
        candidates.extend(
            [
                str(relation_row.get("display_type_zh") or "").strip(),
                str(relation_row.get("canonical_type") or "").strip(),
                str(relation_row.get("type") or "").strip(),
                str(relation_row.get("relation_key") or "").strip(),
                str(relation_row.get("raw_type") or "").strip(),
            ]
        )
    candidates.append(str(raw_key or "").strip())
    for candidate in candidates:
        if candidate:
            return candidate
    return "未知关系"


def _normalize_canonical_type_en(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    ascii_only = re.sub(r"[^0-9A-Za-z]+", "_", raw).strip("_").upper()
    if not ascii_only:
        return ""
    if ascii_only[0].isdigit():
        ascii_only = f"REL_{ascii_only}"
    return ascii_only


def _fallback_canonical_type_en(raw_key: str) -> str:
    normalized = _normalize_canonical_type_en(raw_key)
    if normalized:
        return normalized
    digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:10].upper()
    return f"REL_{digest}"


def _deduplicate_canonical_type_en(
    *,
    canonical_type_en: str,
    raw_key: str,
    used_canonical_types: dict[str, str],
) -> str:
    existing = used_canonical_types.get(canonical_type_en)
    if existing is None or existing == raw_key:
        used_canonical_types[canonical_type_en] = raw_key
        return canonical_type_en
    digest = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()[:6].upper()
    deduplicated = f"{canonical_type_en}_{digest}"
    used_canonical_types[deduplicated] = raw_key
    return deduplicated


def _titleize_canonical_type(canonical_type_en: str) -> str:
    return " ".join(part.capitalize() for part in canonical_type_en.split("_") if part)


def _augment_type_mappings(
    *,
    type_mappings: dict[str, Any],
    relation_mapping: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    relation_types = dict(type_mappings.get("relation_types") or {})
    for raw_key, details in relation_mapping.items():
        next_item = dict(relation_types.get(raw_key) or {})
        next_item["canonical_type_en"] = details["canonical_type_en"]
        next_item["display_type_zh"] = details["display_type_zh"]
        next_item["display_type_en"] = details["display_type_en"]
        next_item["display_type_bilingual"] = details["display_type_bilingual"]
        next_item["teaching_type_zh"] = details["teaching_type_zh"]
        if details.get("legacy_token"):
            next_item["legacy_token"] = details["legacy_token"]
        relation_types[raw_key] = next_item
    type_mappings["relation_types"] = relation_types
    type_mappings["relation_type_export_strategy"] = {
        "default_relation_type_field": "canonical_type_en",
        "teaching_display_fields": ["display_type_zh", "display_type_bilingual"],
        "experimental_zh_type_field": "teaching_type_zh",
    }
    return type_mappings


def _augment_relationship_csv_content(
    *,
    content: str,
    relation_mapping: dict[str, dict[str, Any]],
    is_admin: bool,
) -> str:
    fieldnames, rows = _read_csv_content(content)
    for row in rows:
        raw_key = _resolve_relation_raw_key(row)
        details = relation_mapping.get(raw_key) or relation_mapping.get("UNKNOWN_RELATION")
        if details is None:
            details = {
                "canonical_type_en": _fallback_canonical_type_en(raw_key),
                "display_type_zh": raw_key or "未知关系",
                "display_type_en": _titleize_canonical_type(_fallback_canonical_type_en(raw_key)),
                "display_type_bilingual": f"{raw_key or '未知关系'} / {_titleize_canonical_type(_fallback_canonical_type_en(raw_key))}",
            }
        row["canonical_type_en"] = details["canonical_type_en"]
        row["display_type_zh"] = details["display_type_zh"]
        row["display_type_en"] = details["display_type_en"]
        row["display_type_bilingual"] = details["display_type_bilingual"]
        if is_admin:
            row[":TYPE"] = details["canonical_type_en"]
    next_fieldnames = list(fieldnames)
    for column in ("canonical_type_en", "display_type_zh", "display_type_en", "display_type_bilingual"):
        if column not in next_fieldnames:
            next_fieldnames.append(column)
    return _write_csv_content(rows=rows, fieldnames=next_fieldnames)


def _augment_observation_link_csv_content(*, content: str, is_admin: bool) -> str:
    fieldnames, rows = _read_csv_content(content)
    for row in rows:
        if is_admin:
            row[":TYPE"] = "HAS_OBSERVATION"
        row["canonical_type_en"] = "HAS_OBSERVATION"
        row["display_type_zh"] = "包含观测"
        row["display_type_en"] = "Has Observation"
        row["display_type_bilingual"] = "包含观测 / Has Observation"
    next_fieldnames = list(fieldnames)
    for column in ("canonical_type_en", "display_type_zh", "display_type_en", "display_type_bilingual"):
        if column not in next_fieldnames:
            next_fieldnames.append(column)
    return _write_csv_content(rows=rows, fieldnames=next_fieldnames)


def _read_csv_content(content: str) -> tuple[list[str], list[dict[str, str]]]:
    buffer = io.StringIO(content)
    reader = csv.DictReader(buffer)
    rows = [dict(row) for row in reader]
    return list(reader.fieldnames or []), rows


def _write_csv_content(*, rows: list[dict[str, str]], fieldnames: list[str]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({name: str(row.get(name) or "") for name in fieldnames})
    return buffer.getvalue()


def _build_csv_url_base(*, raw_base_url: str, bundle_name: str) -> str:
    return f"{str(raw_base_url).rstrip('/')}/{bundle_name}/neo4j/cypher"


def _build_import_cypher(
    *,
    csv_base: str,
    relation_mapping: dict[str, dict[str, Any]],
    local: bool,
    teaching: bool,
) -> str:
    entities_path = _csv_path(csv_base=csv_base, filename="entities.csv", local=local)
    relationships_path = _csv_path(csv_base=csv_base, filename="relationships.csv", local=local)
    observations_path = _csv_path(csv_base=csv_base, filename="observations.csv", local=local)
    links_path = _csv_path(csv_base=csv_base, filename="entity_has_observation.csv", local=local)
    lines = [
        "CREATE CONSTRAINT entity_entity_id IF NOT EXISTS FOR (n:Entity) REQUIRE n.entity_id IS UNIQUE;",
        "CREATE CONSTRAINT observation_observation_id IF NOT EXISTS FOR (n:Observation) REQUIRE n.observation_id IS UNIQUE;",
        "",
        f"LOAD CSV WITH HEADERS FROM '{entities_path}' AS row",
        "MERGE (n:Entity {entity_id: row.entity_id})",
        "SET n.canonical_entity_id = row.canonical_entity_id,",
        "    n.canonical_key = row.canonical_key,",
        "    n.display_name = row.display_name,",
        "    n.normalized_name = row.normalized_name,",
        "    n.name = row.name,",
        "    n.primary_type = row.primary_type,",
        "    n.type = row.type,",
        "    n.type_labels = CASE WHEN row.type_labels_pipe = '' THEN [] ELSE split(row.type_labels_pipe, '|') END,",
        "    n.type_labels_json = row.type_labels_json,",
        "    n.description = row.description,",
        "    n.attributes_json = row.attributes_json,",
        "    n.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,",
        "    n.source_chunk_ids_json = row.source_chunk_ids_json,",
        "    n.entity_occurrence_ids = CASE WHEN row.entity_occurrence_ids_pipe = '' THEN [] ELSE split(row.entity_occurrence_ids_pipe, '|') END,",
        "    n.entity_occurrence_ids_json = row.entity_occurrence_ids_json;",
        "",
        f"LOAD CSV WITH HEADERS FROM '{observations_path}' AS row",
        "MERGE (o:Observation {observation_id: row.observation_id})",
        "SET o.text = row.text,",
        "    o.kind = row.kind,",
        "    o.source_kind = row.source_kind,",
        "    o.attribute_key = row.attribute_key,",
        "    o.attribute_value = row.attribute_value,",
        "    o.relation_type = row.relation_type,",
        "    o.peer_entity_id = row.peer_entity_id,",
        "    o.peer_entity_name = row.peer_entity_name,",
        "    o.direction = row.direction,",
        "    o.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,",
        "    o.source_chunk_ids_json = row.source_chunk_ids_json;",
        "",
        f"LOAD CSV WITH HEADERS FROM '{links_path}' AS row",
        "MATCH (e:Entity {entity_id: row.entity_id})",
        "MATCH (o:Observation {observation_id: row.observation_id})",
        f"MERGE (e)-[r:{_quote_relationship_type(_observation_relation_type(teaching=teaching, relation_mapping=relation_mapping))}]->(o)",
        "SET r.canonical_type_en = row.canonical_type_en,",
        "    r.display_type_zh = row.display_type_zh,",
        "    r.display_type_en = row.display_type_en,",
        "    r.display_type_bilingual = row.display_type_bilingual;",
    ]

    relation_items = [
        item
        for raw_key, item in relation_mapping.items()
        if raw_key != "HAS_OBSERVATION"
    ]
    relation_items.sort(key=lambda item: item["canonical_type_en"])
    for item in relation_items:
        lines.extend(
            [
                "",
                f"LOAD CSV WITH HEADERS FROM '{relationships_path}' AS row",
                f"WITH row WHERE row.canonical_type_en = '{item['canonical_type_en']}'",
                "MATCH (source:Entity {entity_id: row.source_entity_id})",
                "MATCH (target:Entity {entity_id: row.target_entity_id})",
                f"CREATE (source)-[r:{_quote_relationship_type(_relation_type_for_script(item=item, teaching=teaching))}]->(target)",
                "SET r.relationship_id = row.relationship_id,",
                "    r.neo4j_rel_type = row.neo4j_rel_type,",
                "    r.source = row.source,",
                "    r.target = row.target,",
                "    r.source_role = row.source_role,",
                "    r.target_role = row.target_role,",
                "    r.relation_key = row.relation_key,",
                "    r.canonical_type = row.canonical_type,",
                "    r.canonical_type_en = row.canonical_type_en,",
                "    r.display_type_zh = row.display_type_zh,",
                "    r.display_type_en = row.display_type_en,",
                "    r.display_type_bilingual = row.display_type_bilingual,",
                "    r.type = row.type,",
                "    r.raw_type = row.raw_type,",
                "    r.raw_types = CASE WHEN row.raw_types_pipe = '' THEN [] ELSE split(row.raw_types_pipe, '|') END,",
                "    r.raw_types_json = row.raw_types_json,",
                "    r.description = row.description,",
                "    r.normalization_status = row.normalization_status,",
                "    r.matched_schema_json = row.matched_schema_json,",
                "    r.source_chunk_ids = CASE WHEN row.source_chunk_ids_pipe = '' THEN [] ELSE [x IN split(row.source_chunk_ids_pipe, '|') | toInteger(x)] END,",
                "    r.source_chunk_ids_json = row.source_chunk_ids_json;",
            ]
        )
    lines.append("")
    return "\n".join(lines)


def _csv_path(*, csv_base: str, filename: str, local: bool) -> str:
    if local:
        return f"file:///{filename}"
    return f"{csv_base.rstrip('/')}/{filename}"


def _relation_type_for_script(*, item: dict[str, Any], teaching: bool) -> str:
    if teaching:
        return str(item["teaching_type_zh"])
    return str(item["canonical_type_en"])


def _observation_relation_type(*, teaching: bool, relation_mapping: dict[str, dict[str, Any]]) -> str:
    if not teaching:
        return "HAS_OBSERVATION"
    return str(relation_mapping["HAS_OBSERVATION"]["teaching_type_zh"])


def _quote_relationship_type(value: str) -> str:
    if RELATION_TYPE_IDENT_RE.fullmatch(value):
        return value
    escaped = value.replace("`", "``")
    return f"`{escaped}`"


def _build_relation_view_profiles(*, relation_view_profile: str, emit_experimental_zh_import: bool) -> dict[str, Any]:
    profiles = {
        "selected_profile": relation_view_profile,
        "engineering": {
            "neo4j_relation_type": "canonical_type_en",
            "display_fields": ["display_type_en", "display_type_bilingual"],
            "query_style": "Use MATCH ()-[r:CANONICAL_TYPE_EN]->() for precise programmatic queries.",
        },
        "teaching": {
            "neo4j_relation_type": "canonical_type_en",
            "display_fields": ["display_type_zh", "display_type_bilingual"],
            "query_style": "Keep English relation types for stable queries, but show Chinese or bilingual captions from relationship properties.",
        },
    }
    if emit_experimental_zh_import:
        profiles["experimental"] = {
            "neo4j_relation_type": "teaching_type_zh",
            "display_fields": ["display_type_zh", "display_type_bilingual"],
            "note": "Experimental only. Enabled via --emit-experimental-zh-import.",
        }
    return profiles


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


def _write_neo4j_readme(
    *,
    neo4j_dir: Path,
    import_targets: list[dict[str, Any]],
    relation_view_profile: str,
    emit_experimental_zh_import: bool,
) -> None:
    lines = [
        "# Neo4j Import Guide",
        "",
        "## Default Imports",
        "- Cloud engineering import: `neo4j/cypher/import.cypher`",
        "- Local engineering import: `neo4j/cypher/import.local.cypher`",
        "- Both use English `canonical_type_en` as the Neo4j relationship type.",
        "- Chinese or bilingual teaching labels come from relationship properties: `display_type_zh` and `display_type_bilingual`.",
        "",
        "## Why The Cloud Script Works Directly",
        "- `import.cypher` uses `raw.githubusercontent.com` URLs, so Neo4j Aura can fetch CSV files without local `file:///` access.",
        "",
        "## Engineering vs Teaching",
        "- Engineering mode keeps stable English relation types for queries, statistics, and downstream programs.",
        "- Teaching mode uses the same English relation types, but you display `display_type_zh` or `display_type_bilingual` in Browser/Bloom captions or result tables.",
        "",
        "## Visualization Hint",
        "- In Neo4j Browser or Bloom, set the relationship caption/text field to `display_type_zh` or `display_type_bilingual` when you want Chinese or bilingual labels.",
        "",
        "## Example Queries",
        "```cypher",
        "MATCH ()-[r:CAUSES]->()",
        "RETURN type(r) AS canonical_type_en, r.display_type_bilingual AS display_name, count(*) AS relation_count",
        "ORDER BY relation_count DESC",
        "LIMIT 10;",
        "```",
        "",
        "```cypher",
        "MATCH ()-[r]->()",
        "RETURN type(r) AS canonical_type_en, r.display_type_zh AS zh_label, r.display_type_bilingual AS bilingual_label",
        "LIMIT 20;",
        "```",
        "",
        f"- Selected relation view profile for this bundle: `{relation_view_profile}`",
    ]
    if emit_experimental_zh_import:
        lines.extend(
            [
                "",
                "## Experimental Chinese Relation Type Scripts",
                "- Cloud experimental script: `neo4j/cypher/import.zh.cypher`",
                "- Local experimental script: `neo4j/cypher/import.zh.local.cypher`",
                "- These scripts are optional and do not replace the default engineering import path.",
            ]
        )
    target_summary = [f"- `{item['path']}` ({item['variant']}, profile={item['profile']})" for item in import_targets]
    if target_summary:
        lines.extend(["", "## Generated Scripts", *target_summary])
    (neo4j_dir / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _int_or_none(value: Any) -> int | None:
    try:
        current = int(value)
    except (TypeError, ValueError):
        return None
    return current if current > 0 else None


if __name__ == "__main__":
    main()
