from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

# ===========================================================
# 1) Tipos / props a relevar (same as nifi_microservicios_scripts.py)
# ===========================================================

SCRIPT_TARGETS: dict[str, list[str]] = {
    "ExecuteScript": [
        "org.apache.nifi.processors.script.ExecuteScript",
    ],
    "InvokeScriptedProcessor": [
        "org.apache.nifi.processors.script.InvokeScriptedProcessor",
    ],
    "ExecuteProcess": [
        "org.apache.nifi.processors.standard.ExecuteProcess",
    ],
    "ExecuteStreamCommand": [
        "org.apache.nifi.processors.standard.ExecuteStreamCommand",
    ],
    "GroovyJSR223": [
        "org.apache.nifi.processors.script.ExecuteGroovyScript",
    ],
}

SCRIPT_PROP_KEYS = [
    "Script File",
    "Script Body",
    "Module Directory",
    "Script Engine",
    "Command",
    "Command Path",
    "Command Arguments",
    "Arguments",
    "Working Directory",
]

# Fallback detection: if any of these props are non-empty, treat as script processor
FALLBACK_PROP_TRIGGERS = {"Script File", "Script Body", "Command", "Command Path"}


# ===========================================================
# 2) Data model
# ===========================================================

@dataclass
class ScriptRef:
    """A single script/command reference found in a processor."""
    pg_path: str           # Full » separated path of process groups
    pg_id: str
    pg_name: str
    processor_name: str
    processor_id: str
    processor_type: str    # short type name
    categoria: str         # key from SCRIPT_TARGETS or "DetectadoPorProp"
    prop_name: str
    ref_type: str          # file / inline / command / param
    value: str             # actual value (Script Body is truncated)
    extra: str = ""        # e.g. "len=4321" for inline bodies


# ===========================================================
# 3) Processor helpers
# ===========================================================

def _get_processor_type(processor: dict) -> str:
    p = processor.get("component", processor)
    return str(p.get("type") or "")


def _get_processor_name(processor: dict) -> str:
    p = processor.get("component", processor)
    return str(p.get("name") or "")


def _get_processor_id(processor: dict) -> str:
    p = processor.get("component", processor)
    return str(p.get("instanceIdentifier") or p.get("id") or "")


def _get_props(processor: dict) -> dict[str, str]:
    p = processor.get("component", processor)
    raw = (
        ((p.get("config") or {}).get("properties") or {})
        or (p.get("properties") or {})
        or {}
    )
    return {k: v for k, v in raw.items() if v not in (None, "", " ", {})}


def _processor_categoria(ptype: str) -> str | None:
    for categoria, tipos in SCRIPT_TARGETS.items():
        if ptype in tipos:
            return categoria
    return None


def _classify_prop(key: str, value: str) -> tuple[str, str, str]:
    """Returns (ref_type, stored_value, extra)."""
    if key == "Script Body":
        resumen = value[:200].replace("\n", " ").replace("\r", " ")
        return "inline", resumen, f"len={len(value)}"
    elif key in ("Script File", "Module Directory"):
        return "file", value, ""
    elif key in ("Command", "Command Path"):
        return "command", value, ""
    else:
        return "param", value, ""


def extract_script_refs_from_processor(
    processor: dict, pg_id: str, pg_name: str, pg_path: str
) -> list[ScriptRef]:
    """Extract all script/command references from a single processor."""
    results: list[ScriptRef] = []
    ptype = _get_processor_type(processor)
    pname = _get_processor_name(processor)
    pid = _get_processor_id(processor)
    props = _get_props(processor)
    short_type = ptype.split(".")[-1]

    categoria = _processor_categoria(ptype)

    # Caso A: known script processor type
    if categoria:
        for key in SCRIPT_PROP_KEYS:
            value = props.get(key)
            if not value:
                continue
            ref_type, stored, extra = _classify_prop(key, value)
            results.append(ScriptRef(
                pg_path=pg_path, pg_id=pg_id, pg_name=pg_name,
                processor_name=pname, processor_id=pid, processor_type=short_type,
                categoria=categoria, prop_name=key,
                ref_type=ref_type, value=stored, extra=extra
            ))
        return results

    # Caso B: not matched by type, but has script/command props
    if any(props.get(k) for k in FALLBACK_PROP_TRIGGERS):
        for key in SCRIPT_PROP_KEYS:
            value = props.get(key)
            if not value:
                continue
            ref_type, stored, extra = _classify_prop(key, value)
            results.append(ScriptRef(
                pg_path=pg_path, pg_id=pg_id, pg_name=pg_name,
                processor_name=pname, processor_id=pid, processor_type=short_type,
                categoria="DetectadoPorProp", prop_name=key,
                ref_type=ref_type, value=stored, extra=extra
            ))

    return results


# ===========================================================
# 4) Flow file traversal
# ===========================================================

def _as_component(obj: dict) -> dict:
    return obj.get("component", obj) if isinstance(obj, dict) else {}


def walk_process_group(
    pg: dict,
    parent_path: list[str],
    results: list[ScriptRef],
) -> None:
    pg = _as_component(pg)
    contents = pg.get("contents") or {}

    pg_id = pg.get("instanceIdentifier") or pg.get("id") or ""
    pg_name = pg.get("name") or "SIN_NOMBRE"
    current_path = parent_path + [pg_name]
    path_str = " » ".join(current_path)

    processors: list[dict] = contents.get("processors") or pg.get("processors") or []
    child_groups: list[dict] = contents.get("processGroups") or pg.get("processGroups") or []

    for processor in processors:
        refs = extract_script_refs_from_processor(processor, pg_id, pg_name, path_str)
        results.extend(refs)

    for child in child_groups:
        walk_process_group(child, current_path, results)


def get_root_group(flow: dict) -> dict:
    if "flow" in flow and "rootGroup" in flow["flow"]:
        return flow["flow"]["rootGroup"]
    return flow.get("rootGroup", flow)


def find_scripts_in_flow(flow_path: Path) -> list[ScriptRef]:
    with flow_path.open("r", encoding="utf-8") as f:
        flow = json.load(f)
    root = get_root_group(flow)
    results: list[ScriptRef] = []
    walk_process_group(root, [], results)
    return results


# ===========================================================
# 5) Pretty print
# ===========================================================

def print_results(results: list[ScriptRef], flow_name: str) -> None:
    if not results:
        print(f"  [No script/command references found in {flow_name}]")
        return

    # Group by PG path for readability
    by_pg: dict[str, list[ScriptRef]] = {}
    for r in results:
        by_pg.setdefault(r.pg_path, []).append(r)

    for pg_path, refs in by_pg.items():
        print(f"\n  PG: {pg_path}")
        for r in refs:
            extra_str = f"  ({r.extra})" if r.extra else ""
            print(f"    [{r.categoria}] {r.processor_name} ({r.processor_type})")
            print(f"      prop      : {r.prop_name}")
            print(f"      ref_type  : {r.ref_type}")
            print(f"      value     : {r.value}{extra_str}")


# ===========================================================
# 6) Main
# ===========================================================

FLOWS_DIR = Path("flows")

def main():
    flow_files = list(FLOWS_DIR.glob("*.json"))
    if not flow_files:
        print(f"No .json flow files found in {FLOWS_DIR}/")
        sys.exit(1)

    all_results: list[ScriptRef] = []

    for flow_path in sorted(flow_files):
        print(f"\n{'='*60}")
        print(f"Flow: {flow_path.name}")
        print('='*60)
        try:
            refs = find_scripts_in_flow(flow_path)
            all_results.extend(refs)
            print_results(refs, flow_path.name)
        except Exception as e:
            print(f"  [ERROR] Failed to process {flow_path.name}: {e}")

    print(f"\n{'='*60}")
    print(f"TOTAL script references found: {len(all_results)}")


if __name__ == "__main__":
    main()
