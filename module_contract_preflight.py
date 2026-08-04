from __future__ import annotations

import ast
import hashlib
import importlib.util
import os
import shutil
import sys
from pathlib import Path

RELEASE_VERSION = "V73.3.6.6.11"

CONTRACTS = [
    {
        "label": "HAM",
        "filename": "ham_restart_research.py",
        "module": "ham_restart_research",
        "version": "V73.3.6.5",
        "callables": ("run_backtest", "run_capture", "_empty_report"),
        "exit_code": 67,
    },
    {
        "label": "FAMILIAR",
        "filename": "familiar_research.py",
        "module": "familiar_research",
        "version": "V73.3.6.6",
        "callables": ("run_backtest", "force_report", "build_shadow_brief"),
        "exit_code": 86,
    },
    {
        "label": "PATTERN_AI_CROSS",
        "filename": "pattern_ai_cross_research.py",
        "module": "pattern_ai_cross_research",
        "version": "V73.3.6.6.2",
        "callables": ("run_backtest", "force_report", "build_shadow_brief", "capture_signal_provenance"),
        "exit_code": 93,
    },
    {
        "label": "MARKET_EXCESS",
        "filename": "market_regime_excess_research.py",
        "module": "market_regime_excess_research",
        "version": "V73.3.6.6.3",
        "callables": ("run_backtest", "force_report"),
        "exit_code": 104,
    },
    {
        "label": "FORMULA_TRUTH",
        "filename": "search_formula_truth_audit.py",
        "module": "search_formula_truth_audit",
        "version": "V73.3.6.6.8",
        "callables": ("capture_truth", "attach_result", "attach_post_result", "run_backtest", "force_report", "load_registry"),
        "exit_code": 115,
    },
    {
        "label": "FORMULA_UNIVERSE",
        "filename": "search_formula_universe_audit.py",
        "module": "search_formula_universe_audit",
        "version": "V73.3.6.6.9",
        "callables": ("run_backtest", "force_report"),
        "exit_code": 116,
    },
    {
        "label": "FORMULA_COMPLETE",
        "filename": "search_formula_complete_pipeline.py",
        "module": "search_formula_complete_pipeline",
        "version": "V73.3.6.6.10.1",
        "callables": ("run_backtest", "force_report", "causal_anchor_v1"),
        "exit_code": 117,
    },
    {
        "label": "CATALYST_SOURCES",
        "filename": "catalyst_source_adapters.py",
        "module": "catalyst_source_adapters",
        "version": "V73.3.6.6.11",
        "callables": ("capture_forward", "ensure_templates", "normalize_rows"),
        "exit_code": 133,
    },
    {
        "label": "SEQUENCE_CONTEXT_CATALYST",
        "filename": "pattern_catalyst_context_pipeline.py",
        "module": "pattern_catalyst_context_pipeline",
        "version": "V73.3.6.6.11",
        "callables": ("run_backtest", "force_report", "sequence_state_v1"),
        "exit_code": 134,
    },
]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ast_contract(path: Path) -> tuple[str | None, bool | None]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    version = None
    research_only = None
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        value = node.value
        for target in targets:
            if not isinstance(target, ast.Name):
                continue
            if target.id == "VERSION" and isinstance(value, ast.Constant):
                version = value.value
            elif target.id == "RESEARCH_ONLY" and isinstance(value, ast.Constant):
                research_only = value.value
    return version, research_only


def load_exact(module_name: str, path: Path):
    # Never accept an older same-name module from scanner/, site-packages, or a stale sys.modules entry.
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot build import spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    workspace = Path(os.environ.get("GITHUB_WORKSPACE") or Path.cwd()).resolve()
    print(f"MODULE_SOURCE_GUARD release={RELEASE_VERSION} workspace={workspace}")

    # Stale bytecode must not mask a newly overwritten source file.
    for pycache in (workspace / "__pycache__", workspace / "scanner" / "__pycache__"):
        if pycache.exists():
            shutil.rmtree(pycache, ignore_errors=True)
            print(f"MODULE_SOURCE_GUARD removed_pycache={pycache}")

    # Root is the only accepted source location. Other paths remain available for dependencies only.
    root_s = str(workspace)
    sys.path[:] = [root_s] + [p for p in sys.path if str(Path(p or ".").resolve()) != root_s]

    for contract in CONTRACTS:
        path = (workspace / contract["filename"]).resolve()
        label = contract["label"]
        expected = contract["version"]
        if not path.is_file():
            print(
                f"{label}_PREFLIGHT module=MISSING research_only=None contract_ok=0 "
                f"expected={expected} module_file={path} reason=ROOT_FILE_MISSING"
            )
            print(f"❌ {contract['filename']} must be committed at the repository root.")
            return int(contract["exit_code"])

        try:
            static_version, static_research = ast_contract(path)
        except Exception as exc:
            print(
                f"{label}_PREFLIGHT module=UNREADABLE research_only=None contract_ok=0 "
                f"expected={expected} module_file={path} file_sha256={sha256(path)} "
                f"reason=AST_ERROR:{type(exc).__name__}:{exc}"
            )
            return int(contract["exit_code"])

        try:
            module = load_exact(contract["module"], path)
            runtime_path = Path(getattr(module, "__file__", "")).resolve()
            runtime_version = getattr(module, "VERSION", None)
            runtime_research = getattr(module, "RESEARCH_ONLY", None)
            callable_state = {
                name: callable(getattr(module, name, None)) for name in contract["callables"]
            }
            ok = (
                runtime_path == path
                and static_version == expected
                and runtime_version == expected
                and static_research is True
                and runtime_research is True
                and all(callable_state.values())
            )
            missing = ",".join(name for name, present in callable_state.items() if not present) or "-"
            print(
                f"{label}_PREFLIGHT module={runtime_version} research_only={runtime_research} "
                f"contract_ok={int(ok)} expected={expected} module_file={runtime_path} "
                f"root_file={path} file_sha256={sha256(path)} static_version={static_version} "
                f"missing_callables={missing}"
            )
            if not ok:
                print(
                    f"❌ {label} source contract mismatch. Overwrite repository-root "
                    f"{contract['filename']} with the release file; do not upload only run_scanner.yml."
                )
                return int(contract["exit_code"])
        except Exception as exc:
            print(
                f"{label}_PREFLIGHT module=IMPORT_ERROR research_only=None contract_ok=0 "
                f"expected={expected} module_file={path} file_sha256={sha256(path)} "
                f"reason={type(exc).__name__}:{exc}"
            )
            return int(contract["exit_code"])

    print("✅ HAM/FAMILIAR/PATTERN-AI-CROSS/MARKET-EXCESS/FORMULA-TRUTH/FORMULA-UNIVERSE/FORMULA-COMPLETE/CATALYST-SOURCES/SEQUENCE-CONTEXT-CATALYST exact-root source preflight PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
