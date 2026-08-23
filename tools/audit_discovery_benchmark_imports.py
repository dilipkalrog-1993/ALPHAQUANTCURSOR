#!/usr/bin/env python3
"""Print and validate the benchmark's complete local static import graph."""
from __future__ import annotations

import ast
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENTRYPOINT = "tools.run_discovery_benchmark"
FORBIDDEN_MODULES = {"streamlit", "app", "appemergentquant_v3_1"}
FORBIDDEN_TEXT = {"session_state", "cache_data", "cache_resource"}


def module_path(module: str) -> Path | None:
    candidate = ROOT.joinpath(*module.split("."))
    for path in (candidate.with_suffix(".py"), candidate / "__init__.py"):
        if path.is_file():
            return path
    return None


def imported_modules(module: str, path: Path) -> set[str]:
    imports: set[str] = set()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    package = module.split(".")[:-1] if path.name != "__init__.py" else module.split(".")
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            prefix = package[:len(package) - node.level + 1] if node.level else []
            base = ".".join([*prefix, node.module or ""]).strip(".")
            if base:
                imports.add(base)
    return imports


def audit(entrypoint: str = ENTRYPOINT) -> tuple[list[tuple[str, Path]], list[str]]:
    queue = deque([entrypoint])
    seen: set[str] = set()
    local: list[tuple[str, Path]] = []
    violations: list[str] = []
    while queue:
        module = queue.popleft()
        if module in seen:
            continue
        seen.add(module)
        parts = module.split(".")
        for end in range(1, len(parts)):
            package = ".".join(parts[:end])
            if module_path(package) is not None and package not in seen:
                queue.append(package)
        path = module_path(module)
        if path is None:
            if module.split(".")[0] in FORBIDDEN_MODULES:
                violations.append(f"forbidden import: {module}")
            continue
        local.append((module, path.relative_to(ROOT)))
        source = path.read_text(encoding="utf-8")
        for token in FORBIDDEN_TEXT:
            if token in source:
                violations.append(f"forbidden UI reference: {module}: {token}")
        for imported in imported_modules(module, path):
            if imported.split(".")[0] in FORBIDDEN_MODULES:
                violations.append(f"forbidden import: {module} -> {imported}")
            if module_path(imported) is not None:
                queue.append(imported)
    return sorted(local), sorted(set(violations))


def main() -> int:
    local, violations = audit()
    print(f"Local modules reachable from {ENTRYPOINT}:")
    for module, path in local:
        print(f"- {module} ({path})")
    if violations:
        print("VIOLATIONS:")
        for violation in violations:
            print(f"- {violation}")
        return 1
    print(f"PASS: {len(local)} local modules; no Streamlit/UI dependencies")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
