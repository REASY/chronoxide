#!/usr/bin/env python3
"""Load an exact publication-scale validator bundle under isolated Python."""

from __future__ import annotations

import hashlib
import stat
import sys
import types
from pathlib import Path


def _source_bytes(bundle: Path, name: str) -> tuple[Path, bytes]:
    path = bundle / name
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise RuntimeError(f"validator bundle source is not a regular file: {path}")
    return path, path.read_bytes()


def _load_module(name: str, path: Path, source: bytes) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__file__ = str(path)
    module.__package__ = ""
    sys.modules[name] = module
    exec(compile(source, str(path), "exec"), module.__dict__)
    return module


def _source_authority(path: Path, source: bytes) -> dict[str, str]:
    return {
        "path": str(path),
        "sha256": hashlib.sha256(source).hexdigest(),
    }


def main() -> None:
    if not (
        sys.flags.isolated
        and sys.flags.no_site
        and sys.flags.dont_write_bytecode
    ):
        raise RuntimeError("validator bootstrap requires Python -I -S -B")
    bundle = Path(__file__).resolve().parent
    bootstrap_path, bootstrap_source = _source_bytes(
        bundle, "live_query_scale_validator_bootstrap.py"
    )
    phase1_path, phase1_source = _source_bytes(bundle, "phase1_replay_gate.py")
    _load_module("phase1_replay_gate", phase1_path, phase1_source)
    gate_path, gate_source = _source_bytes(
        bundle, "live_query_ingest_ab_gate.py"
    )
    globals_for_gate = {
        "__builtins__": __builtins__,
        "__file__": str(gate_path),
        "__name__": "__main__",
        "__package__": "",
        "_CHRONOXIDE_SCALE_VALIDATOR_BOOTSTRAP": {
            "schema": "chronoxide/live-query-publication-scale-bootstrap/v1",
            "bootstrap": _source_authority(
                bootstrap_path, bootstrap_source
            ),
            "entrypoint": _source_authority(gate_path, gate_source),
            "phase1": _source_authority(phase1_path, phase1_source),
        },
    }
    exec(compile(gate_source, str(gate_path), "exec"), globals_for_gate)


if __name__ == "__main__":
    main()
