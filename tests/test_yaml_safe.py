# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the centralized sexagesimal-safe YAML helpers (#1072)."""

import io
from pathlib import Path

from fabric_cicd._common._yaml_safe import dump_yaml, load_yaml

SRC_DIR = Path(__file__).resolve().parents[1] / "src" / "fabric_cicd"


def test_load_yaml_preserves_unquoted_time():
    """Unquoted HH:MM:SS values load as strings instead of base-60 integers."""
    data = load_yaml("start: 08:00:00\nend: 18:00:00\n")
    assert data == {"start": "08:00:00", "end": "18:00:00"}


def test_load_yaml_still_parses_normal_scalars():
    """Ordinary ints, floats, and booleans keep resolving as usual."""
    data = load_yaml("count: 42\nratio: 1.5\nflag: true\nname: hello\n")
    assert data == {"count": 42, "ratio": 1.5, "flag": True, "name": "hello"}


def test_load_yaml_accepts_stream():
    """load_yaml works with an open text stream, not just a string."""
    stream = io.StringIO("end: 18:00:00\n")
    assert load_yaml(stream) == {"end": "18:00:00"}


def test_dump_yaml_emits_unquoted_time():
    """A string time round-trips through dump/load unchanged and stays unquoted."""
    dumped = dump_yaml({"end": "18:00:00"})
    assert "18:00:00" in dumped
    assert "'18:00:00'" not in dumped
    assert load_yaml(dumped) == {"end": "18:00:00"}


def test_dump_yaml_preserves_key_order():
    """dump_yaml defaults to sort_keys=False so original ordering is kept."""
    dumped = dump_yaml({"z": 1, "a": 2})
    assert dumped.index("z:") < dumped.index("a:")


def test_no_raw_yaml_round_trip_in_src():
    """Guardrail: no source module bypasses the shared helpers with raw load/dump.

    Raw ``yaml.safe_load`` / ``yaml.dump`` (and bare ``yaml.load(x)``) re-introduce
    the YAML 1.1 sexagesimal corruption fixed in #1072. All loading and dumping must
    go through ``_yaml_safe`` (or ``yaml.load`` with an explicit sexagesimal-safe
    ``Loader=``), so this test fails if a raw round-trip call reappears.
    """
    forbidden = ("yaml.safe_load(", "yaml.safe_dump(", "yaml.dump(", "yaml.full_load(")
    offenders = []
    for path in SRC_DIR.rglob("*.py"):
        if path.name == "_yaml_safe.py":
            continue
        text = path.read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or '"""' in stripped:
                continue
            if any(token in line for token in forbidden):
                offenders.append(f"{path.name}: {stripped}")
            # bare yaml.load without an explicit safe Loader= is also forbidden
            if "yaml.load(" in line and "Loader=" not in line:
                offenders.append(f"{path.name}: {stripped}")
    assert not offenders, "Raw YAML round-trip calls found; route them through _yaml_safe:\n" + "\n".join(offenders)
