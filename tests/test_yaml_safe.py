# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the centralized sexagesimal-safe YAML helpers."""

import io
from pathlib import Path

import yaml

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


def test_load_yaml_matches_pyyaml_except_sexagesimal():
    """Our loader is byte-identical to yaml.safe_load for every non-sexagesimal scalar.

    This locks in the guarantee that stripping the base-60 branch did not change
    resolution of any other literal. The edge cases below (unsigned exponent stays a
    string, signed exponent is a float, ``0o17`` stays a string) are ones a naive
    hand-written resolver gets wrong, so they are asserted explicitly.
    """
    non_sexagesimal = [
        "42",
        "-7",
        "+9",
        "0o17",
        "0x1F",
        "0b1010",
        "1.5",
        "-3.2e4",
        "3.2e+4",
        "1_000",
        ".inf",
        "-.inf",
        "true",
        "false",
        "null",
        "~",
        "hello",
        "'18:00:00'",
        "2001-12-15",
        "2001-12-15T02:59:43Z",
        "[1, 2, 3]",
        "{a: 1}",
        "3723",
        "64800",
    ]
    for raw in non_sexagesimal:
        expected = yaml.safe_load(raw)
        actual = load_yaml(raw)
        assert type(actual) is type(expected), f"type differs for {raw!r}"
        assert actual == expected, f"value differs for {raw!r}"


def test_load_yaml_only_sexagesimal_differs():
    """Colon-separated time literals are the only inputs that diverge from PyYAML.

    Note PyYAML's sexagesimal-int branch requires a leading 1-9, so ``08:30:00``
    already survives as a string in stock PyYAML; only non-zero-leading hours such
    as ``18:00:00`` are corrupted. Our loader keeps all of them as strings.
    """
    corrupted_by_pyyaml = ["18:00:00", "1:2:3"]
    for raw in corrupted_by_pyyaml:
        assert isinstance(yaml.safe_load(raw), int)
        assert load_yaml(raw) == raw

    for raw in ["18:00:00", "08:30:00", "1:2:3", "23:59:59"]:
        assert load_yaml(raw) == raw


def test_dump_yaml_matches_pyyaml_for_normal_data():
    """dump_yaml output matches stock yaml.dump (same options) for non-time data."""
    data = {"count": 42, "ratio": 1.5, "flag": True, "name": "hello", "items": [1, 2, 3]}
    expected = yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=False)
    assert dump_yaml(data) == expected


def test_strip_sexagesimal_branch_rejects_unexpected_format():
    """The resolver strip guards against a PyYAML wrapper-format change instead of corrupting silently."""
    import re

    import pytest

    from fabric_cicd._common._yaml_safe import _strip_sexagesimal_branch

    with pytest.raises(RuntimeError):
        _strip_sexagesimal_branch(re.compile(r"^[0-9]+$"))


def test_dumper_resolvers_derived_from_safedumper():
    """SafeYamlDumper's resolvers derive from SafeDumper's own table, not SafeLoader's."""
    from fabric_cicd._common._yaml_safe import SafeYamlDumper

    assert set(SafeYamlDumper.yaml_implicit_resolvers) == set(yaml.SafeDumper.yaml_implicit_resolvers)


def test_no_raw_yaml_round_trip_in_src():
    """Guardrail: no source module bypasses the shared helpers with raw load/dump.

    Raw ``yaml.safe_load`` / ``yaml.dump`` (and bare ``yaml.load(x)``) re-introduce
    the YAML 1.1 sexagesimal corruption. All loading and dumping must
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
