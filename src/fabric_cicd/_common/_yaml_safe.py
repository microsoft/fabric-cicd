# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Centralized, sexagesimal-safe YAML loading and dumping.

PyYAML implements YAML 1.1, whose implicit resolvers parse unquoted ``HH:MM:SS``
values as base-60 (sexagesimal) integers (for example ``18:00:00`` -> ``64800``).
When file content is round-tripped through ``load`` / ``dump``, this silently
corrupts time values.

This module is the single place YAML is loaded and dumped across the package.
:func:`load_yaml` and :func:`dump_yaml` strip only the base-60 branch from the
int/float implicit resolvers, so time strings are preserved (loaded as strings,
emitted unquoted) while normal integers, floats, octal, hex, and binary literals
resolve exactly as before. All other code should call these helpers instead of
using ``yaml.safe_load`` / ``yaml.dump`` directly, so this behavior stays
consistent and the corruption class cannot silently reappear.
"""

import re
from typing import Union

import yaml

# The base-60 sub-pattern PyYAML uses inside its int/float implicit resolvers.
# Any alternation branch containing it is the sexagesimal branch we want to drop.
_SEXAGESIMAL_SUBPATTERN = ":[0-5]?[0-9]"

# Tags whose implicit resolvers carry the sexagesimal branch.
_NUMERIC_TAGS = ("tag:yaml.org,2002:int", "tag:yaml.org,2002:float")


def _strip_sexagesimal_branch(rx: re.Pattern) -> re.Pattern:
    """Return a copy of a PyYAML numeric resolver with only its base-60 branch removed.

    PyYAML's int/float implicit resolvers are a top-level ``^(?: A | B | ... )$``
    alternation in which exactly the branches matching sexagesimal (``HH:MM:SS``)
    notation contain ``:[0-5]?[0-9]``. Dropping just those branches yields a resolver
    that is byte-for-byte identical to PyYAML's for every other input, so normal
    ints, floats, exponents, hex, octal, and binary literals resolve exactly as
    before while colon-separated time values fall through to being strings.

    Raises:
        RuntimeError: If the pattern is not the expected ``^(?:...)$`` wrapper. This
            makes an unexpected future PyYAML change fail loudly at import instead of
            silently producing a resolver that could re-corrupt time values.
    """
    prefix, suffix = "^(?:", ")$"
    pattern = rx.pattern
    if not (pattern.startswith(prefix) and pattern.endswith(suffix)):
        msg = f"Unexpected PyYAML numeric resolver format; cannot strip sexagesimal branch safely: {pattern!r}"
        raise RuntimeError(msg)

    inner = pattern[len(prefix) : -len(suffix)]
    # PyYAML's numeric resolvers are a flat top-level alternation. A couple of the
    # non-sexagesimal branches contain an inner ``|`` inside a group (for example
    # ``[-+]?(?:0|[1-9][0-9_]*)``). Splitting naively on ``|`` cuts those into
    # fragments, but none of those fragments contain _SEXAGESIMAL_SUBPATTERN, so they
    # are all kept and rejoined with ``|``, exactly reconstructing the original. Only
    # the whole sexagesimal branches (which have no inner ``|``) are dropped. The
    # re.compile below reuses the source pattern's own flags and would raise if a
    # future pattern ever broke this invariant.
    kept = [branch for branch in inner.split("|") if _SEXAGESIMAL_SUBPATTERN not in branch]
    return re.compile(prefix + "|".join(kept) + suffix, rx.flags)


def _sexagesimal_safe_resolvers(source_cls: type) -> dict:
    """Copy ``source_cls``'s implicit resolver table with the numeric base-60 branches removed.

    The table is derived from the given class's own resolvers (rather than a
    hard-coded ``SafeLoader``) so a loader is built from ``SafeLoader``'s table and a
    dumper from ``SafeDumper``'s, keeping each self-consistent even if PyYAML ever
    diverges the two.
    """
    resolvers = {}
    for first_char, mappings in source_cls.yaml_implicit_resolvers.items():
        resolvers[first_char] = [
            (tag, _strip_sexagesimal_branch(regexp) if tag in _NUMERIC_TAGS else regexp) for tag, regexp in mappings
        ]
    return resolvers


def _install_sexagesimal_safe_resolvers(cls: type) -> None:
    """Install a sexagesimal-free implicit resolver table on ``cls``.

    Derives the table from ``cls``'s own (inherited) PyYAML resolvers via
    :func:`_strip_sexagesimal_branch`, leaving every non-numeric resolver
    (bool, null, timestamp, etc.) untouched. Assigning the result shadows the
    inherited attribute without mutating the PyYAML base class.
    """
    cls.yaml_implicit_resolvers = _sexagesimal_safe_resolvers(cls)


class SafeYamlLoader(yaml.SafeLoader):
    """A ``SafeLoader`` that does not apply YAML 1.1 sexagesimal (base-60) parsing.

    Unquoted ``HH:MM:SS`` values are preserved as strings instead of being parsed
    as base-60 integers, while all other scalar types resolve as usual.
    """


class SafeYamlDumper(yaml.SafeDumper):
    """A ``SafeDumper`` counterpart to :class:`SafeYamlLoader`.

    With the sexagesimal implicit resolvers removed, string time values are emitted
    unquoted (for example ``18:00:00``) rather than being force-quoted, matching the
    representation the Fabric portal writes.
    """


_install_sexagesimal_safe_resolvers(SafeYamlLoader)
_install_sexagesimal_safe_resolvers(SafeYamlDumper)


def load_yaml(stream: Union[str, bytes, any], loader_cls: type = SafeYamlLoader) -> any:
    """Load YAML content without YAML 1.1 sexagesimal corruption.

    Args:
        stream: YAML content as a string, bytes, or an open text stream.
        loader_cls: The loader class to use. Defaults to :class:`SafeYamlLoader`.
            A sexagesimal-safe subclass (for example one that also detects
            duplicate keys) may be supplied instead.

    Returns:
        The parsed Python object (``dict``, ``list``, scalar, or ``None``).
    """
    return yaml.load(stream, Loader=loader_cls)


def dump_yaml(
    data: any,
    *,
    default_flow_style: bool = False,
    allow_unicode: bool = True,
    sort_keys: bool = False,
) -> str:
    """Dump a Python object to YAML without corrupting or reordering values.

    Uses :class:`SafeYamlDumper` so string time values round-trip unquoted, and
    defaults to ``sort_keys=False`` so the original key order is preserved.

    Args:
        data: The Python object to serialize.
        default_flow_style: Passed through to ``yaml.dump``.
        allow_unicode: Passed through to ``yaml.dump``.
        sort_keys: Passed through to ``yaml.dump``; defaults to ``False``.

    Returns:
        The serialized YAML string.
    """
    return yaml.dump(
        data,
        Dumper=SafeYamlDumper,
        default_flow_style=default_flow_style,
        allow_unicode=allow_unicode,
        sort_keys=sort_keys,
    )
