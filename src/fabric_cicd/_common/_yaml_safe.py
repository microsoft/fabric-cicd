# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Centralized, sexagesimal-safe YAML loading and dumping.

PyYAML implements YAML 1.1, whose implicit resolvers parse unquoted ``HH:MM:SS``
values as base-60 (sexagesimal) integers (for example ``18:00:00`` -> ``64800``).
When file content is round-tripped through ``load`` / ``dump``, this silently
corrupts time values such as Environment ``live_pool`` schedules, which the Fabric
API then rejects as invalid TimeSpans (see issue #1072).

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


def _strip_sexagesimal_branch(pattern: str) -> str:
    """Remove only the base-60 alternation branch from a PyYAML numeric resolver pattern.

    PyYAML's int/float implicit resolvers are a top-level ``^(?: A | B | ... )$``
    alternation in which exactly the branches matching sexagesimal (``HH:MM:SS``)
    notation contain ``:[0-5]?[0-9]``. Dropping just those branches yields a resolver
    that is byte-for-byte identical to PyYAML's for every other input, so normal
    ints, floats, exponents, hex, octal, and binary literals resolve exactly as
    before while colon-separated time values fall through to being strings.
    """
    inner = pattern[len("^(?:") : -len(")$")]
    kept = [branch for branch in inner.split("|") if _SEXAGESIMAL_SUBPATTERN not in branch]
    return "^(?:" + "|".join(kept) + ")$"


def _sexagesimal_safe_resolvers() -> dict:
    """Build a copy of PyYAML's implicit resolver table with the base-60 branches removed."""
    compiled = {}
    for tag_suffix in _NUMERIC_TAGS:
        for _, mappings in yaml.SafeLoader.yaml_implicit_resolvers.items():
            match = next((rx for tag, rx in mappings if tag == tag_suffix), None)
            if match is not None:
                compiled[tag_suffix] = re.compile(_strip_sexagesimal_branch(match.pattern), re.VERBOSE)
                break

    resolvers = {}
    for first_char, mappings in yaml.SafeLoader.yaml_implicit_resolvers.items():
        resolvers[first_char] = [(tag, compiled.get(tag, regexp)) for tag, regexp in mappings]
    return resolvers


def _install_sexagesimal_safe_resolvers(cls: type) -> None:
    """Install the sexagesimal-free implicit resolver table on ``cls``.

    Derives the table from PyYAML's own default resolvers (see
    :func:`_strip_sexagesimal_branch`), leaving every non-numeric resolver
    (bool, null, timestamp, etc.) untouched.
    """
    cls.yaml_implicit_resolvers = _sexagesimal_safe_resolvers()


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
