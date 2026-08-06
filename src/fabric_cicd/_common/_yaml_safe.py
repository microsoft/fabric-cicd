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

# YAML 1.1 int/float implicit resolvers as used by PyYAML, with the sexagesimal
# (``(:[0-5]?[0-9])+``) branch removed so ``HH:MM:SS`` values stay strings.
_INT_RESOLVER = re.compile(
    r"""^(?:[-+]?0b[0-1_]+
    |[-+]?0[0-7_]+
    |[-+]?(?:0|[1-9][0-9_]*)
    |[-+]?0x[0-9a-fA-F_]+)$""",
    re.VERBOSE,
)
_FLOAT_RESOLVER = re.compile(
    r"""^(?:[-+]?(?:[0-9][0-9_]*)\.[0-9_]*(?:[eE][-+]?[0-9]+)?
    |\.[0-9_]+(?:[eE][-+]?[0-9]+)?
    |[-+]?[0-9][0-9_]*(?:[eE][-+]?[0-9]+)
    |[-+]?\.(?:inf|Inf|INF)
    |\.(?:nan|NaN|NAN))$""",
    re.VERBOSE,
)


def _install_sexagesimal_safe_resolvers(cls: type) -> None:
    """Replace the int/float implicit resolvers on ``cls`` with sexagesimal-free versions.

    Copies PyYAML's default implicit resolver table onto ``cls`` and swaps the
    ``int`` and ``float`` regexes for ones that omit the base-60 branch, leaving
    every other resolver (bool, null, timestamp, etc.) untouched.
    """
    replacements = {
        "tag:yaml.org,2002:int": _INT_RESOLVER,
        "tag:yaml.org,2002:float": _FLOAT_RESOLVER,
    }
    resolvers = {}
    for first_char, mappings in yaml.SafeLoader.yaml_implicit_resolvers.items():
        resolvers[first_char] = [(tag, replacements.get(tag, regexp)) for tag, regexp in mappings]
    cls.yaml_implicit_resolvers = resolvers


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
