# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Utilities for YAML-based deployment configuration."""

import logging
from typing import Optional

from fabric_cicd import constants
from fabric_cicd._common._config_validator import ConfigValidator

logger = logging.getLogger(__name__)


def load_config_file(config_file_path: str, environment: str, config_override: Optional[dict] = None) -> dict:
    """Load and validate YAML configuration file.

    Args:
        config_file_path: Path to the YAML config file
        environment: Target environment for deployment
        config_override: Optional dictionary to override specific configuration values

    Returns:
        Parsed and validated configuration dictionary
    """
    validator = ConfigValidator()
    return validator.validate_config_file(config_file_path, environment, config_override)


def get_config_value(config_section: dict, key: str, environment: str) -> str | list | bool | None:
    """Extract a value from config, handling both single and environment-specific formats.

    Args:
        config_section: The config section to extract from
        key: The key to extract
        environment: Target environment

    Returns:
        The extracted value, or None if key doesn't exist or environment not found in dict
    """
    if key not in config_section:
        return None

    value = config_section[key]

    if isinstance(value, dict):
        return value.get(environment)

    return value


def extract_workspace_settings(config: dict, environment: str) -> dict:
    """Extract workspace-specific settings from config for the given environment."""
    environment = environment.strip()
    core = config["core"]
    settings = {}

    # Workspace ID or name - required, validation ensures environment exists
    if "workspace_id" in core:
        if isinstance(core["workspace_id"], dict):
            settings["workspace_id"] = core["workspace_id"][environment]
        else:
            settings["workspace_id"] = core["workspace_id"]
        logger.info(f"Using workspace ID '{settings['workspace_id']}'")

    elif "workspace" in core:
        if isinstance(core["workspace"], dict):
            settings["workspace_name"] = core["workspace"][environment]
        else:
            settings["workspace_name"] = core["workspace"]
        logger.info(f"Using workspace '{settings['workspace_name']}'")

    # Repository directory - required, validation ensures environment exists
    if "repository_directory" in core:
        if isinstance(core["repository_directory"], dict):
            settings["repository_directory"] = core["repository_directory"][environment]
        else:
            settings["repository_directory"] = core["repository_directory"]

    # Optional settings - validation logs warning if environment not found
    item_types_in_scope = get_config_value(core, "item_types_in_scope", environment)
    if item_types_in_scope is not None:
        settings["item_types_in_scope"] = item_types_in_scope

    parameter_file_path = get_config_value(core, "parameter", environment)
    if parameter_file_path is not None:
        settings["parameter_file_path"] = parameter_file_path

    return settings


def extract_publish_settings(config: dict, environment: str) -> dict:
    """Extract publish-specific settings from config for the given environment."""
    settings = {}

    if "publish" not in config:
        return settings

    publish_config = config["publish"]

    # Optional settings - validation logs debug if environment not found
    exclude_regex = get_config_value(publish_config, "exclude_regex", environment)
    if exclude_regex is not None:
        settings["exclude_regex"] = exclude_regex

    folder_exclude_regex = get_config_value(publish_config, "folder_exclude_regex", environment)
    if folder_exclude_regex is not None:
        settings["folder_exclude_regex"] = folder_exclude_regex

    items_to_include = get_config_value(publish_config, "items_to_include", environment)
    if items_to_include is not None:
        settings["items_to_include"] = items_to_include

    shortcut_exclude_regex = get_config_value(publish_config, "shortcut_exclude_regex", environment)
    if shortcut_exclude_regex is not None:
        settings["shortcut_exclude_regex"] = shortcut_exclude_regex

    # Skip defaults to False if environment not found
    if "skip" in publish_config:
        skip_value = publish_config["skip"]
        if isinstance(skip_value, dict):
            settings["skip"] = skip_value.get(environment, False)
        else:
            settings["skip"] = skip_value

    return settings


def extract_unpublish_settings(config: dict, environment: str) -> dict:
    """Extract unpublish-specific settings from config for the given environment."""
    settings = {}

    if "unpublish" not in config:
        return settings

    unpublish_config = config["unpublish"]

    # Optional settings - validation logs debug if environment not found
    exclude_regex = get_config_value(unpublish_config, "exclude_regex", environment)
    if exclude_regex is not None:
        settings["exclude_regex"] = exclude_regex

    items_to_include = get_config_value(unpublish_config, "items_to_include", environment)
    if items_to_include is not None:
        settings["items_to_include"] = items_to_include

    # Skip defaults to False if environment not found
    if "skip" in unpublish_config:
        skip_value = unpublish_config["skip"]
        if isinstance(skip_value, dict):
            settings["skip"] = unpublish_config["skip"].get(environment, False)
        else:
            settings["skip"] = skip_value

    return settings


def apply_config_overrides(config: dict, environment: str) -> None:
    """Apply feature flags and constants overrides from config.

    Args:
        config: Configuration dictionary
        environment: Target environment for deployment
    """
    if "features" in config:
        features = config["features"]
        features_list = features.get(environment, []) if isinstance(features, dict) else features

        for feature in features_list:
            constants.FEATURE_FLAG.add(feature)
            logger.info(f"Enabled feature flag: {feature}")

    if "constants" in config:
        constants_section = config["constants"]
        # Check if it's an environment mapping (all values are dicts)
        if all(isinstance(v, dict) for v in constants_section.values()):
            constants_dict = constants_section.get(environment, {})
        else:
            constants_dict = constants_section

        for key, value in constants_dict.items():
            if hasattr(constants, key):
                setattr(constants, key, value)
                logger.warning(f"Override constant {key} = {value}")
