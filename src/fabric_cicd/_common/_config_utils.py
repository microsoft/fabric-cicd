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


def update_setting(
    settings: dict, config: dict, key: str, environment: str, default_value: str = None, output_key: str = None
) -> None:
    """
    Gets a config value using get_config_value and updates the settings dictionary
    if the value is not None.

    Args:
        settings: The settings dictionary to update
        config: The configuration dictionary
        key: The key to extract from the config
        environment: Target environment
        default_value: The default value to set if the config value is None
        output_key: The key to use in the settings dictionary (defaults to `key` if None)
    """
    value = get_config_value(config, key, environment)
    target_key = output_key or key
    if value is not None:
        settings[target_key] = value
    elif default_value is not None:
        settings[target_key] = default_value


def extract_workspace_settings(config: dict, environment: str) -> dict:
    """Extract workspace-specific settings from config for the given environment."""
    environment = environment.strip()
    core = config["core"]
    settings = {}

    # Workspace ID or name - required, validation ensures value exists for target environment
    if "workspace_id" in core:
        settings["workspace_id"] = get_config_value(core, "workspace_id", environment)
        logger.info(f"Using workspace ID '{settings['workspace_id']}'")
    elif "workspace" in core:
        settings["workspace_name"] = get_config_value(core, "workspace", environment)
        logger.info(f"Using workspace '{settings['workspace_name']}'")

    # Repository directory - required, validation ensures value exists for target environment
    if "repository_directory" in core:
        settings["repository_directory"] = get_config_value(core, "repository_directory", environment)

    # Optional settings - validation logs warning if value not found for target environment
    update_setting(settings, core, "item_types_in_scope", environment)
    update_setting(settings, core, "parameter", environment, output_key="parameter_file_path")

    return settings


def extract_publish_settings(config: dict, environment: str) -> dict:
    """Extract publish-specific settings from config for the given environment."""
    settings = {}

    if "publish" in config:
        publish_config = config["publish"]

        # Optional settings - validation logs debug if value not found for target environment
        settings_to_update = [
            "exclude_regex",
            "folder_exclude_regex",
            "items_to_include",
            "shortcut_exclude_regex",
        ]
        for key in settings_to_update:
            update_setting(settings, publish_config, key, environment)

        # Skip defaults to False if setting not found
        update_setting(settings, publish_config, "skip", environment, default_value=False)

    return settings


def extract_unpublish_settings(config: dict, environment: str) -> dict:
    """Extract unpublish-specific settings from config for the given environment."""
    settings = {}

    if "unpublish" in config:
        unpublish_config = config["unpublish"]

        # Optional settings - validation logs debug if value not found for target environment
        settings_to_update = [
            "exclude_regex",
            "items_to_include",
        ]
        for key in settings_to_update:
            update_setting(settings, unpublish_config, key, environment)

        # Skip defaults to False if setting not found
        update_setting(settings, unpublish_config, "skip", environment, default_value=False)

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
