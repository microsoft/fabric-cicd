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


def extract_workspace_settings(config: dict, environment: str) -> dict:
    """Extract workspace-specific settings from config for the given environment."""
    environment = environment.strip()
    core = config["core"]
    settings = {}

    # Extract workspace ID or name based on environment
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

    # Extract other settings
    if "repository_directory" in core:
        if isinstance(core["repository_directory"], dict):
            settings["repository_directory"] = core["repository_directory"][environment]
        else:
            settings["repository_directory"] = core["repository_directory"]

    if "item_types_in_scope" in core:
        if isinstance(core["item_types_in_scope"], dict):
            settings["item_types_in_scope"] = core["item_types_in_scope"][environment]
        else:
            settings["item_types_in_scope"] = core["item_types_in_scope"]

    if "parameter" in core:
        if isinstance(core["parameter"], dict):
            settings["parameter_file_path"] = core["parameter"][environment]
        else:
            settings["parameter_file_path"] = core["parameter"]

    return settings


def extract_publish_settings(config: dict, environment: str) -> dict:
    """Extract publish-specific settings from config for the given environment."""
    settings = {}

    if "publish" in config:
        publish_config = config["publish"]

        if "exclude_regex" in publish_config:
            if isinstance(publish_config["exclude_regex"], dict):
                settings["exclude_regex"] = publish_config["exclude_regex"][environment]
            else:
                settings["exclude_regex"] = publish_config["exclude_regex"]

        if "folder_exclude_regex" in publish_config:
            if isinstance(publish_config["folder_exclude_regex"], dict):
                settings["folder_exclude_regex"] = publish_config["folder_exclude_regex"][environment]
            else:
                settings["folder_exclude_regex"] = publish_config["folder_exclude_regex"]

        if "items_to_include" in publish_config:
            if isinstance(publish_config["items_to_include"], dict):
                settings["items_to_include"] = publish_config["items_to_include"][environment]
            else:
                settings["items_to_include"] = publish_config["items_to_include"]

        if "shortcut_exclude_regex" in publish_config:
            if isinstance(publish_config["shortcut_exclude_regex"], dict):
                settings["shortcut_exclude_regex"] = publish_config["shortcut_exclude_regex"][environment]
            else:
                settings["shortcut_exclude_regex"] = publish_config["shortcut_exclude_regex"]

        if "skip" in publish_config:
            if isinstance(publish_config["skip"], dict):
                settings["skip"] = publish_config["skip"].get(environment, False)
            else:
                settings["skip"] = publish_config["skip"]

    return settings


def extract_unpublish_settings(config: dict, environment: str) -> dict:
    """Extract unpublish-specific settings from config for the given environment."""
    settings = {}

    if "unpublish" in config:
        unpublish_config = config["unpublish"]

        if "exclude_regex" in unpublish_config:
            if isinstance(unpublish_config["exclude_regex"], dict):
                settings["exclude_regex"] = unpublish_config["exclude_regex"][environment]
            else:
                settings["exclude_regex"] = unpublish_config["exclude_regex"]

        if "items_to_include" in unpublish_config:
            if isinstance(unpublish_config["items_to_include"], dict):
                settings["items_to_include"] = unpublish_config["items_to_include"][environment]
            else:
                settings["items_to_include"] = unpublish_config["items_to_include"]

        if "shortcut_exclude_regex" in unpublish_config:
            if isinstance(unpublish_config["shortcut_exclude_regex"], dict):
                settings["shortcut_exclude_regex"] = unpublish_config["shortcut_exclude_regex"][environment]
            else:
                settings["shortcut_exclude_regex"] = unpublish_config["shortcut_exclude_regex"]

        if "skip" in unpublish_config:
            if isinstance(unpublish_config["skip"], dict):
                settings["skip"] = unpublish_config["skip"].get(environment, False)
            else:
                settings["skip"] = unpublish_config["skip"]

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
