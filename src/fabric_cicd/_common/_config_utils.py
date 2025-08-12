# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Configuration utilities for YAML-based deployment configuration."""

import logging
from pathlib import Path

import yaml

from fabric_cicd import constants
from fabric_cicd._common._exceptions import InputError

logger = logging.getLogger(__name__)


def load_config_file(config_file: str) -> dict:
    """Load and validate YAML configuration file."""
    config_path = Path(config_file)
    if not config_path.exists():
        error_msg = f"Configuration file not found: {config_file}"
        raise FileNotFoundError(error_msg)

    try:
        with config_path.open(encoding="utf-8") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        error_msg = f"Invalid YAML syntax in configuration file: {e}"
        raise InputError(error_msg, logger) from e

    if not isinstance(config, dict):
        error_msg = "Configuration file must contain a YAML dictionary"
        raise InputError(error_msg, logger)

    # Validate required sections
    if "core" not in config:
        error_msg = "Configuration file must contain a 'core' section"
        raise InputError(error_msg, logger)

    return config


def extract_workspace_settings(config: dict, environment: str) -> dict:
    """Extract workspace-specific settings from config for the given environment."""
    core = config["core"]
    settings = {}

    # Extract workspace ID or name based on environment
    if "workspace_id" in core:
        if isinstance(core["workspace_id"], dict):
            if environment not in core["workspace_id"]:
                error_msg = f"Environment '{environment}' not found in workspace_id mappings"
                raise InputError(error_msg, logger)
            settings["workspace_id"] = core["workspace_id"][environment]
        else:
            settings["workspace_id"] = core["workspace_id"]

    if "workspace" in core:
        if isinstance(core["workspace"], dict):
            if environment not in core["workspace"]:
                error_msg = f"Environment '{environment}' not found in workspace mappings"
                raise InputError(error_msg, logger)
            settings["workspace_name"] = core["workspace"][environment]
        else:
            settings["workspace_name"] = core["workspace"]

    # Validate that either workspace_id or workspace_name is provided
    if "workspace_id" not in settings and "workspace_name" not in settings:
        error_msg = "Configuration must specify either 'workspace_id' or 'workspace' in core section"
        raise InputError(error_msg, logger)

    # Extract other required settings
    if "repository_directory" not in core:
        error_msg = "Configuration must specify 'repository_directory' in core section"
        raise InputError(error_msg, logger)
    settings["repository_directory"] = core["repository_directory"]

    # Optional settings
    if "item_types_in_scope" in core:
        settings["item_types_in_scope"] = core["item_types_in_scope"]

    return settings


def extract_publish_settings(config: dict, environment: str) -> dict:
    """Extract publish-specific settings from config for the given environment."""
    settings = {}

    if "publish" in config:
        publish_config = config["publish"]

        # Extract exclude regex
        if "exclude_regex" in publish_config:
            settings["exclude_regex"] = publish_config["exclude_regex"]

        # Extract items to include
        if "items_to_include" in publish_config:
            settings["items_to_include"] = publish_config["items_to_include"]

        # Extract environment-specific skip setting
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

        # Extract exclude regex
        if "exclude_regex" in unpublish_config:
            settings["exclude_regex"] = unpublish_config["exclude_regex"]

        # Extract items to include
        if "items_to_include" in unpublish_config:
            settings["items_to_include"] = unpublish_config["items_to_include"]

        # Extract environment-specific skip setting
        if "skip" in unpublish_config:
            if isinstance(unpublish_config["skip"], dict):
                settings["skip"] = unpublish_config["skip"].get(environment, False)
            else:
                settings["skip"] = unpublish_config["skip"]

    return settings


def apply_config_overrides(config: dict) -> None:
    """Apply feature flags and constants overrides from config."""
    # Apply feature flags
    if "features" in config and isinstance(config["features"], list):
        for feature in config["features"]:
            if isinstance(feature, str):
                constants.FEATURE_FLAG.add(feature)
                logger.info(f"Enabled feature flag: {feature}")

    # Apply constants overrides
    if "constants" in config and isinstance(config["constants"], dict):
        for key, value in config["constants"].items():
            if hasattr(constants, key):
                setattr(constants, key, value)
                logger.info(f"Override constant {key} = {value}")
            else:
                logger.warning(f"Unknown constant '{key}' in configuration, ignoring")
