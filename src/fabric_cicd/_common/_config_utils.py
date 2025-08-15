# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Configuration utilities for YAML-based deployment configuration."""

import logging

from fabric_cicd import constants
from fabric_cicd._common._config_validator import ConfigValidator, validate_config_for_environment

logger = logging.getLogger(__name__)


def load_config_file(config_file: str) -> dict:
    """Load and validate YAML configuration file."""
    validator = ConfigValidator()
    return validator.validate_config_file(config_file)


def extract_workspace_settings(config: dict, environment: str) -> dict:
    """Extract workspace-specific settings from config for the given environment."""
    # Validate environment-specific requirements
    validate_config_for_environment(config, environment)

    environment = environment.strip()
    core = config["core"]
    settings = {}

    # Extract workspace ID or name based on environment
    if "workspace_id" in core:
        if isinstance(core["workspace_id"], dict):
            settings["workspace_id"] = core["workspace_id"][environment]
        else:
            settings["workspace_id"] = core["workspace_id"]

    if "workspace" in core:
        if isinstance(core["workspace"], dict):
            settings["workspace_name"] = core["workspace"][environment]
        else:
            settings["workspace_name"] = core["workspace"]

    # Extract other settings
    settings["repository_directory"] = core["repository_directory"]

    if "item_types_in_scope" in core:
        settings["item_types_in_scope"] = core["item_types_in_scope"]

    return settings


def extract_publish_settings(config: dict, environment: str) -> dict:
    """Extract publish-specific settings from config for the given environment."""
    settings = {}

    if "publish" in config:
        publish_config = config["publish"]

        if "exclude_regex" in publish_config:
            settings["exclude_regex"] = publish_config["exclude_regex"]

        if "items_to_include" in publish_config:
            settings["items_to_include"] = publish_config["items_to_include"]

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
            settings["exclude_regex"] = unpublish_config["exclude_regex"]

        if "items_to_include" in unpublish_config:
            settings["items_to_include"] = unpublish_config["items_to_include"]

        if "skip" in unpublish_config:
            if isinstance(unpublish_config["skip"], dict):
                settings["skip"] = unpublish_config["skip"].get(environment, False)
            else:
                settings["skip"] = unpublish_config["skip"]

    return settings


def apply_config_overrides(config: dict) -> None:
    """Apply feature flags and constants overrides from config."""
    if "features" in config and isinstance(config["features"], list):
        for feature in config["features"]:
            constants.FEATURE_FLAG.add(feature)
            logger.info(f"Enabled feature flag: {feature}")

    if "constants" in config and isinstance(config["constants"], dict):
        for key, value in config["constants"].items():
            if hasattr(constants, key):
                setattr(constants, key, value)
                logger.info(f"Override constant {key} = {value}")
