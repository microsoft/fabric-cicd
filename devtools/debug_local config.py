# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# The following is intended for developers of fabric-cicd to debug locally against the github repo

import sys
from pathlib import Path

from azure.identity import ClientSecretCredential

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

from fabric_cicd import append_feature_flag, change_log_level, deploy_with_config

# Uncomment to enable debug
# change_log_level()

# feature flags required for this feature
append_feature_flag("enable_experimental_features")
append_feature_flag("enable_config_deploy")

# config_override_dict = {"core": {"item_types_in_scope": ["Notebook"]}, "publish": {"skip": {"dev": False}}}

# In this example, the config file sits within the root/sample/workspace directory
config_file = str(root_directory / "sample" / "workspace" / "config.yml")

deploy_with_config(
    config_file_path=config_file,
    # Comment out if environment is not needed
    environment="dev",
    # Uncomment to use SPN auth
    # token_credential=token_credential,
    # Uncomment to override specific config values (pass in a dictionary of override values)
    # config_override=config_override_dict
)
