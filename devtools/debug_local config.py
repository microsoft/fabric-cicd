# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# The following is intended for developers of fabric-cicd to debug locally against the github repo

import sys
from pathlib import Path

from azure.identity import ClientSecretCredential

root_directory = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_directory / "src"))

from fabric_cicd import change_log_level, deploy_with_config

# Uncomment to enable debug
# change_log_level()

# In this example, the config file sits within the root/sample/workspace directory
config_file = str(root_directory / "sample" / "workspace" / "config.yml")

deploy_with_config(config_file=config_file, environment="dev")
