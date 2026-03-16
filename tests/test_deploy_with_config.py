# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the config-based deployment functionality."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from fabric_cicd import DeploymentResult, DeploymentStatus, constants, deploy_with_config
from fabric_cicd._common._config_utils import (
    config_overrides_scope,
    extract_publish_settings,
    extract_unpublish_settings,
    extract_workspace_settings,
    load_config_file,
)
from fabric_cicd._common._config_validator import ConfigValidationError
from fabric_cicd._common._exceptions import InputError, PublishError


class TestConfigFileLoading:
    """Test config file loading and validation."""

    def test_load_valid_config_file(self, tmp_path):
        """Test loading a valid YAML config file."""
        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "12345678-1234-1234-1234-123456789abc"},
                "repository_directory": "test/path",
            }
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        result = load_config_file(str(config_file), "dev")
        # Verify the structure is correct
        assert result["core"]["workspace_id"] == config_data["core"]["workspace_id"]
        # Verify path was resolved to absolute path and exists
        resolved_path = Path(result["core"]["repository_directory"])
        assert resolved_path.is_absolute()
        assert resolved_path.exists()
        assert resolved_path.is_dir()

    def test_load_config_file_with_override(self, tmp_path):
        """Test loading a YAML config file with overrides."""
        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "12345678-1234-1234-1234-123456789abc"},
                "repository_directory": "test/path",
            }
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Define override values
        config_override = {
            "core": {"workspace_id": {"dev": "87654321-4321-4321-4321-123456789abc"}},
            "publish": {"skip": False, "exclude_regex": "^TEST.*"},
        }

        result = load_config_file(str(config_file), "dev", config_override)

        # Verify the overridden values
        assert result["core"]["workspace_id"]["dev"] == "87654321-4321-4321-4321-123456789abc"
        assert result["publish"]["skip"] == False
        assert result["publish"]["exclude_regex"] == "^TEST.*"

        # Verify path was still resolved to absolute path and exists
        resolved_path = Path(result["core"]["repository_directory"])
        assert resolved_path.is_absolute()
        assert resolved_path.exists()
        assert resolved_path.is_dir()

    def test_load_nonexistent_config_file(self):
        """Test loading a non-existent config file raises ConfigValidationError."""
        with pytest.raises(ConfigValidationError, match="Configuration file not found"):
            load_config_file("nonexistent.yml", "N/A")

    def test_load_invalid_yaml_syntax(self, tmp_path):
        """Test loading a file with invalid YAML syntax raises InputError."""
        config_file = tmp_path / "invalid.yml"
        config_file.write_text("invalid: yaml: content: [")

        with pytest.raises(InputError, match="Invalid YAML syntax"):
            load_config_file(str(config_file), "N/A")

    def test_load_non_dict_yaml(self, tmp_path):
        """Test loading a YAML file that doesn't contain a dictionary."""
        config_file = tmp_path / "list.yml"
        config_file.write_text("- item1\n- item2")

        with pytest.raises(ConfigValidationError, match="Configuration must be a dictionary"):
            load_config_file(str(config_file), "N/A")

    def test_load_config_missing_core_section(self, tmp_path):
        """Test loading a config file without required 'core' section."""
        config_data = {"publish": {"skip": {"dev": True}}}
        config_file = tmp_path / "no_core.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        with pytest.raises(ConfigValidationError, match="must contain a 'core' section"):
            load_config_file(str(config_file), "N/A")


class TestWorkspaceSettingsExtraction:
    """Test workspace settings extraction from config."""

    def test_extract_workspace_id_by_environment(self):
        """Test extracting workspace ID based on environment."""
        config = {
            "core": {
                "workspace_id": {
                    "dev": "11111111-1111-1111-1111-111111111111",
                    "prod": "22222222-2222-2222-2222-222222222222",
                },
                "repository_directory": "test/path",
            }
        }

        settings = extract_workspace_settings(config, "dev")
        assert settings["workspace_id"] == "11111111-1111-1111-1111-111111111111"
        assert settings["repository_directory"] == "test/path"

    def test_extract_workspace_name_by_environment(self):
        """Test extracting workspace name based on environment."""
        config = {
            "core": {
                "workspace": {"dev": "dev-workspace", "prod": "prod-workspace"},
                "repository_directory": "test/path",
            }
        }

        settings = extract_workspace_settings(config, "dev")
        assert settings["workspace_name"] == "dev-workspace"
        assert settings["repository_directory"] == "test/path"

    def test_extract_single_workspace_id(self, tmp_path):
        """Test config with single workspace ID (non-environment-specific)."""
        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": "33333333-3333-3333-3333-333333333333",
                "repository_directory": "test/path",
            }
        }
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump(config_data))

        # Single workspace IDs are supported
        config = load_config_file(str(config_file), "N/A")
        assert config["core"]["workspace_id"] == "33333333-3333-3333-3333-333333333333"

    def test_extract_missing_environment(self, tmp_path):
        """Test error when environment not found in workspace mappings during config loading."""
        config_data = {
            "core": {
                "workspace_id": {"dev": "44444444-4444-4444-4444-444444444444"},
                "repository_directory": "test/path",
            }
        }
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump(config_data))

        # Environment validation should happen during config loading, not extraction
        with pytest.raises(
            ConfigValidationError, match=r"Environment 'prod' not found in 'core.workspace_id' mappings"
        ):
            load_config_file(str(config_file), "prod")

    def test_extract_missing_workspace_config(self, tmp_path):
        """Test error when neither workspace_id nor workspace is provided."""
        config_data = {
            "core": {
                "repository_directory": "test/path",
            }
        }
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump(config_data))

        with pytest.raises(ConfigValidationError, match="must specify either 'workspace_id' or 'workspace'"):
            load_config_file(str(config_file), "N/A")

    def test_extract_missing_repository_directory(self, tmp_path):
        """Test error when repository_directory is missing."""
        config_data = {
            "core": {
                "workspace_id": {"dev": "55555555-5555-5555-5555-555555555555"},
            }
        }
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump(config_data))

        with pytest.raises(ConfigValidationError, match="must specify 'repository_directory'"):
            load_config_file(str(config_file), "N/A")

    def test_extract_optional_item_types(self):
        """Test extracting optional item_types_in_scope."""
        config = {
            "core": {
                "workspace_id": "66666666-6666-6666-6666-666666666666",
                "repository_directory": "test/path",
                "item_types_in_scope": ["Notebook", "DataPipeline"],
            }
        }

        settings = extract_workspace_settings(config, "dev")
        assert settings["item_types_in_scope"] == ["Notebook", "DataPipeline"]

    def test_extract_parameter_file_path_string(self):
        """Test extracting parameter file path as string."""
        config = {
            "core": {
                "workspace_id": "12345678-1234-1234-1234-123456789abc",
                "repository_directory": "test/path",
                "parameter": "parameter.yml",
            }
        }

        settings = extract_workspace_settings(config, "dev")
        assert settings["parameter_file_path"] == "parameter.yml"

    def test_extract_parameter_file_path_environment_mapping(self):
        """Test extracting parameter file path from environment mapping."""
        config = {
            "core": {
                "workspace_id": {
                    "dev": "11111111-1111-1111-1111-111111111111",
                    "prod": "22222222-2222-2222-2222-222222222222",
                },
                "repository_directory": "test/path",
                "parameter": {"dev": "dev-parameter.yml", "prod": "prod-parameter.yml"},
            }
        }

        settings = extract_workspace_settings(config, "dev")
        assert settings["parameter_file_path"] == "dev-parameter.yml"

        settings_prod = extract_workspace_settings(config, "prod")
        assert settings_prod["parameter_file_path"] == "prod-parameter.yml"

    def test_extract_parameter_file_path_missing(self):
        """Test extracting workspace settings when parameter field is missing."""
        config = {
            "core": {
                "workspace_id": "33333333-3333-3333-3333-333333333333",
                "repository_directory": "test/path",
            }
        }

        settings = extract_workspace_settings(config, "dev")
        assert "parameter_file_path" not in settings


class TestPublishSettingsExtraction:
    """Test publish settings extraction from config."""

    def testextract_publish_settings_with_skip(self):
        """Test extracting publish settings with environment-specific skip."""
        config = {
            "publish": {
                "exclude_regex": "^DONT_DEPLOY.*",
                "skip": {"dev": True, "prod": False},
            }
        }

        settings = extract_publish_settings(config, "dev")
        assert settings["exclude_regex"] == "^DONT_DEPLOY.*"
        assert settings["skip"] is True

        settings = extract_publish_settings(config, "prod")
        assert settings["skip"] is False

    def testextract_publish_settings_with_items_to_include(self):
        """Test extracting publish settings with items_to_include."""
        config = {
            "publish": {
                "items_to_include": ["item1.Notebook", "item2.DataPipeline"],
            }
        }

        settings = extract_publish_settings(config, "dev")
        assert settings["items_to_include"] == ["item1.Notebook", "item2.DataPipeline"]

    def testextract_publish_settings_no_config(self):
        """Test extracting publish settings when no publish config exists."""
        config = {}

        settings = extract_publish_settings(config, "dev")
        assert settings == {}

    def testextract_publish_settings_single_skip_value(self):
        """Test extracting publish settings with single skip value (not environment-specific)."""
        config = {
            "publish": {
                "skip": True,
            }
        }

        settings = extract_publish_settings(config, "dev")
        assert settings["skip"] is True

    def test_extract_publish_settings_with_shortcut_exclude_regex(self):
        """Test extracting publish settings with shortcut_exclude_regex."""
        config = {
            "publish": {
                "shortcut_exclude_regex": "^temp_.*",
            }
        }

        settings = extract_publish_settings(config, "dev")
        assert settings["shortcut_exclude_regex"] == "^temp_.*"

    def test_extract_publish_settings_with_environment_specific_shortcut_exclude_regex(self):
        """Test extracting publish settings with environment-specific shortcut_exclude_regex."""
        config = {
            "publish": {
                "shortcut_exclude_regex": {"dev": "^dev_temp_.*", "prod": "^staging_.*"},
            }
        }

        settings = extract_publish_settings(config, "dev")
        assert settings["shortcut_exclude_regex"] == "^dev_temp_.*"

        settings = extract_publish_settings(config, "prod")
        assert settings["shortcut_exclude_regex"] == "^staging_.*"


class TestUnpublishSettingsExtraction:
    """Test unpublish settings extraction from config."""

    def testextract_unpublish_settings_with_skip(self):
        """Test extracting unpublish settings with environment-specific skip."""
        config = {
            "unpublish": {
                "exclude_regex": "^DEBUG.*",
                "skip": {"dev": True, "prod": False},
            }
        }

        settings = extract_unpublish_settings(config, "dev")
        assert settings["exclude_regex"] == "^DEBUG.*"
        assert settings["skip"] is True

        settings = extract_unpublish_settings(config, "prod")
        assert settings["skip"] is False

    def testextract_unpublish_settings_no_config(self):
        """Test extracting unpublish settings when no unpublish config exists."""
        config = {}

        settings = extract_unpublish_settings(config, "dev")
        assert settings == {}


class TestConfigOverrides:
    """Test feature flags and constants overrides."""

    def test_feature_flags_applied_within_scope(self):
        """Test feature flags are active inside config_overrides_scope."""
        foo = "enable_foo_feature"
        bar = "enable_bar_feature"
        config = {"features": [foo, bar]}

        original_flags = constants.FEATURE_FLAG.copy()

        with config_overrides_scope(config, "N/A"):
            assert foo in constants.FEATURE_FLAG
            assert bar in constants.FEATURE_FLAG

        # Verify flags are restored after exiting scope
        assert original_flags == constants.FEATURE_FLAG

    def test_feature_flags_restored_after_scope(self):
        """Test feature flags set by config do not persist after scope exits."""
        config = {"features": ["enable_test_feature"]}

        original_flags = constants.FEATURE_FLAG.copy()

        with config_overrides_scope(config, "N/A"):
            assert "enable_test_feature" in constants.FEATURE_FLAG

        assert "enable_test_feature" not in constants.FEATURE_FLAG
        assert original_flags == constants.FEATURE_FLAG

    def test_constants_overrides_applied_within_scope(self):
        """Test constants overrides are active inside config_overrides_scope."""
        original_url = constants.DEFAULT_API_ROOT_URL
        config = {"constants": {"DEFAULT_API_ROOT_URL": "https://custom.api.com"}}

        with config_overrides_scope(config, "N/A"):
            assert constants.DEFAULT_API_ROOT_URL == "https://custom.api.com"

        # Verify constant is restored after exiting scope
        assert original_url == constants.DEFAULT_API_ROOT_URL

    def test_constants_overrides_restored_after_scope(self):
        """Test constants set by config do not persist after scope exits."""
        original_url = constants.DEFAULT_API_ROOT_URL
        config = {"constants": {"DEFAULT_API_ROOT_URL": "https://temporary.api.com"}}

        with config_overrides_scope(config, "N/A"):
            pass

        assert original_url == constants.DEFAULT_API_ROOT_URL

    def test_user_constants_preserved_across_scope(self):
        """Test that constants set by user before scope are preserved after scope exits."""
        original_url = constants.DEFAULT_API_ROOT_URL
        constants.DEFAULT_API_ROOT_URL = "https://user-set.api.com"

        config = {"constants": {"FABRIC_API_ROOT_URL": "https://config-set.fabric.com"}}
        original_fabric_url = constants.FABRIC_API_ROOT_URL

        with config_overrides_scope(config, "N/A"):
            assert constants.DEFAULT_API_ROOT_URL == "https://user-set.api.com"
            assert constants.FABRIC_API_ROOT_URL == "https://config-set.fabric.com"

        assert constants.DEFAULT_API_ROOT_URL == "https://user-set.api.com"
        assert original_fabric_url == constants.FABRIC_API_ROOT_URL

        # Clean up
        constants.DEFAULT_API_ROOT_URL = original_url

    def test_user_constant_same_key_restored_after_scope(self):
        """Test that when user sets a constant and config overrides the same key, user value is restored."""
        original_url = constants.DEFAULT_API_ROOT_URL
        constants.DEFAULT_API_ROOT_URL = "https://user-set.api.com"

        config = {"constants": {"DEFAULT_API_ROOT_URL": "https://config-override.api.com"}}

        with config_overrides_scope(config, "N/A"):
            assert constants.DEFAULT_API_ROOT_URL == "https://config-override.api.com"

        assert constants.DEFAULT_API_ROOT_URL == "https://user-set.api.com"

        # Clean up
        constants.DEFAULT_API_ROOT_URL = original_url

    def test_no_overrides_does_not_error(self):
        """Test config_overrides_scope works with empty config."""
        original_flags = constants.FEATURE_FLAG.copy()
        config = {}

        with config_overrides_scope(config, "N/A"):
            pass

        assert original_flags == constants.FEATURE_FLAG

    def test_overrides_restored_on_exception(self):
        """Test that overrides are restored even when an exception occurs inside the scope."""
        original_url = constants.DEFAULT_API_ROOT_URL
        original_flags = constants.FEATURE_FLAG.copy()
        config = {
            "features": ["enable_test_feature"],
            "constants": {"DEFAULT_API_ROOT_URL": "https://will-fail.api.com"},
        }

        msg = "deployment failed"
        with pytest.raises(RuntimeError, match=msg), config_overrides_scope(config, "N/A"):
            raise RuntimeError(msg)

        assert original_url == constants.DEFAULT_API_ROOT_URL
        assert original_flags == constants.FEATURE_FLAG

    def test_user_flags_preserved_across_scope(self):
        """Test that flags set by user before scope are preserved after scope exits."""
        original_flags = constants.FEATURE_FLAG.copy()
        constants.FEATURE_FLAG.add("user_set_flag")

        config = {"features": ["config_set_flag"]}

        with config_overrides_scope(config, "N/A"):
            assert "user_set_flag" in constants.FEATURE_FLAG
            assert "config_set_flag" in constants.FEATURE_FLAG

        assert "user_set_flag" in constants.FEATURE_FLAG
        assert "config_set_flag" not in constants.FEATURE_FLAG

        # Clean up
        constants.FEATURE_FLAG.clear()
        constants.FEATURE_FLAG.update(original_flags)

    def test_environment_specific_feature_flags(self):
        """Test environment-specific feature flags are resolved correctly."""
        original_flags = constants.FEATURE_FLAG.copy()
        config = {
            "features": {
                "dev": ["enable_dev_feature"],
                "prod": ["enable_prod_feature"],
            }
        }

        with config_overrides_scope(config, "dev"):
            assert "enable_dev_feature" in constants.FEATURE_FLAG
            assert "enable_prod_feature" not in constants.FEATURE_FLAG

        assert original_flags == constants.FEATURE_FLAG

    def test_environment_specific_constants(self):
        """Test environment-specific constants overrides are resolved correctly."""
        original_url = constants.DEFAULT_API_ROOT_URL
        config = {
            "constants": {
                "DEFAULT_API_ROOT_URL": {
                    "dev": "https://dev.api.com",
                    "prod": "https://prod.api.com",
                }
            }
        }

        with config_overrides_scope(config, "dev"):
            assert constants.DEFAULT_API_ROOT_URL == "https://dev.api.com"

        assert original_url == constants.DEFAULT_API_ROOT_URL


class TestConfigOverridesIntegration:
    """Integration tests for config_overrides_scope with deploy_with_config."""

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_response_collection_via_config_features(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test that enable_response_collection set in config features enables response collection."""
        _ = mock_unpublish
        _ = mock_publish

        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
            "features": ["enable_response_collection"],
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace_instance = MagicMock()
        mock_workspace_instance.responses = {"Notebook": {"MyNotebook": {"body": {"id": "123"}}}}
        mock_workspace.return_value = mock_workspace_instance

        result = deploy_with_config(str(config_file), "dev")

        assert isinstance(result, DeploymentResult)
        assert result.responses == {"Notebook": {"MyNotebook": {"body": {"id": "123"}}}}

        # Verify feature flag was restored after scope exit
        assert "enable_response_collection" not in constants.FEATURE_FLAG

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_failure_with_partial_responses_via_config_features(
        self, mock_unpublish, mock_publish, mock_workspace, tmp_path
    ):
        """Test that partial responses are attached to exceptions when enable_response_collection is set via config."""
        _ = mock_publish

        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
            "features": ["enable_response_collection"],
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace_instance = MagicMock()
        mock_workspace_instance.responses = {"Notebook": {"MyNotebook": {"body": {"id": "123"}}}}
        mock_workspace.return_value = mock_workspace_instance

        mock_unpublish.side_effect = RuntimeError("Unpublish failed")

        with pytest.raises(RuntimeError) as exc_info:
            deploy_with_config(str(config_file), "dev")

        e = exc_info.value
        assert hasattr(e, "deployment_result")
        assert e.deployment_result.responses == {"Notebook": {"MyNotebook": {"body": {"id": "123"}}}}

        # Verify feature flag was restored after scope exit
        assert "enable_response_collection" not in constants.FEATURE_FLAG


class TestDeployWithConfig:
    """Test the main deploy_with_config function."""

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_full_deployment(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test full deployment with config file."""
        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        # Create test config file
        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
                "item_types_in_scope": ["Notebook", "DataPipeline"],
            },
            "publish": {
                "exclude_regex": "^DONT_DEPLOY.*",
                "skip": {"dev": False},
            },
            "unpublish": {
                "exclude_regex": "^DEBUG.*",
                "skip": {"dev": False},
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Mock workspace instance
        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance

        # Execute deployment
        deploy_with_config(str(config_file), "dev")

        # Verify workspace creation
        # Note: repository_directory will be resolved to absolute path during validation
        call_args = mock_workspace.call_args[1]
        assert call_args["workspace_id"] == "77777777-7777-7777-7777-777777777777"
        assert call_args["workspace_name"] is None
        assert "test" in call_args["repository_directory"]  # Path will be resolved to absolute
        assert "path" in call_args["repository_directory"]
        assert call_args["item_type_in_scope"] == ["Notebook", "DataPipeline"]
        assert call_args["environment"] == "dev"
        assert call_args["token_credential"] is None

        # Verify publish and unpublish calls
        mock_publish.assert_called_once_with(
            mock_workspace_instance,
            item_name_exclude_regex="^DONT_DEPLOY.*",
            folder_path_exclude_regex=None,
            folder_path_to_include=None,
            items_to_include=None,
            shortcut_exclude_regex=None,
        )
        mock_unpublish.assert_called_once_with(
            mock_workspace_instance,
            item_name_exclude_regex="^DEBUG.*",
            items_to_include=None,
        )

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_skip_operations(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test deployment with skip flags enabled."""
        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        # Create test config file with skip flags
        config_data = {
            "core": {
                "workspace_id": {"dev": "88888888-8888-8888-8888-888888888888"},
                "repository_directory": "test/path",
            },
            "publish": {
                "skip": {"dev": True},
            },
            "unpublish": {
                "skip": {"dev": True},
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Mock workspace instance
        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance

        # Execute deployment
        deploy_with_config(str(config_file), "dev")

        # Verify workspace creation
        mock_workspace.assert_called_once()

        # Verify that publish and unpublish are NOT called due to skip flags
        mock_publish.assert_not_called()
        mock_unpublish.assert_not_called()

    def test_deploy_with_config_missing_file(self):
        """Test deployment with missing config file."""
        with pytest.raises(ConfigValidationError, match="Configuration file not found"):
            deploy_with_config("nonexistent.yml", "dev")

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_with_token_credential(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test deployment with custom token credential."""
        # Mark unused mocks to avoid linting warnings
        _ = mock_unpublish
        _ = mock_publish

        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        # Create test config file
        config_data = {
            "core": {
                "workspace_id": {"dev": "99999999-9999-9999-9999-999999999999"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Mock workspace instance and token credential
        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance
        mock_credential = MagicMock()

        # Execute deployment
        deploy_with_config(str(config_file), "dev", token_credential=mock_credential)

        # Verify workspace creation with token credential
        # Note: repository_directory will be resolved to absolute path during validation
        call_args = mock_workspace.call_args[1]
        assert call_args["workspace_id"] == "99999999-9999-9999-9999-999999999999"
        assert call_args["workspace_name"] is None
        assert "test" in call_args["repository_directory"]  # Path will be resolved to absolute
        assert "path" in call_args["repository_directory"]
        assert call_args["item_type_in_scope"] is None
        assert call_args["environment"] == "dev"
        assert call_args["token_credential"] == mock_credential

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_with_config_override(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test deployment with config override."""
        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        # Create test config file with default publish.skip = True to skip publishing
        config_data = {
            "core": {
                "workspace_id": {"dev": "12345678-1234-1234-1234-123456789abc"},
                "repository_directory": "test/path",
            },
            "publish": {
                "skip": {"dev": True},
            },
            "unpublish": {
                "skip": {"dev": True},
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Define config override to override the skip flags
        config_override = {
            "publish": {"skip": {"dev": False}},  # Override to NOT skip publish
            "unpublish": {"skip": {"dev": False}},  # Override to NOT skip unpublish
        }

        # Mock workspace instance
        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance

        # Execute deployment with config override
        deploy_with_config(str(config_file), "dev", config_override=config_override)

        # Verify workspace creation
        mock_workspace.assert_called_once()

        # Verify that publish and unpublish ARE called because the override turns off the skip flags
        mock_publish.assert_called_once()
        mock_unpublish.assert_called_once()

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_shortcut_exclude_regex(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test deployment with shortcut_exclude_regex in config."""
        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        # Create test config file with shortcut_exclude_regex
        config_data = {
            "core": {
                "workspace_id": "12345678-1234-1234-1234-123456789abc",
                "repository_directory": "test/path",
                "item_types_in_scope": ["Lakehouse"],
            },
            "publish": {
                "shortcut_exclude_regex": "^temp_.*",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Mock workspace instance
        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance

        # Execute deployment
        deploy_with_config(str(config_file), "dev")

        # Verify publish was called with shortcut_exclude_regex parameter
        mock_publish.assert_called_once_with(
            mock_workspace_instance,
            item_name_exclude_regex=None,
            folder_path_exclude_regex=None,
            folder_path_to_include=None,
            items_to_include=None,
            shortcut_exclude_regex="^temp_.*",
        )
        # Verify unpublish was also called (but without shortcut_exclude_regex since it's publish-only)
        mock_unpublish.assert_called_once()

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_folder_path_to_include_passed_to_publish(self, _mock_unpublish, mock_publish, mock_workspace, tmp_path):  # noqa: PT019
        """Test that folder_path_to_include from config is passed to publish_all_items."""
        test_repo_dir = tmp_path / "repo"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": "11111111-1111-1111-1111-111111111111",
                "repository_directory": str(test_repo_dir),
            },
            "publish": {
                "folder_path_to_include": ["/my/folder/path"],
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace.return_value = MagicMock()
        deploy_with_config(str(config_file), "dev")

        call_args = mock_publish.call_args[1]
        assert call_args["folder_path_to_include"] == ["/my/folder/path"]

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_folder_path_to_include_defaults_to_none(self, _mock_unpublish, mock_publish, mock_workspace, tmp_path):  # noqa: PT019
        """Test that folder_path_to_include defaults to None when not specified."""
        test_repo_dir = tmp_path / "repo"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": "11111111-1111-1111-1111-111111111111",
                "repository_directory": str(test_repo_dir),
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace.return_value = MagicMock()
        deploy_with_config(str(config_file), "dev")

        call_args = mock_publish.call_args[1]
        assert call_args["folder_path_to_include"] is None

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_folder_path_to_include_environment_specific(self, _mock_unpublish, mock_publish, mock_workspace, tmp_path):  # noqa: PT019
        """Test that folder_path_to_include resolves environment-specific values."""
        test_repo_dir = tmp_path / "repo"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {
                    "dev": "11111111-1111-1111-1111-111111111111",
                    "prod": "22222222-2222-2222-2222-222222222222",
                },
                "repository_directory": str(test_repo_dir),
            },
            "publish": {
                "folder_path_to_include": {"dev": ["/dev/folder"], "prod": ["/prod/folder"]},
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace.return_value = MagicMock()
        deploy_with_config(str(config_file), "dev")

        call_args = mock_publish.call_args[1]
        assert call_args["folder_path_to_include"] == ["/dev/folder"]


class TestConfigIntegration:
    """Integration tests for config functionality."""

    def test_sample_config_file_structure(self):
        """Test that the sample config file can be loaded and parsed correctly."""
        # Test with the actual sample config file
        sample_config_path = Path(__file__).parent.parent / "sample" / "workspace" / "config.yml"

        if sample_config_path.exists():
            # The sample config file might have directory references that don't exist in the test environment
            # So we just verify it can be parsed as valid YAML
            import yaml

            with sample_config_path.open(encoding="utf-8") as f:
                config = yaml.safe_load(f)

            # Verify basic structure
            assert "core" in config

            # If we have a valid environment, test basic functionality
            if (
                "core" in config
                and "workspace_id" in config["core"]
                and isinstance(config["core"]["workspace_id"], dict)
            ):
                test_env = next(iter(config["core"]["workspace_id"].keys()))

                # Only test the config extraction without path validation
                workspace_settings = extract_workspace_settings(config, test_env)
                assert "repository_directory" in workspace_settings

                extract_publish_settings(config, test_env)
                extract_unpublish_settings(config, test_env)
                with config_overrides_scope(config, test_env):
                    pass

    def test_config_validation_comprehensive(self, tmp_path):
        """Test comprehensive config validation with all sections."""
        # Create the actual directory structure that the config references
        sample_workspace_dir = tmp_path / "sample" / "workspace"
        sample_workspace_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {
                    "dev": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "test": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "prod": "cccccccc-cccc-cccc-cccc-cccccccccccc",
                },
                "repository_directory": "sample/workspace",
                "item_types_in_scope": ["Environment", "Notebook", "DataPipeline"],
            },
            "publish": {
                "exclude_regex": "^DONT_DEPLOY.*",
                "items_to_include": ["item1.Notebook"],
                "skip": {"dev": True, "test": False, "prod": False},
            },
            "unpublish": {"exclude_regex": "^DEBUG.*", "skip": {"dev": True, "test": False, "prod": False}},
            "features": ["enable_shortcut_publish"],
            "constants": {"DEFAULT_API_ROOT_URL": "https://msitapi.fabric.microsoft.com"},
        }

        config_file = tmp_path / "comprehensive_config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Test loading and parsing
        config = load_config_file(str(config_file), "dev")

        # Config validation may modify the config (e.g., resolve paths)
        # So we test the important parts separately
        assert "core" in config
        assert config["core"]["workspace_id"] == config_data["core"]["workspace_id"]
        assert "Notebook" in config["core"]["item_types_in_scope"]
        assert "publish" in config
        assert config["publish"]["exclude_regex"] == config_data["publish"]["exclude_regex"]

        # Test all environment extractions
        for env in ["dev", "test", "prod"]:
            workspace_settings = extract_workspace_settings(config, env)
            assert workspace_settings["workspace_id"] == config_data["core"]["workspace_id"][env]

            publish_settings = extract_publish_settings(config, env)
            assert publish_settings["skip"] == config_data["publish"]["skip"][env]

            unpublish_settings = extract_unpublish_settings(config, env)
            assert unpublish_settings["skip"] == config_data["unpublish"]["skip"][env]


class TestConfigUtilsExtractSettings:
    """Test config utility functions for extracting settings."""

    def test_extract_publish_settings_with_folder_exclude_regex(self):
        """Test extracting publish settings with folder_exclude_regex."""
        config = {
            "publish": {
                "folder_exclude_regex": "^/DONT_DEPLOY_FOLDER",
            }
        }

        settings = extract_publish_settings(config, "dev")
        assert settings["folder_exclude_regex"] == "^/DONT_DEPLOY_FOLDER"

    def test_extract_publish_settings_with_environment_specific_folder_exclude_regex(self):
        """Test extracting publish settings with environment-specific folder_exclude_regex."""
        config = {
            "publish": {
                "folder_exclude_regex": {"dev": "^/DEV_FOLDER", "prod": "^/PROD_FOLDER"},
            }
        }

        settings = extract_publish_settings(config, "dev")
        assert settings["folder_exclude_regex"] == "^/DEV_FOLDER"

        settings = extract_publish_settings(config, "prod")
        assert settings["folder_exclude_regex"] == "^/PROD_FOLDER"

    def test_extract_publish_settings_missing_environment_skips_setting(self):
        """Test that missing environment in optional publish settings skips the setting."""
        config = {
            "publish": {
                "exclude_regex": {"dev": "^DEV.*"},  # Only dev defined
                "folder_exclude_regex": {"dev": "^/DEV_FOLDER"},  # Only dev defined
            }
        }

        # prod environment not defined - settings should be skipped
        settings = extract_publish_settings(config, "prod")
        assert "exclude_regex" not in settings
        assert "folder_exclude_regex" not in settings

    def test_extract_unpublish_settings_missing_environment_skips_setting(self):
        """Test that missing environment in optional unpublish settings skips the setting."""
        config = {
            "unpublish": {
                "exclude_regex": {"dev": "^DEV.*"},  # Only dev defined
                "items_to_include": {"dev": ["item1"]},  # Only dev defined
            }
        }

        # prod environment not defined - settings should be skipped
        settings = extract_unpublish_settings(config, "prod")
        assert "exclude_regex" not in settings
        assert "items_to_include" not in settings

    def test_extract_publish_settings_skip_defaults_false_when_env_missing(self):
        """Test that skip defaults to False when environment is not in skip mapping."""
        config = {
            "publish": {
                "skip": {"dev": True},  # Only dev defined
            }
        }

        # prod environment not defined - skip should default to False
        settings = extract_publish_settings(config, "prod")
        assert settings["skip"] is False

    def test_extract_unpublish_settings_skip_defaults_false_when_env_missing(self):
        """Test that skip defaults to False when environment is not in skip mapping."""
        config = {
            "unpublish": {
                "skip": {"dev": True},  # Only dev defined
            }
        }

        # prod environment not defined - skip should default to False
        settings = extract_unpublish_settings(config, "prod")
        assert settings["skip"] is False

    def test_extract_workspace_settings_optional_fields_missing_environment(self):
        """Test that optional workspace fields are skipped when environment is missing."""
        config = {
            "core": {
                "workspace_id": "12345678-1234-1234-1234-123456789abc",  # Simple value
                "repository_directory": "/path/to/repo",
                "item_types_in_scope": {"dev": ["Notebook"]},  # Only dev defined
                "parameter": {"dev": "dev-param.yml"},  # Only dev defined
            }
        }

        # prod environment not defined for optional fields - they should be skipped
        settings = extract_workspace_settings(config, "prod")
        assert "item_types_in_scope" not in settings
        assert "parameter_file_path" not in settings
        # Required fields should still be present
        assert settings["workspace_id"] == "12345678-1234-1234-1234-123456789abc"
        assert settings["repository_directory"] == "/path/to/repo"

    def test_extract_publish_settings_shortcut_exclude_regex_missing_environment(self):
        """Test that shortcut_exclude_regex is skipped when environment is missing."""
        config = {
            "publish": {
                "shortcut_exclude_regex": {"dev": "^dev_temp_.*"},  # Only dev defined
            }
        }

        # prod environment not defined - setting should be skipped
        settings = extract_publish_settings(config, "prod")
        assert "shortcut_exclude_regex" not in settings

    def test_extract_publish_settings_items_to_include_missing_environment(self):
        """Test that items_to_include is skipped when environment is missing."""
        config = {
            "publish": {
                "items_to_include": {"dev": ["item1.Notebook", "item2.DataPipeline"]},  # Only dev defined
            }
        }

        # prod environment not defined - setting should be skipped
        settings = extract_publish_settings(config, "prod")
        assert "items_to_include" not in settings

    def test_extract_publish_settings_folder_path_to_include_list(self):
        """Test extract_publish_settings returns folder_path_to_include as a list."""
        config = {
            "publish": {
                "folder_path_to_include": ["/my/folder/path"],
            },
        }
        result = extract_publish_settings(config, "dev")
        assert result["folder_path_to_include"] == ["/my/folder/path"]

    def test_extract_publish_settings_folder_path_to_include_env_specific(self):
        """Test extract_publish_settings resolves folder_path_to_include per environment."""
        config = {
            "publish": {
                "folder_path_to_include": {"dev": ["/dev/folder"], "prod": ["/prod/folder"]},
            },
        }
        result = extract_publish_settings(config, "dev")
        assert result["folder_path_to_include"] == ["/dev/folder"]

    def test_extract_publish_settings_folder_path_to_include_missing(self):
        """Test extract_publish_settings defaults folder_path_to_include to None."""
        config = {
            "publish": {
                "exclude_regex": "^SKIP.*",
            },
        }
        settings = extract_publish_settings(config, "dev")
        assert "folder_path_to_include" not in settings

    def test_extract_publish_settings_no_publish_section_folder_path_to_include(self):
        """Test extract_publish_settings defaults folder_path_to_include to None when no publish section."""
        config = {}
        settings = extract_publish_settings(config, "dev")
        assert "folder_path_to_include" not in settings


class TestGetConfigValue:
    """Test the get_config_value utility function."""

    def test_get_config_value_key_not_present(self):
        """Test get_config_value when key doesn't exist."""
        from fabric_cicd._common._config_utils import get_config_value

        config = {"other_key": "value"}
        result = get_config_value(config, "missing_key", "dev")
        assert result is None

    def test_get_config_value_simple_value(self):
        """Test get_config_value with simple (non-dict) value."""
        from fabric_cicd._common._config_utils import get_config_value

        config = {"key": "simple_value"}
        result = get_config_value(config, "key", "dev")
        assert result == "simple_value"

    def test_get_config_value_dict_with_environment(self):
        """Test get_config_value with dict containing target environment."""
        from fabric_cicd._common._config_utils import get_config_value

        config = {"key": {"dev": "dev_value", "prod": "prod_value"}}
        result = get_config_value(config, "key", "dev")
        assert result == "dev_value"

    def test_get_config_value_dict_missing_environment(self):
        """Test get_config_value with dict missing target environment."""
        from fabric_cicd._common._config_utils import get_config_value

        config = {"key": {"dev": "dev_value"}}
        result = get_config_value(config, "key", "prod")
        assert result is None

    def test_get_config_value_list_value(self):
        """Test get_config_value with list value."""
        from fabric_cicd._common._config_utils import get_config_value

        config = {"key": ["item1", "item2"]}
        result = get_config_value(config, "key", "dev")
        assert result == ["item1", "item2"]

    def test_get_config_value_bool_value(self):
        """Test get_config_value with boolean value."""
        from fabric_cicd._common._config_utils import get_config_value

        config = {"key": True}
        result = get_config_value(config, "key", "dev")
        assert result is True


class TestDeploymentResult:
    """Test DeploymentResult and DeploymentStatus types."""

    def test_deployment_status_completed_value(self):
        """Test DeploymentStatus.COMPLETED has expected value."""
        assert DeploymentStatus.COMPLETED.value == "completed"

    def test_deployment_status_failed_value(self):
        """Test DeploymentStatus.FAILED has expected value."""
        assert DeploymentStatus.FAILED.value == "failed"

    def test_deployment_status_string_comparison(self):
        """Test DeploymentStatus supports string comparison via str inheritance."""
        assert DeploymentStatus.COMPLETED == "completed"
        assert DeploymentStatus.FAILED == "failed"

    def test_deployment_result_structure(self):
        """Test DeploymentResult fields are set correctly."""
        result = DeploymentResult(
            status=DeploymentStatus.COMPLETED,
            message="Test message",
        )
        assert result.status == DeploymentStatus.COMPLETED
        assert result.message == "Test message"

    def test_deployment_result_responses_defaults_to_none(self):
        """Test DeploymentResult.responses defaults to None when not provided."""
        result = DeploymentResult(
            status=DeploymentStatus.COMPLETED,
            message="Test message",
        )
        assert result.responses is None

    def test_deployment_result_with_responses(self):
        """Test DeploymentResult stores responses when provided."""
        responses = {"Notebook": {"MyNotebook": {"body": {"id": "123"}}}}
        result = DeploymentResult(
            status=DeploymentStatus.FAILED,
            message="Deployment failed",
            responses=responses,
        )
        assert result.status == DeploymentStatus.FAILED
        assert result.message == "Deployment failed"
        assert result.responses == responses


class TestDeployWithConfigReturnValue:
    """Test deploy_with_config returns DeploymentResult."""

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    @patch("fabric_cicd.constants.FEATURE_FLAG", set(["enable_experimental_features", "enable_config_deploy"]))
    def test_deploy_with_config_returns_deployment_result(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test that deploy_with_config returns a DeploymentResult on success."""
        # Mark unused mocks to avoid linting warnings
        _ = mock_unpublish
        _ = mock_publish

        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        # Create test config file
        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Mock workspace instance
        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance

        # Execute deployment
        result = deploy_with_config(str(config_file), "dev")

        # Verify result is a DeploymentResult
        assert isinstance(result, DeploymentResult)
        assert result.status == DeploymentStatus.COMPLETED
        assert "completed successfully" in result.message

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    @patch("fabric_cicd.constants.FEATURE_FLAG", set(["enable_experimental_features", "enable_config_deploy"]))
    def test_deploy_with_config_returns_completed_when_skipping_operations(
        self, mock_unpublish, mock_publish, mock_workspace, tmp_path
    ):
        """Test that deploy_with_config returns COMPLETED status even when skipping operations."""
        # Create the actual directory structure that the config references
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        # Create test config file with skip flags
        config_data = {
            "core": {
                "workspace_id": {"dev": "88888888-8888-8888-8888-888888888888"},
                "repository_directory": "test/path",
            },
            "publish": {
                "skip": {"dev": True},
            },
            "unpublish": {
                "skip": {"dev": True},
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Mock workspace instance
        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance

        # Execute deployment
        result = deploy_with_config(str(config_file), "dev")

        # Verify result is a DeploymentResult with COMPLETED status
        assert isinstance(result, DeploymentResult)
        assert result.status == DeploymentStatus.COMPLETED

        # Verify that publish and unpublish are NOT called due to skip flags
        mock_publish.assert_not_called()
        mock_unpublish.assert_not_called()


class TestDeployWithConfigFailures:
    """Test deploy_with_config raises exceptions properly on failure."""

    def test_deploy_with_config_invalid_yaml_raises_input_error(self, tmp_path):
        """Test that deploy_with_config raises InputError for invalid YAML syntax."""
        config_file = tmp_path / "invalid.yml"
        config_file.write_text("invalid: yaml: content: [")

        with pytest.raises(InputError, match="Invalid YAML syntax"):
            deploy_with_config(str(config_file), "dev")

    def test_deploy_with_config_missing_core_raises_config_validation_error(self, tmp_path):
        """Test that deploy_with_config raises ConfigValidationError when core section is missing."""
        config_data = {"publish": {"skip": True}}
        config_file = tmp_path / "no_core.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        with pytest.raises(ConfigValidationError, match="must contain a 'core' section"):
            deploy_with_config(str(config_file), "dev")

    def test_deploy_with_config_missing_environment_raises_config_validation_error(self, tmp_path):
        """Test that deploy_with_config raises ConfigValidationError when environment is not in workspace mappings."""
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "12345678-1234-1234-1234-123456789abc"},
                "repository_directory": "test/path",
            }
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        with pytest.raises(ConfigValidationError, match="Environment 'prod' not found"):
            deploy_with_config(str(config_file), "prod")

    def test_deploy_with_config_missing_workspace_id_raises_config_validation_error(self, tmp_path):
        """Test that deploy_with_config raises ConfigValidationError when workspace_id is missing."""
        config_data = {
            "core": {
                "repository_directory": "test/path",
            }
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        with pytest.raises(ConfigValidationError, match="must specify either 'workspace_id' or 'workspace'"):
            deploy_with_config(str(config_file), "dev")

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_publish_error_propagates(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test that PublishError from publish_all_items propagates through deploy_with_config."""
        _ = mock_unpublish

        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace.return_value = MagicMock()
        mock_publish.side_effect = PublishError(
            errors=[("FailedNotebook", RuntimeError("API call failed"))],
            logger=MagicMock(),
        )

        with pytest.raises(PublishError, match="Failed to publish 1 item"):
            deploy_with_config(str(config_file), "dev")

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_workspace_creation_error_propagates(
        self, mock_unpublish, mock_publish, mock_workspace, tmp_path
    ):
        """Test that exceptions from FabricWorkspace constructor propagate through deploy_with_config."""
        _ = mock_unpublish
        _ = mock_publish

        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace.side_effect = Exception("Workspace initialization failed")

        with pytest.raises(Exception, match="Workspace initialization failed"):
            deploy_with_config(str(config_file), "dev")

        mock_publish.assert_not_called()
        mock_unpublish.assert_not_called()

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_unpublish_error_propagates(
        self, mock_unpublish, mock_publish, mock_workspace, tmp_path
    ):
        """Test that exceptions from unpublish_all_orphan_items propagate and publish was already called."""
        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace.return_value = MagicMock()
        mock_unpublish.side_effect = RuntimeError("Unpublish operation failed")

        with pytest.raises(RuntimeError, match="Unpublish operation failed"):
            deploy_with_config(str(config_file), "dev")

        # Verify publish was called successfully before unpublish failed
        mock_publish.assert_called_once()


class TestDeployWithConfigExceptionAttributes:
    """Test that deploy_with_config attaches deployment attributes to exceptions."""

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_exception_has_deployment_status_and_message(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test that raised exceptions have deployment_status and deployment_message attributes."""
        _ = mock_unpublish

        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace.return_value = MagicMock()
        mock_publish.side_effect = RuntimeError("Something broke")

        with pytest.raises(RuntimeError) as exc_info:
            deploy_with_config(str(config_file), "dev")

        e = exc_info.value
        assert hasattr(e, "deployment_result")
        assert e.deployment_result.status == DeploymentStatus.FAILED
        assert "Something broke" in e.deployment_result.message

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    @patch(
        "fabric_cicd.constants.FEATURE_FLAG",
        set(["enable_experimental_features", "enable_config_deploy", "enable_response_collection"]),
    )
    def test_exception_has_partial_responses_when_enabled(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test that partial responses are attached to exceptions when response collection is enabled."""
        _ = mock_publish

        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace_instance = MagicMock()
        mock_workspace_instance.responses = {"Notebook": {"MyNotebook": {"body": {"id": "123"}}}}
        mock_workspace.return_value = mock_workspace_instance

        mock_unpublish.side_effect = RuntimeError("Unpublish failed")

        with pytest.raises(RuntimeError) as exc_info:
            deploy_with_config(str(config_file), "dev")

        e = exc_info.value
        assert hasattr(e, "deployment_result")
        assert e.deployment_result.responses == {"Notebook": {"MyNotebook": {"body": {"id": "123"}}}}

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_exception_no_responses_when_flag_disabled(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test that responses are not attached to exceptions when response collection is disabled."""
        _ = mock_unpublish

        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace.return_value = MagicMock()
        mock_publish.side_effect = RuntimeError("Publish failed")

        with pytest.raises(RuntimeError) as exc_info:
            deploy_with_config(str(config_file), "dev")

        e = exc_info.value
        assert hasattr(e, "deployment_result")
        assert e.deployment_result.responses is None

    def test_pre_workspace_failure_has_deployment_attributes(self, tmp_path):
        """Test that failures before workspace creation still have deployment attributes."""
        config_file = tmp_path / "invalid.yml"
        config_file.write_text("invalid: yaml: content: [")

        with pytest.raises(InputError) as exc_info:
            deploy_with_config(str(config_file), "dev")

        e = exc_info.value
        assert hasattr(e, "deployment_result")
        assert e.deployment_result.status == DeploymentStatus.FAILED
        assert e.deployment_result.message is not None
        assert e.deployment_result.responses is None


class TestDeployWithConfigResponseCollection:
    """Test deploy_with_config response collection integration."""

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    @patch(
        "fabric_cicd.constants.FEATURE_FLAG",
        set(["enable_experimental_features", "enable_config_deploy", "enable_response_collection"]),
    )
    def test_result_responses_is_dict_when_enabled(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test that result.responses is a dict (not string) when response collection is enabled."""
        _ = mock_unpublish
        _ = mock_publish

        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace_instance = MagicMock()
        mock_workspace_instance.responses = {"Notebook": {"MyNotebook": {"body": {"id": "123"}}}}
        mock_workspace.return_value = mock_workspace_instance

        result = deploy_with_config(str(config_file), "dev")

        assert isinstance(result, DeploymentResult)
        assert isinstance(result.responses, dict)
        assert result.responses == {"Notebook": {"MyNotebook": {"body": {"id": "123"}}}}

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    @patch(
        "fabric_cicd.constants.FEATURE_FLAG",
        set(["enable_experimental_features", "enable_config_deploy"]),
    )
    def test_result_responses_is_none_when_disabled(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test that result.responses is None when response collection is not enabled."""
        _ = mock_unpublish
        _ = mock_publish

        test_repo_dir = tmp_path / "test" / "path"
        test_repo_dir.mkdir(parents=True)

        config_data = {
            "core": {
                "workspace_id": {"dev": "77777777-7777-7777-7777-777777777777"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with Path.open(config_file, "w") as f:
            yaml.dump(config_data, f)

        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance

        result = deploy_with_config(str(config_file), "dev")

        assert isinstance(result, DeploymentResult)
        assert result.responses is None


class TestCollectResponses:
    """Test the _collect_responses helper function."""

    def test_returns_none_when_responses_disabled(self):
        from fabric_cicd.publish import _collect_responses

        workspace = MagicMock()
        workspace.responses = {"Notebook": {"nb1": {"body": {}}}}
        assert _collect_responses(workspace, responses_enabled=False) is None

    def test_returns_none_when_workspace_is_none(self):
        from fabric_cicd.publish import _collect_responses

        assert _collect_responses(None, responses_enabled=True) is None

    def test_returns_none_when_responses_empty_dict(self):
        from fabric_cicd.publish import _collect_responses

        workspace = MagicMock()
        workspace.responses = {}
        assert _collect_responses(workspace, responses_enabled=True) is None

    def test_returns_responses_when_available(self):
        from fabric_cicd.publish import _collect_responses

        workspace = MagicMock()
        workspace.responses = {"Notebook": {"nb1": {"body": {"id": "123"}}}}
        result = _collect_responses(workspace, responses_enabled=True)
        assert result == {"Notebook": {"nb1": {"body": {"id": "123"}}}}

    def test_returns_none_when_responses_is_none(self):
        from fabric_cicd.publish import _collect_responses

        workspace = MagicMock()
        workspace.responses = None
        assert _collect_responses(workspace, responses_enabled=True) is None
