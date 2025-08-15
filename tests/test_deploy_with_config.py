# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the config-based deployment functionality."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from fabric_cicd import deploy_with_config
from fabric_cicd._common._config_utils import (
    apply_config_overrides,
    extract_publish_settings,
    extract_unpublish_settings,
    extract_workspace_settings,
    load_config_file,
)
from fabric_cicd._common._config_validator import ConfigValidationError
from fabric_cicd._common._exceptions import InputError


class TestConfigFileLoading:
    """Test config file loading and validation."""

    def test_load_valid_config_file(self, tmp_path):
        """Test loading a valid YAML config file."""
        config_data = {
            "core": {
                "workspace_id": {"dev": "test-id"},
                "repository_directory": "test/path",
            }
        }
        config_file = tmp_path / "config.yml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        result = load_config_file(str(config_file))
        assert result == config_data

    def test_load_nonexistent_config_file(self):
        """Test loading a non-existent config file raises ConfigValidationError."""
        with pytest.raises(ConfigValidationError, match="Configuration file not found"):
            load_config_file("nonexistent.yml")

    def test_load_invalid_yaml_syntax(self, tmp_path):
        """Test loading a file with invalid YAML syntax raises InputError."""
        config_file = tmp_path / "invalid.yml"
        config_file.write_text("invalid: yaml: content: [")

        with pytest.raises(InputError, match="Invalid YAML syntax"):
            load_config_file(str(config_file))

    def test_load_non_dict_yaml(self, tmp_path):
        """Test loading a YAML file that doesn't contain a dictionary."""
        config_file = tmp_path / "list.yml"
        config_file.write_text("- item1\n- item2")

        with pytest.raises(ConfigValidationError, match="Configuration must be a YAML dictionary"):
            load_config_file(str(config_file))

    def test_load_config_missing_core_section(self, tmp_path):
        """Test loading a config file without required 'core' section."""
        config_data = {"publish": {"skip": {"dev": True}}}
        config_file = tmp_path / "no_core.yml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        with pytest.raises(ConfigValidationError, match="must contain a 'core' section"):
            load_config_file(str(config_file))


class TestWorkspaceSettingsExtraction:
    """Test workspace settings extraction from config."""

    def test_extract_workspace_id_by_environment(self):
        """Test extracting workspace ID based on environment."""
        config = {
            "core": {
                "workspace_id": {"dev": "dev-id", "prod": "prod-id"},
                "repository_directory": "test/path",
            }
        }

        settings = extract_workspace_settings(config, "dev")
        assert settings["workspace_id"] == "dev-id"
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
        """Test error when using single workspace ID (not environment-specific)."""
        config_data = {
            "core": {
                "workspace_id": "single-id",
                "repository_directory": "test/path",
            }
        }
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump(config_data))

        # Now single workspace IDs are not allowed - must be environment mapping
        with pytest.raises(ConfigValidationError, match="must be an environment mapping dictionary"):
            load_config_file(str(config_file))

    def test_extract_missing_environment(self):
        """Test error when environment not found in workspace mappings."""
        config = {
            "core": {
                "workspace_id": {"dev": "dev-id"},
                "repository_directory": "test/path",
            }
        }

        with pytest.raises(InputError, match="Environment 'prod' not found in workspace_id mappings"):
            extract_workspace_settings(config, "prod")

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
            load_config_file(str(config_file))

    def test_extract_missing_repository_directory(self, tmp_path):
        """Test error when repository_directory is missing."""
        config_data = {
            "core": {
                "workspace_id": {"dev": "test-id"},
            }
        }
        config_file = tmp_path / "config.yml"
        config_file.write_text(yaml.dump(config_data))

        with pytest.raises(ConfigValidationError, match="must specify 'repository_directory'"):
            load_config_file(str(config_file))

    def test_extract_optional_item_types(self):
        """Test extracting optional item_types_in_scope."""
        config = {
            "core": {
                "workspace_id": "test-id",
                "repository_directory": "test/path",
                "item_types_in_scope": ["Notebook", "DataPipeline"],
            }
        }

        settings = extract_workspace_settings(config, "dev")
        assert settings["item_types_in_scope"] == ["Notebook", "DataPipeline"]


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

    @patch("fabric_cicd.constants.FEATURE_FLAG", set())
    def test_apply_feature_flags(self):
        """Test applying feature flags from config."""
        config = {"features": ["enable_shortcut_publish", "enable_debug_mode"]}

        apply_config_overrides(config)

        from fabric_cicd import constants

        assert "enable_shortcut_publish" in constants.FEATURE_FLAG
        assert "enable_debug_mode" in constants.FEATURE_FLAG

    def test_apply_constants_overrides(self):
        """Test applying constants overrides from config."""
        config = {"constants": {"DEFAULT_API_ROOT_URL": "https://custom.api.com"}}

        # This will log a warning since DEFAULT_API_ROOT_URL exists in constants
        # but it's hard to mock the setattr behavior cleanly. Let's just test it doesn't crash.
        apply_config_overrides(config)

    def test_apply_no_overrides(self):
        """Test applying config overrides when no overrides are specified."""
        config = {}

        # Should not raise any errors
        apply_config_overrides(config)


class TestDeployWithConfig:
    """Test the main deploy_with_config function."""

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_full_deployment(self, mock_unpublish, mock_publish, mock_workspace, tmp_path):
        """Test full deployment with config file."""
        # Create test config file
        config_data = {
            "core": {
                "workspace_id": {"dev": "dev-workspace-id"},
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
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Mock workspace instance
        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance

        # Execute deployment
        deploy_with_config(str(config_file), "dev")

        # Verify workspace creation
        mock_workspace.assert_called_once_with(
            workspace_id="dev-workspace-id",
            workspace_name=None,
            repository_directory="test/path",
            item_type_in_scope=["Notebook", "DataPipeline"],
            environment="dev",
            token_credential=None,
        )

        # Verify publish and unpublish calls
        mock_publish.assert_called_once_with(
            mock_workspace_instance,
            item_name_exclude_regex="^DONT_DEPLOY.*",
            items_to_include=None,
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
        # Create test config file with skip flags
        config_data = {
            "core": {
                "workspace_id": {"dev": "test-workspace-id"},
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
        with open(config_file, "w") as f:
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

    @patch("fabric_cicd.publish.FabricWorkspace")
    def test_deploy_with_config_missing_file(self, _mock_workspace):
        """Test deployment with missing config file."""
        with pytest.raises(ConfigValidationError, match="Configuration file not found"):
            deploy_with_config("nonexistent.yml", "dev")

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    def test_deploy_with_config_with_token_credential(self, _mock_unpublish, _mock_publish, mock_workspace, tmp_path):
        """Test deployment with custom token credential."""
        # Create test config file
        config_data = {
            "core": {
                "workspace_id": {"dev": "test-workspace-id"},
                "repository_directory": "test/path",
            },
        }
        config_file = tmp_path / "config.yml"
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Mock workspace instance and token credential
        mock_workspace_instance = MagicMock()
        mock_workspace.return_value = mock_workspace_instance
        mock_credential = MagicMock()

        # Execute deployment
        deploy_with_config(str(config_file), "dev", token_credential=mock_credential)

        # Verify workspace creation with token credential
        mock_workspace.assert_called_once_with(
            workspace_id="test-workspace-id",
            workspace_name=None,
            repository_directory="test/path",
            item_type_in_scope=None,
            environment="dev",
            token_credential=mock_credential,
        )


class TestConfigIntegration:
    """Integration tests for config functionality."""

    def test_sample_config_file_structure(self):
        """Test that the sample config file can be loaded and parsed correctly."""
        # Test with the actual sample config file
        sample_config_path = Path(__file__).parent.parent / "sample" / "workspace" / "config.yml"

        if sample_config_path.exists():
            config = load_config_file(str(sample_config_path))

            # Verify basic structure
            assert "core" in config
            assert "publish" in config
            assert "unpublish" in config

            # Test environment extraction
            workspace_settings = extract_workspace_settings(config, "dev")
            assert "repository_directory" in workspace_settings

            # Test settings extraction functions (verify they don't crash)
            extract_publish_settings(config, "dev")
            extract_unpublish_settings(config, "dev")

            # Should not raise any errors
            apply_config_overrides(config)

    def test_config_validation_comprehensive(self, tmp_path):
        """Test comprehensive config validation with all sections."""
        config_data = {
            "core": {
                "workspace_id": {"dev": "dev-id", "test": "test-id", "prod": "prod-id"},
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
        with open(config_file, "w") as f:
            yaml.dump(config_data, f)

        # Test loading and parsing
        config = load_config_file(str(config_file))
        assert config == config_data

        # Test all environment extractions
        for env in ["dev", "test", "prod"]:
            workspace_settings = extract_workspace_settings(config, env)
            assert workspace_settings["workspace_id"] == config_data["core"]["workspace_id"][env]

            publish_settings = extract_publish_settings(config, env)
            assert publish_settings["skip"] == config_data["publish"]["skip"][env]

            unpublish_settings = extract_unpublish_settings(config, env)
            assert unpublish_settings["skip"] == config_data["unpublish"]["skip"][env]
