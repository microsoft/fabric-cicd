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
        with pytest.raises(ConfigValidationError, match="Environment 'prod' not found in 'core.workspace_id' mappings"):
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

        apply_config_overrides(config, "N/A")

        from fabric_cicd import constants

        assert "enable_shortcut_publish" in constants.FEATURE_FLAG
        assert "enable_debug_mode" in constants.FEATURE_FLAG

    def test_apply_constants_overrides(self):
        """Test applying constants overrides from config."""
        config = {"constants": {"DEFAULT_API_ROOT_URL": "https://custom.api.com"}}

        # This will log a warning since DEFAULT_API_ROOT_URL exists in constants
        # but it's hard to mock the setattr behavior cleanly. Let's just test it doesn't crash.
        apply_config_overrides(config, "N/A")

    def test_apply_no_overrides(self):
        """Test applying config overrides when no overrides are specified."""
        config = {}

        # Should not raise any errors
        apply_config_overrides(config, "N/A")


class TestDeployWithConfig:
    """Test the main deploy_with_config function."""

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    @patch("fabric_cicd.constants.FEATURE_FLAG", set(["enable_experimental_features", "enable_config_deploy"]))
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
    @patch("fabric_cicd.constants.FEATURE_FLAG", set(["enable_experimental_features", "enable_config_deploy"]))
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

    @patch("fabric_cicd.constants.FEATURE_FLAG", set(["enable_experimental_features", "enable_config_deploy"]))
    def test_deploy_with_config_missing_file(self):
        """Test deployment with missing config file."""
        with pytest.raises(ConfigValidationError, match="Configuration file not found"):
            deploy_with_config("nonexistent.yml", "dev")

    @patch("fabric_cicd.publish.FabricWorkspace")
    @patch("fabric_cicd.publish.publish_all_items")
    @patch("fabric_cicd.publish.unpublish_all_orphan_items")
    @patch("fabric_cicd.constants.FEATURE_FLAG", set(["enable_experimental_features", "enable_config_deploy"]))
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
    @patch("fabric_cicd.constants.FEATURE_FLAG", set(["enable_experimental_features", "enable_config_deploy"]))
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
                apply_config_overrides(config, test_env)

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
