# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the enable_strict_environment_mapping feature flag and null values in environment mappings."""

import contextlib
import logging
from unittest.mock import MagicMock, patch

import pytest
import yaml

from fabric_cicd import constants, deploy_with_config
from fabric_cicd._common._config_utils import (
    config_overrides_scope,
    extract_publish_settings,
    extract_unpublish_settings,
    extract_workspace_settings,
)
from fabric_cicd._common._config_validator import ConfigValidationError, ConfigValidator, _get_config_fields

STRICT_FLAG = constants.FeatureFlag.ENABLE_STRICT_ENVIRONMENT_MAPPING.value
WORKSPACE_ID = "11111111-1111-1111-1111-111111111111"
TARGET = "PROD"

# (section, field, value for DEV). A section of None means the field is at the top level of the config.
OPTIONAL_FIELDS = [
    ("core", "item_types_in_scope", ["Notebook"]),
    ("core", "parameter", "parameter.yml"),
    ("publish", "exclude_regex", "^DEBUG.*"),
    ("publish", "folder_exclude_regex", "^/legacy"),
    ("publish", "folder_path_to_include", ["/reports"]),
    ("publish", "shortcut_exclude_regex", "^tmp_"),
    ("publish", "items_to_include", ["Hello World.Notebook"]),
    ("publish", "skip", True),
    ("unpublish", "exclude_regex", "^DEBUG.*"),
    ("unpublish", "items_to_include", ["Hello World.Notebook"]),
    ("unpublish", "skip", True),
    (None, "features", ["enable_shortcut_publish"]),
]
FIELD_IDS = [f"{section}.{field}" if section else field for section, field, _ in OPTIONAL_FIELDS]


def _strict_error(display_name: str, available_envs: tuple = ("DEV",), environment: str = TARGET) -> str:
    return constants.CONFIG_VALIDATION_MSGS["environment"]["strict_env_not_set"].format(
        display_name, environment, list(available_envs), environment
    )


def _config_with(section, field, mapping, base=None):
    config = base or {"core": {"workspace_id": WORKSPACE_ID, "repository_directory": "workspace"}}
    target = config if section is None else config.setdefault(section, {})
    target[field] = mapping
    return config


def _write_config(tmp_path, config):
    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_file = tmp_path / "config.yml"
    config_file.write_text(yaml.dump(config))
    return config_file


def _validate(tmp_path, config, environment=TARGET, config_override=None):
    config_file = _write_config(tmp_path, config)
    return ConfigValidator().validate_config_file(str(config_file), environment, config_override)


@contextlib.contextmanager
def _mocked_deployment():
    """Mock the workspace and the API calls that deploy_with_config makes."""
    with (
        patch("fabric_cicd.publish.FabricWorkspace") as mock_workspace,
        patch("fabric_cicd.publish.publish_all_items") as mock_publish,
        patch("fabric_cicd.publish.unpublish_all_orphan_items") as mock_unpublish,
    ):
        yield mock_workspace, mock_publish, mock_unpublish


def _deploy(config_file):
    """Run deploy_with_config for the target environment with the workspace and the API calls mocked."""
    with _mocked_deployment() as mocks:
        deploy_with_config(config_file_path=str(config_file), token_credential=MagicMock(), environment=TARGET)
    return mocks


@pytest.fixture(autouse=True)
def isolated_feature_flags():
    """Run every test with an empty, private set of feature flags."""
    with patch.object(constants, "FEATURE_FLAG", set()):
        yield


@pytest.fixture
def strict_flag():
    """Set the strict environment mapping flag for one test."""
    constants.FEATURE_FLAG.add(STRICT_FLAG)


def test_optional_fields_cover_every_optional_config_field():
    """Test OPTIONAL_FIELDS lists every optional field of _get_config_fields(), so the parametrized tests stay complete."""
    optional_fields = {display for _, _, display, required, _ in _get_config_fields({}) if not required}

    assert optional_fields == {*FIELD_IDS, "constants"}


class TestWithoutStrictFlag:
    """Without the flag, a missing environment keeps the default behavior."""

    @pytest.mark.parametrize(("section", "field", "value"), OPTIONAL_FIELDS, ids=FIELD_IDS)
    def test_missing_environment_is_skipped(self, tmp_path, section, field, value):
        """Test a missing environment in an optional field passes validation."""
        _validate(tmp_path, _config_with(section, field, {"DEV": value}))

    def test_missing_constant_environment_is_skipped(self, tmp_path):
        """Test a missing environment in a constants mapping passes validation."""
        config = _config_with(None, "constants", {"ACCEPTED_ITEM_TYPES": {"DEV": ["Notebook"]}})

        _validate(tmp_path, config)

    @pytest.mark.parametrize(
        ("section", "field", "value", "level"),
        [
            ("core", "item_types_in_scope", ["Notebook"], logging.WARNING),
            ("unpublish", "skip", True, logging.DEBUG),
        ],
        ids=["warning-field", "debug-field"],
    )
    def test_missing_environment_is_logged(self, tmp_path, caplog, section, field, value, level):
        """Test a missing environment logs the existing warning or debug message instead of an error."""
        caplog.set_level(logging.DEBUG, logger="fabric_cicd")

        _validate(tmp_path, _config_with(section, field, {"DEV": value}))

        record = next(r for r in caplog.records if f"'{section}.{field}'" in r.getMessage())
        assert record.levelno == level
        assert "This setting will be skipped" in record.getMessage()


class TestWithStrictFlag:
    """With the flag, a missing environment in an optional field is a validation error."""

    @pytest.mark.usefixtures("strict_flag")
    @pytest.mark.parametrize(("section", "field", "value"), OPTIONAL_FIELDS, ids=FIELD_IDS)
    def test_missing_environment_fails(self, tmp_path, section, field, value):
        """Test a missing environment in each optional field is a validation error."""
        display_name = f"{section}.{field}" if section else field

        with pytest.raises(ConfigValidationError) as error:
            _validate(tmp_path, _config_with(section, field, {"DEV": value}))

        assert error.value.validation_errors == [_strict_error(display_name)]

    @pytest.mark.usefixtures("strict_flag")
    def test_error_message_text(self, tmp_path):
        """Test the full text of the error, as users see it."""
        with pytest.raises(ConfigValidationError) as error:
            _validate(tmp_path, _config_with("core", "item_types_in_scope", {"DEV": ["Notebook"]}))

        assert error.value.validation_errors == [
            "'core.item_types_in_scope' is not set for environment 'PROD'. Available environments: ['DEV']. "
            "Without strict mapping, fabric-cicd ignores the setting for that environment. "
            "Add 'PROD' to the mapping, or set it to null to keep the default."
        ]

    @pytest.mark.usefixtures("strict_flag")
    def test_missing_constant_environment_fails(self, tmp_path, caplog):
        """Test a missing environment in a constants mapping is an error, without the skip warning."""
        caplog.set_level(logging.DEBUG, logger="fabric_cicd")
        config = _config_with(None, "constants", {"ACCEPTED_ITEM_TYPES": {"DEV": ["Notebook"]}})

        with pytest.raises(ConfigValidationError) as error:
            _validate(tmp_path, config)

        assert error.value.validation_errors == [_strict_error("constants.ACCEPTED_ITEM_TYPES")]
        assert "will be skipped" not in caplog.text

    @pytest.mark.usefixtures("strict_flag")
    def test_every_missing_setting_is_reported_at_once(self, tmp_path):
        """Test all missing settings are collected into one validation error."""
        config = _config_with("core", "item_types_in_scope", {"DEV": ["Notebook"]})
        config = _config_with("unpublish", "skip", {"DEV": True}, base=config)

        with pytest.raises(ConfigValidationError) as error:
            _validate(tmp_path, config)

        assert error.value.validation_errors == [
            _strict_error("core.item_types_in_scope"),
            _strict_error("unpublish.skip"),
        ]

    @pytest.mark.usefixtures("strict_flag")
    def test_flat_values_and_mappings_with_the_target_pass(self, tmp_path):
        """Test flat values and mappings that contain the target environment pass, also when other environments are absent."""
        config = _config_with("core", "item_types_in_scope", ["Notebook"])
        config = _config_with("unpublish", "skip", {"DEV": True, TARGET: False}, base=config)
        config = _config_with("publish", "exclude_regex", {TARGET: "^DEBUG.*"}, base=config)

        _validate(tmp_path, config)

    @pytest.mark.usefixtures("strict_flag")
    def test_required_field_keeps_its_own_error(self, tmp_path):
        """Test a missing environment in a required field keeps the existing error."""
        config = {"core": {"workspace_id": {"DEV": WORKSPACE_ID}, "repository_directory": "workspace"}}

        with pytest.raises(ConfigValidationError) as error:
            _validate(tmp_path, config)

        assert error.value.validation_errors == [
            constants.CONFIG_VALIDATION_MSGS["environment"]["env_not_found"].format(
                TARGET, "core.workspace_id", ["DEV"]
            )
        ]


class TestStrictFlagFromConfigFile:
    """The flag also works from the features section, which is applied only after validation."""

    def test_flag_in_features_list(self, tmp_path):
        """Test the flag in a features list enables the check."""
        config = _config_with(None, "features", [STRICT_FLAG])
        config = _config_with("core", "item_types_in_scope", {"DEV": ["Notebook"]}, base=config)

        with pytest.raises(ConfigValidationError) as error:
            _validate(tmp_path, config)

        assert error.value.validation_errors == [_strict_error("core.item_types_in_scope")]

    def test_flag_in_features_mapping_for_target_environment(self, tmp_path):
        """Test the flag in the features mapping of the target environment enables the check."""
        config = _config_with(None, "features", {"DEV": ["enable_shortcut_publish"], TARGET: [STRICT_FLAG]})
        config = _config_with("core", "item_types_in_scope", {"DEV": ["Notebook"]}, base=config)

        with pytest.raises(ConfigValidationError) as error:
            _validate(tmp_path, config)

        assert error.value.validation_errors == [_strict_error("core.item_types_in_scope")]

    def test_flag_for_another_environment_is_ignored(self, tmp_path):
        """Test the flag in the features mapping of another environment does not enable the check."""
        config = _config_with(None, "features", {"DEV": [STRICT_FLAG], TARGET: ["enable_shortcut_publish"]})
        config = _config_with("core", "item_types_in_scope", {"DEV": ["Notebook"]}, base=config)

        _validate(tmp_path, config)

    def test_flag_in_config_override(self, tmp_path):
        """Test the flag in a config_override features section enables the check."""
        config = _config_with("core", "item_types_in_scope", {"DEV": ["Notebook"]})

        with pytest.raises(ConfigValidationError) as error:
            _validate(tmp_path, config, config_override={"features": [STRICT_FLAG]})

        assert error.value.validation_errors == [_strict_error("core.item_types_in_scope")]

    def test_validation_does_not_enable_the_flag(self, tmp_path):
        """Test validation reads the flag from the features section without adding it to FEATURE_FLAG."""
        _validate(tmp_path, _config_with(None, "features", [STRICT_FLAG]))

        assert STRICT_FLAG not in constants.FEATURE_FLAG

    def test_deploy_with_config_stops_before_publish(self, tmp_path):
        """Test deploy_with_config fails before it creates the workspace or publishes anything."""
        config = _config_with(None, "features", [STRICT_FLAG])
        config = _config_with("core", "item_types_in_scope", {"DEV": ["Notebook"]}, base=config)
        config_file = _write_config(tmp_path, config)

        with (
            _mocked_deployment() as (mock_workspace, mock_publish, mock_unpublish),
            pytest.raises(ConfigValidationError) as error,
        ):
            deploy_with_config(config_file_path=str(config_file), token_credential=MagicMock(), environment=TARGET)

        assert error.value.validation_errors == [_strict_error("core.item_types_in_scope")]
        mock_workspace.assert_not_called()
        mock_publish.assert_not_called()
        mock_unpublish.assert_not_called()
        assert STRICT_FLAG not in constants.FEATURE_FLAG


class TestNullInMapping:
    """null in an optional mapping means "deliberately not set".

    At deployment time it has the same effect as a missing key. Unlike a missing key, it passes validation with the flag set.
    """

    @pytest.mark.parametrize("strict", [False, True], ids=["default", "strict"])
    @pytest.mark.parametrize(("section", "field", "value"), OPTIONAL_FIELDS, ids=FIELD_IDS)
    def test_null_is_accepted(self, tmp_path, request, section, field, value, strict):
        """Test null in each optional field passes validation, with and without the flag."""
        if strict:
            request.getfixturevalue("strict_flag")

        _validate(tmp_path, _config_with(section, field, {"DEV": value, TARGET: None}))

    @pytest.mark.usefixtures("strict_flag")
    def test_null_constant_is_accepted_and_not_applied(self, tmp_path):
        """Test a null constant passes validation and does not override the constant."""
        original = constants.DEFAULT_API_ROOT_URL
        config = _config_with(
            None, "constants", {"DEFAULT_API_ROOT_URL": {"DEV": "https://api.fabric.microsoft.com", TARGET: None}}
        )

        validated = _validate(tmp_path, config)

        with config_overrides_scope(validated, TARGET):
            assert original == constants.DEFAULT_API_ROOT_URL

    @pytest.mark.parametrize(
        ("field", "core"),
        [
            (
                "workspace_id",
                {"workspace_id": {"DEV": WORKSPACE_ID, TARGET: None}, "repository_directory": "workspace"},
            ),
            ("workspace", {"workspace": {"DEV": "My Workspace", TARGET: None}, "repository_directory": "workspace"}),
            (
                "repository_directory",
                {"workspace_id": WORKSPACE_ID, "repository_directory": {"DEV": "workspace", TARGET: None}},
            ),
        ],
        ids=["workspace_id", "workspace", "repository_directory"],
    )
    def test_null_is_rejected_for_required_fields(self, tmp_path, field, core):
        """Test null stays invalid in the mapping of a required field."""
        with pytest.raises(ConfigValidationError) as error:
            _validate(tmp_path, {"core": core})

        # An invalid workspace mapping also reports the missing workspace identifier, as the existing validation does
        assert (
            f"'{field}' value for environment '{TARGET}' must be a str, got NoneType" in error.value.validation_errors
        )

    def test_null_folder_filter_does_not_conflict(self, tmp_path):
        """Test a null folder filter does not conflict with the other folder filter."""
        config = _config_with("publish", "folder_exclude_regex", {"DEV": "^/legacy", TARGET: None})
        config = _config_with("publish", "folder_path_to_include", {TARGET: ["/reports"]}, base=config)

        _validate(tmp_path, config)

    def test_null_folder_filter_reaches_publish_as_unset(self, tmp_path):
        """Test a null folder filter reaches publish_all_items as None, so its own conflict check passes."""
        config = _config_with("publish", "folder_exclude_regex", {"DEV": "^/legacy", TARGET: None})
        config = _config_with("publish", "folder_path_to_include", {TARGET: ["/reports"]}, base=config)

        _, mock_publish, _ = _deploy(_write_config(tmp_path, config))

        call_kwargs = mock_publish.call_args[1]
        assert call_kwargs["folder_path_exclude_regex"] is None
        assert call_kwargs["folder_path_to_include"] == ["/reports"]

    def test_null_features_apply_no_flags(self):
        """Test null in the features mapping applies no feature flags."""
        config = {"features": {"DEV": ["enable_shortcut_publish"], TARGET: None}}

        with config_overrides_scope(config, TARGET):
            assert set() == constants.FEATURE_FLAG

    def test_null_settings_resolve_to_defaults(self):
        """Test null settings resolve to the same defaults as missing settings."""
        mapping = {"DEV": "x", TARGET: None}
        config = {
            "core": {"workspace_id": WORKSPACE_ID, "repository_directory": "/repo", "parameter": mapping},
            "publish": {"exclude_regex": mapping, "skip": {"DEV": True, TARGET: None}},
            "unpublish": {"items_to_include": {"DEV": ["x"], TARGET: None}, "skip": {"DEV": True, TARGET: None}},
        }

        assert "parameter_file_path" not in extract_workspace_settings(config, TARGET)
        assert extract_publish_settings(config, TARGET) == {"skip": False}
        assert extract_unpublish_settings(config, TARGET) == {"skip": False}

    def test_null_parameter_skips_parameterization(self, tmp_path):
        """Test a null parameter skips parameterization, also when a parameter.yml exists."""
        config = _config_with("core", "parameter", {"DEV": "workspace/parameter.yml", TARGET: None})
        config_file = _write_config(tmp_path, config)
        (tmp_path / "workspace" / "parameter.yml").write_text(
            "find_replace:\n  - find_value: 'old'\n    replace_value:\n      DEV: 'new'\n"
        )

        mock_workspace, _, _ = _deploy(config_file)

        call_kwargs = mock_workspace.call_args[1]
        assert call_kwargs["parameter_file_path"] is None
        assert call_kwargs["skip_parameterization"] is True

    def test_null_parameter_with_empty_environment(self, tmp_path):
        """Test a null parameter is skipped in path resolution, also when the environment is an empty string."""
        (tmp_path / "workspace").mkdir()
        (tmp_path / "workspace" / "parameter.yml").write_text("find_replace: []\n")
        config = _config_with("core", "parameter", {"DEV": "workspace/parameter.yml", TARGET: None})

        validated = _validate(tmp_path, config, environment="")

        assert validated["core"]["parameter"][TARGET] is None
