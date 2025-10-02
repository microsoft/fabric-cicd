import tempfile
from pathlib import Path

from fabric_cicd._parameter._parameter import Parameter


class TestParameterExtend:
    """Test class for parameter extend functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repository_directory = Path(self.temp_dir.name)
        self.item_type_in_scope = ["Notebook", "DataPipeline"]
        self.environment = "PPE"

    def teardown_method(self):
        """Clean up test environment."""
        self.temp_dir.cleanup()

    def test_extend_basic_functionality(self):
        """Test basic extend functionality with one parameter file."""
        # Create main parameter file
        main_param_content = """
extend:
  - parameter_1.yml

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Create extended parameter file
        extended_param_content = """
find_replace:
  - find_value: "extended-value"
    replace_value:
      PPE: "extended-ppe"
      PROD: "extended-prod"
"""
        extended_param_file = self.repository_directory / "parameter_1.yml"
        extended_param_file.write_text(extended_param_content)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify the extended file was loaded and merged
        assert "find_replace" in parameter.environment_parameter
        assert len(parameter.environment_parameter["find_replace"]) == 2

        # Check both main and extended values are present
        find_values = [item["find_value"] for item in parameter.environment_parameter["find_replace"]]
        assert "main-value" in find_values
        assert "extended-value" in find_values

    def test_extend_multiple_files(self):
        """Test extend functionality with multiple parameter files."""
        # Create main parameter file
        main_param_content = """
extend:
  - parameter_1.yml
  - parameter_2.yml

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Create first extended parameter file
        extended_param_content_1 = """
find_replace:
  - find_value: "db52be81-c2b2-4261-84fa-840c67f4bbd0"
    replace_value:
      PPE: "81bbb339-8d0b-46e8-bfa6-289a159c0733"
      PROD: "5d6a1b16-447f-464a-b959-45d0fed35ca0"
"""
        extended_param_file_1 = self.repository_directory / "parameter_1.yml"
        extended_param_file_1.write_text(extended_param_content_1)

        # Create second extended parameter file with regex pattern
        extended_param_content_2 = r"""
find_replace:
  - find_value: '\#\s*META\s+"default_lakehouse":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"'
    replace_value:
      PPE: "$items.Lakehouse.WithoutSchema.id"
      PROD: "$items.Lakehouse.WithoutSchema.id"
"""
        extended_param_file_2 = self.repository_directory / "parameter_2.yml"
        extended_param_file_2.write_text(extended_param_content_2)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify all files were loaded and merged
        assert "find_replace" in parameter.environment_parameter
        assert len(parameter.environment_parameter["find_replace"]) == 3

        # Check all values are present
        find_values = [item["find_value"] for item in parameter.environment_parameter["find_replace"]]
        assert "main-value" in find_values
        assert "db52be81-c2b2-4261-84fa-840c67f4bbd0" in find_values
        assert (
            r'\#\s*META\s+"default_lakehouse":\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"'
            in find_values
        )

    def test_extend_missing_file(self):
        """Test that missing extended files are handled gracefully."""
        # Create main parameter file
        main_param_content = """
extend:
  - nonexistent.yml

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify only main parameter is loaded
        assert "find_replace" in parameter.environment_parameter
        assert len(parameter.environment_parameter["find_replace"]) == 1
        assert parameter.environment_parameter["find_replace"][0]["find_value"] == "main-value"

    def test_extend_validation_errors(self):
        """Test validation errors for extend parameter."""
        # Create main parameter file with invalid extend
        main_param_content = """
extend:
  - path: invalid_structure

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Test parameter loading should fail due to invalid extend structure
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify validation fails
        is_valid, msg = parameter._validate_extend_parameter()
        assert not is_valid
        assert "must be a string file path" in msg

    def test_extend_empty_file_path_fails_validation(self):
        """Test that empty file paths fail validation."""
        # Create main parameter file with empty extend path
        main_param_content = """
extend:
  - ""

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify validation fails for empty string
        is_valid, msg = parameter._validate_extend_parameter()
        assert not is_valid
        assert "must be a non-empty string" in msg

    def test_extend_multiple_sections(self):
        """Test extend with multiple parameter sections."""
        # Create main parameter file
        main_param_content = """
extend:
  - extended_params.yml

find_replace:
  - find_value: "main-find"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"

key_value_replace:
  - find_key: "$.main.key"
    replace_value:
      PPE: "main-key-ppe"
      PROD: "main-key-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Create extended parameter file
        extended_param_content = """
find_replace:
  - find_value: "extended-find"
    replace_value:
      PPE: "extended-ppe"
      PROD: "extended-prod"

spark_pool:
  - instance_pool_id: "extended-pool-id"
    replace_value:
      PPE:
        type: "Capacity"
        name: "ExtendedPool"
      PROD:
        type: "Capacity"
        name: "ExtendedPool"
"""
        extended_param_file = self.repository_directory / "extended_params.yml"
        extended_param_file.write_text(extended_param_content)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify all sections are present and merged correctly
        assert "find_replace" in parameter.environment_parameter
        assert "key_value_replace" in parameter.environment_parameter
        assert "spark_pool" in parameter.environment_parameter

        # Check find_replace has both main and extended values
        assert len(parameter.environment_parameter["find_replace"]) == 2

        # Check key_value_replace has only main value (extended didn't have this section)
        assert len(parameter.environment_parameter["key_value_replace"]) == 1

        # Check spark_pool has only extended value (main didn't have this section)
        assert len(parameter.environment_parameter["spark_pool"]) == 1

    def test_extend_recursion_prevention(self):
        """Test that extend sections are removed from extended files to prevent recursion."""
        # Create main parameter file
        main_param_content = """
extend:
  - extended_params.yml

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Create extended parameter file with its own extend section (should be ignored)
        extended_param_content = """
extend:
  - should_be_ignored.yml

find_replace:
  - find_value: "extended-value"
    replace_value:
      PPE: "extended-ppe"
      PROD: "extended-prod"
"""
        extended_param_file = self.repository_directory / "extended_params.yml"
        extended_param_file.write_text(extended_param_content)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify only main and first extended parameters are loaded (no recursion)
        assert "find_replace" in parameter.environment_parameter
        assert len(parameter.environment_parameter["find_replace"]) == 2

        find_values = [item["find_value"] for item in parameter.environment_parameter["find_replace"]]
        assert "main-value" in find_values
        assert "extended-value" in find_values

    def test_extend_no_extend_section(self):
        """Test parameter file without extend section works normally."""
        # Create main parameter file without extend
        main_param_content = """
find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify normal parameter loading
        assert "find_replace" in parameter.environment_parameter
        assert len(parameter.environment_parameter["find_replace"]) == 1
        assert parameter.environment_parameter["find_replace"][0]["find_value"] == "main-value"
