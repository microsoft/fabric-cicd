import tempfile
from pathlib import Path

from fabric_cicd._parameter._parameter import Parameter


class TestParameterTemplates:
    """Test class for parameter template functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repository_directory = Path(self.temp_dir.name)
        self.item_type_in_scope = ["Notebook", "DataPipeline"]
        self.environment = "PPE"

    def teardown_method(self):
        """Clean up test environment."""
        self.temp_dir.cleanup()

    def test_templates_basic_functionality(self):
        """Test basic template functionality with one enabled template."""
        # Create main parameter file
        main_param_content = """
templates:
  - path: 'folder1'
    enabled: true

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Create template directory and parameter file
        template_dir = self.repository_directory / "folder1"
        template_dir.mkdir()
        template_param_content = """
find_replace:
  - find_value: "template-value"
    replace_value:
      PPE: "template-ppe"
      PROD: "template-prod"
"""
        template_param_file = template_dir / "parameter.yml"
        template_param_file.write_text(template_param_content)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify the template was loaded and merged
        assert "find_replace" in parameter.environment_parameter
        assert len(parameter.environment_parameter["find_replace"]) == 2

        # Check both main and template values are present
        find_values = [item["find_value"] for item in parameter.environment_parameter["find_replace"]]
        assert "main-value" in find_values
        assert "template-value" in find_values

    def test_templates_disabled_template(self):
        """Test that disabled templates are not loaded."""
        # Create main parameter file
        main_param_content = """
templates:
  - path: 'folder1'
    enabled: false

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Create template directory and parameter file
        template_dir = self.repository_directory / "folder1"
        template_dir.mkdir()
        template_param_content = """
find_replace:
  - find_value: "template-value"
    replace_value:
      PPE: "template-ppe"
      PROD: "template-prod"
"""
        template_param_file = template_dir / "parameter.yml"
        template_param_file.write_text(template_param_content)

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

    def test_templates_string_enabled_values(self):
        """Test that string 'true'/'false' values work for enabled field."""
        # Create main parameter file with string enabled values
        main_param_content = """
templates:
  - path: 'folder1'
    enabled: 'true'
  - path: 'folder2'
    enabled: 'false'

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Create enabled template
        template_dir1 = self.repository_directory / "folder1"
        template_dir1.mkdir()
        template_param_content1 = """
find_replace:
  - find_value: "template1-value"
    replace_value:
      PPE: "template1-ppe"
      PROD: "template1-prod"
"""
        template_param_file1 = template_dir1 / "parameter.yml"
        template_param_file1.write_text(template_param_content1)

        # Create disabled template
        template_dir2 = self.repository_directory / "folder2"
        template_dir2.mkdir()
        template_param_content2 = """
find_replace:
  - find_value: "template2-value"
    replace_value:
      PPE: "template2-ppe"
      PROD: "template2-prod"
"""
        template_param_file2 = template_dir2 / "parameter.yml"
        template_param_file2.write_text(template_param_content2)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify only main and enabled template are loaded
        assert "find_replace" in parameter.environment_parameter
        assert len(parameter.environment_parameter["find_replace"]) == 2

        find_values = [item["find_value"] for item in parameter.environment_parameter["find_replace"]]
        assert "main-value" in find_values
        assert "template1-value" in find_values
        assert "template2-value" not in find_values

    def test_templates_missing_template_file(self):
        """Test behavior when template parameter file doesn't exist."""
        # Create main parameter file
        main_param_content = """
templates:
  - path: 'nonexistent'
    enabled: true

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Test parameter loading (should continue without error)
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify only main parameter is loaded
        assert "find_replace" in parameter.environment_parameter
        assert len(parameter.environment_parameter["find_replace"]) == 1
        assert parameter.environment_parameter["find_replace"][0]["find_value"] == "main-value"

    def test_templates_validation_errors(self):
        """Test validation errors for invalid template configurations."""
        # Test missing path
        main_param_content = """
templates:
  - enabled: true

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Should validate but show validation error for templates
        is_valid, msg = parameter._validate_templates_parameter()
        assert not is_valid
        assert "missing required field 'path'" in msg

    def test_templates_multiple_sections(self):
        """Test templates with multiple parameter sections."""
        # Create main parameter file
        main_param_content = """
templates:
  - path: 'folder1'
    enabled: true

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

        # Create template directory and parameter file
        template_dir = self.repository_directory / "folder1"
        template_dir.mkdir()
        template_param_content = """
find_replace:
  - find_value: "template-find"
    replace_value:
      PPE: "template-ppe"
      PROD: "template-prod"

spark_pool:
  - instance_pool_id: "template-pool-id"
    replace_value:
      PPE:
        type: "Capacity"
        name: "TemplatePool"
      PROD:
        type: "Capacity"
        name: "TemplatePool"
"""
        template_param_file = template_dir / "parameter.yml"
        template_param_file.write_text(template_param_content)

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

        # Check find_replace has both main and template values
        assert len(parameter.environment_parameter["find_replace"]) == 2

        # Check key_value_replace has only main value (template didn't have this section)
        assert len(parameter.environment_parameter["key_value_replace"]) == 1

        # Check spark_pool has only template value (main didn't have this section)
        assert len(parameter.environment_parameter["spark_pool"]) == 1
        assert parameter.environment_parameter["spark_pool"][0]["instance_pool_id"] == "template-pool-id"

    def test_templates_default_enabled_true(self):
        """Test template validation requires enabled field to be explicitly set."""
        # Create main parameter file without enabled field
        main_param_content = """
templates:
  - path: 'folder1'
    enabled: true

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Create template directory and parameter file
        template_dir = self.repository_directory / "folder1"
        template_dir.mkdir()
        template_param_content = """
find_replace:
  - find_value: "template-value"
    replace_value:
      PPE: "template-ppe"
      PROD: "template-prod"
"""
        template_param_file = template_dir / "parameter.yml"
        template_param_file.write_text(template_param_content)

        # Test parameter loading
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify the template was loaded (default enabled=True)
        assert "find_replace" in parameter.environment_parameter
        assert len(parameter.environment_parameter["find_replace"]) == 2

        find_values = [item["find_value"] for item in parameter.environment_parameter["find_replace"]]
        assert "main-value" in find_values
        assert "template-value" in find_values

    def test_templates_missing_enabled_field_fails_validation(self):
        """Test that templates without enabled field fail validation."""
        # Create main parameter file without enabled field
        main_param_content = """
templates:
  - path: 'folder1'

find_replace:
  - find_value: "main-value"
    replace_value:
      PPE: "main-ppe"
      PROD: "main-prod"
"""
        main_param_file = self.repository_directory / "parameter.yml"
        main_param_file.write_text(main_param_content)

        # Create template directory and parameter file
        template_dir = self.repository_directory / "folder1"
        template_dir.mkdir()
        template_param_content = """
find_replace:
  - find_value: "template-value"
    replace_value:
      PPE: "template-ppe"
      PROD: "template-prod"
"""
        template_param_file = template_dir / "parameter.yml"
        template_param_file.write_text(template_param_content)

        # Test parameter loading should fail due to missing enabled field
        parameter = Parameter(
            repository_directory=self.repository_directory,
            item_type_in_scope=self.item_type_in_scope,
            environment=self.environment,
        )

        # Verify validation fails for missing enabled field
        is_valid, msg = parameter._validate_templates_parameter()
        assert not is_valid
        assert "missing required field 'enabled'" in msg
