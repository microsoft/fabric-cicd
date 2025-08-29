import re
try:
    import tomllib  # Python 3.11+
except ImportError:
    import toml as tomllib  # Fallback for older Python versions
from pathlib import Path


def extract_python_version_range(requires_python_str):
    """
    Extract Python version range from requires-python string.
    
    Args:
        requires_python_str: String like ">=3.9,<3.13"
        
    Returns:
        Tuple of (min_version, max_version_exclusive) like ("3.9", "3.13")
    """
    # Extract minimum version (>=X.Y)
    min_match = re.search(r'>=(\d+\.\d+)', requires_python_str)
    min_version = min_match.group(1) if min_match else None
    
    # Extract maximum version (<X.Y)
    max_match = re.search(r'<(\d+\.\d+)', requires_python_str)
    max_version_exclusive = max_match.group(1) if max_match else None
    
    return min_version, max_version_exclusive


def format_python_version_range(min_version, max_version_exclusive):
    """
    Format Python version range for user display.
    
    Args:
        min_version: String like "3.9"
        max_version_exclusive: String like "3.13"
        
    Returns:
        String like "3.9 to 3.12"
    """
    if not min_version:
        return "Version not specified"
    
    if not max_version_exclusive:
        return f"{min_version} or higher"
    
    # Convert exclusive max to inclusive max (e.g., <3.13 means up to 3.12)
    max_parts = max_version_exclusive.split('.')
    max_major = int(max_parts[0])
    max_minor = int(max_parts[1])
    
    # Decrement minor version for inclusive range
    if max_minor > 0:
        max_inclusive = f"{max_major}.{max_minor - 1}"
    else:
        max_inclusive = f"{max_major - 1}.99"  # Edge case, unlikely
    
    return f"{min_version} to {max_inclusive}"


def on_page_markdown(markdown, **kwargs):
    """
    Replace Python version placeholder with dynamic version from pyproject.toml
    """
    if "<!--PYTHON-VERSION-REQUIREMENTS-->" in markdown:
        # Get pyproject.toml path (4 levels up from this file)
        root_directory = Path(__file__).resolve().parent.parent.parent.parent
        pyproject_path = root_directory / "pyproject.toml"
        
        try:
            # Load pyproject.toml
            with open(pyproject_path, 'rb') as f:
                try:
                    pyproject_data = tomllib.load(f)
                except AttributeError:
                    # Fallback for older toml library
                    f.seek(0)
                    content = f.read().decode('utf-8')
                    pyproject_data = tomllib.loads(content)
            
            # Extract requires-python
            requires_python = pyproject_data.get('project', {}).get('requires-python', '')
            
            if requires_python:
                min_version, max_version_exclusive = extract_python_version_range(requires_python)
                version_display = format_python_version_range(min_version, max_version_exclusive)
                replacement = f"**Requirements**: Python {version_display}"
            else:
                replacement = "**Requirements**: Python version not specified"
                
        except Exception as e:
            # Fallback if something goes wrong
            replacement = "**Requirements**: Python (see pyproject.toml for details)"
        
        # Replace the placeholder
        markdown = markdown.replace("<!--PYTHON-VERSION-REQUIREMENTS-->", replacement)
    
    return markdown