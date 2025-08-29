import sys
import re
from pathlib import Path

root_directory = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(root_directory / "src"))

def get_python_version_requirements():
    """Parse pyproject.toml to extract Python version requirements."""
    pyproject_path = root_directory / "pyproject.toml"
    
    if not pyproject_path.exists():
        return "3.9 to 3.12"  # fallback
    
    with pyproject_path.open() as f:
        content = f.read()
    
    # Find the requires-python line
    match = re.search(r'requires-python\s*=\s*"([^"]+)"', content)
    if not match:
        return "3.9 to 3.12"  # fallback
    
    requires_python = match.group(1)
    
    # Parse version constraints like ">=3.9,<3.13"
    min_version_match = re.search(r'>=(\d+\.\d+)', requires_python)
    max_version_match = re.search(r'<(\d+\.\d+)', requires_python)
    
    if min_version_match and max_version_match:
        min_version = min_version_match.group(1)
        max_version_str = max_version_match.group(1)
        # Convert max version to the last supported version
        # e.g., "<3.13" means "up to 3.12"
        max_parts = max_version_str.split('.')
        max_major = int(max_parts[0])
        max_minor = int(max_parts[1])
        actual_max = f"{max_major}.{max_minor - 1}"
        return f"{min_version} to {actual_max}"
    
    return "3.9 to 3.12"  # fallback


def on_page_markdown(markdown, **kwargs):
    """Replace Python version placeholders with dynamic versions from pyproject.toml."""
    python_version = get_python_version_requirements()
    
    # Replace patterns in documentation
    patterns = [
        (r'\*\*Requirements\*\*:\s*Python\s+[\d\.]+ to [\d\.]+', f'**Requirements**: Python {python_version}'),
        (r'\(version\s+[\d\.]+ to [\d\.]+\)', f'(version {python_version})')
    ]
    
    for pattern, replacement in patterns:
        markdown = re.sub(pattern, replacement, markdown)
    
    return markdown