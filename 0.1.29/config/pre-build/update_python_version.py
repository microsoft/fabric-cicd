import re
try:
    import tomllib  # Python 3.11+
except ImportError:
    import toml as tomllib  # Fallback for older Python versions
from pathlib import Path


def on_page_markdown(markdown, **kwargs):
    """
    Replace Python version placeholders with versions from pyproject.toml
    """
    if "<!--MIN-PYTHON-VERSION-->" in markdown or "<!--MAX-PYTHON-VERSION-->" in markdown:
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
            
            # Extract min and max versions
            min_version = "3.9"  # fallback
            max_version = "3.12"  # fallback
            
            if requires_python:
                min_match = re.search(r'>=(\d+\.\d+)', requires_python)
                max_match = re.search(r'<(\d+\.\d+)', requires_python)
                
                if min_match:
                    min_version = min_match.group(1)
                if max_match:
                    # Convert exclusive max to inclusive (e.g., <3.13 means up to 3.12)
                    max_parts = max_match.group(1).split('.')
                    max_major = int(max_parts[0])
                    max_minor = int(max_parts[1])
                    if max_minor > 0:
                        max_version = f"{max_major}.{max_minor - 1}"
                
        except Exception:
            # Use fallback values
            min_version = "3.9"
            max_version = "3.12"
        
        # Replace placeholders
        markdown = markdown.replace("<!--MIN-PYTHON-VERSION-->", min_version)
        markdown = markdown.replace("<!--MAX-PYTHON-VERSION-->", max_version)
    
    return markdown