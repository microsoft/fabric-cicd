import subprocess
import sys

def install_missing_packages(modules):
    """
    Check if required packages are installed and install them if they are missing.
    """
    for import_name, package_name in modules.items():
        try:
            # Attempt to import the module
            __import__(import_name)
        except ImportError:
            # If the import fails, install the package
            print(f"{import_name} not found. Installing {package_name}...")
            try:
                subprocess.check_call([sys.executable, '-m', 'pip', 'install', package_name])
            except subprocess.CalledProcessError as e:
                print(f"Failed to install {package_name}: {str(e)}")

# Dictionary of modules to check and their corresponding package names
modules = {
    'yaml': 'pyyaml',
    'requests': 'requests',
    'azure.identity': 'azure-identity'
}

# Run the installation check when the module is imported
install_missing_packages(modules)
