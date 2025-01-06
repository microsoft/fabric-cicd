import os
import sys

# Set location of importable modules
root_path = str(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.insert(0, root_path + "/src")
sys.path.insert(0, root_path + "/sample")

# Configuration file for the Sphinx documentation builder.

project = "fabric-cicd"
copyright = "Microsoft Corporation"
author = "Microsoft Corporation"

# Load the version
with open(root_path + "/VERSION") as version_file:
    release = version_file.read().strip()

extensions = [
    "sphinx.ext.autodoc",
    "sphinx_autodoc_typehints",
    "myst_parser",
]

html_theme = "sphinx_rtd_theme"

autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
    "special-members": "__init__",
}

suppress_warnings = ["myst.header"]
