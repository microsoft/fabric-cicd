import os
import sys

sys.path.insert(0, os.path.abspath("../../src/"))

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "fabric-cicd"
copyright = "Microsoft Corporation"
author = "Microsoft Corporation"
release = "0.1.3"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
]

intersphinx_mapping = {
    "python": ("http://docs.python.org/", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
    "pandas": ("http://pandas.pydata.org/pandas-docs/dev", None),
}

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

# List of packages we don't want to install in the environment
autodoc_mock_imports = []

napoleon_numpy_docstring = True
