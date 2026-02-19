# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "SDP Global Sky Model"
copyright = "2019-2025 SKA SDP Developers"
author = "SKA SDP Developers"
version = "0.3.0"
release = "0.3.0"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_new_tab_link",
]

exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "ska_ser_sphinx_theme"

# -- Extension configuration -------------------------------------------------

# Copybutton configuration
copybutton_exclude = ".linenos, .go, .gp"
copybutton_prompt_text = "In \[\d*\]: "
copybutton_prompt_is_regexp = True
copybutton_copy_empty_lines = False

# Intersphinx configuration
intersphinx_mapping = {
    "python": ("https://docs.python.org/", None)
}
