# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

sys.path.insert(0, os.path.abspath(".."))

# -- Project information -----------------------------------------------------

project = "SCALE-MS"
copyright = "2019"
author = "SCALE-MS collaboration"

# The full version, including alpha/beta/rc tags
release = "0"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "sphinx.ext.todo",  # Note: todo_include_todos=True is required for directives to produce output.
    "sphinx.ext.viewcode",
    "sphinxcontrib.autoprogram",
    "sphinxcontrib.plantuml",
]

default_role = "any"

# Note that config options can be overridden on the command line with `-D`. E.g.
#     sphinx-build -D todo_include_todos=1 -b html -c docs/ docs/ build/html

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

root_doc = "index"

# -- Options for extlinks --------------------

extlinks = {"issue": ("https://github.com/SCALE-MS/scale-ms/issues/%s", "issue %s")}

# -- Options for intersphinx extension ---------------------------------------

# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "radical.pilot": ("https://radicalpilot.readthedocs.io/en/stable/", None),
    "radical.saga": ("https://radicalsaga.readthedocs.io/en/stable/", None),
    "radical.utils": ("https://radicalutils.readthedocs.io/en/stable", None),
    "msgpack": ("https://msgpack-python.readthedocs.io/en/latest/", None),
    "mpi4py": ("https://mpi4py.readthedocs.io/en/stable/", None),
    "pytest": ("https://docs.pytest.org/en/latest/", None),
}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']

# -- Options for autodoc -----------------------------------------------------
# autodoc_mock_imports = ['radical']
autodoc_default_options = {
    # For some reason, "inherited-members" seems to irrevocably activate ":members:"
    # "inherited-members": True,
    # 'show-inheritance': True,
}
autoclass_content = "class"
autodoc_class_signature = "separated"
autodoc_member_order = "groupwise"
autodoc_typehints = "description"
autodoc_typehints_description_target = "all"
autodoc_type_aliases = {
    "rp.Pilot": "radical.pilot.Pilot",
    "rp.Session": "radical.pilot.Session",
    "rp.PilotManager": "radical.pilot.PilotManager",
    "rp.TaskManager": "radical.pilot.TaskManager",
    "rp.Task": "radical.pilot.Task",
    "radical.pilot.pilot.Pilot": "radical.pilot.Pilot",
}

# -- Options for Napoleon ----------------------------------------------------
napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = False
# napoleon_include_private_with_doc = False
# napoleon_include_special_with_doc = False
# napoleon_use_admonition_for_examples = False
# napoleon_use_admonition_for_notes = False
# napoleon_use_admonition_for_references = False
# napoleon_use_ivar = False
# napoleon_use_param = True
# napoleon_use_rtype = True
# napoleon_use_keyword = True
napoleon_custom_sections = ["Design notes", "Caveats"]

# -- Options for plantuml extension ------------------------------------------

# Refer to https://pypi.org/project/sphinxcontrib-plantuml/ for the expected
# behavior of the plantuml wrapper script.
# Wrapper script location may not be necessary.
# plantuml = '/usr/bin/plantuml'
plantuml_output_format = "svg"
