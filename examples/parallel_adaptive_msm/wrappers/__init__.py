"""
Provide submodules for different simulation tools in a wrappers package.

As we refine a normalized interface for simulation tools, we can provide a
scalems.wrappers submodule with normalized interfaces for various tools.
Ultimately, we may be able to dispatch to the wrappers through a scalems.simulator
front end or something.

The idea would be that all wrappers return similar objects, so that they could be used
interchangeably by the rest of the tools.

All wrappers should have, to the extent possible, the same methods.
"""