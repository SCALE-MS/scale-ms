=========================
Sample work graph records
=========================

.. rubric:: Example

Simple simulation using :ref:`data sources <simulation input>` produced by previously defined work::

    {
        "version": "gmxapi_graph_0_2",
        "elements":
        {
            "<mdrun hash>":
            {
                "operation": ["gmxapi", "mdrun"],
                "input":
                {
                    "parameters": "<simulation_parameters_hash>",
                    "simulation_state": "<initial_state_hash>",
                    "microstate": "<initial_coordinates_hash>"
                },
                "output":
                {
                    "trajectory":
                    {
                        "meta": { "resource": { "type": "gmxapi.simulation.Trajectory",
                                                        "shape": [1] } }
                    },
                    "parameters": "gmxapi.Mapping"
                },
            }
        }
    }

.. admonition:: Question

    How do we handle the knowability of abstract outputs, such as
    "parameters" whose keys are unknown when a node is added, or whose value type
    and dimensionality cannot be known until runtime?

.. rubric:: Example

This example exercises more of the :ref:`grammar` in a complete, self-contained
graph.

Simulation reading inputs from the filesystem, with an attached restraint from a
pluggable extension module::

    {
        "version": "gmxapi_graph_0_2",
        "elements":
        {
            "<read_tpr hash>":
            {
                "label": "tpr_input",
                "operation": ["gmxapi", "read_tpr"],
                "input":
                {
                   "filename": ["topol.tpr"]
                },
                "output":
                {
                    "parameters": "gmxapi.Mapping",
                    "simulation_state": "gmxapi.simulation.SimulationState",
                    "microstate": { "meta": { "resource": { "type": "gmxapi.Float64",
                                                            "shape": [<N>, 6] } } }
               }
            }
            "<mdrun_hash>":
            {
                "label": "md_simulation_1",
                "operation": ["gmxapi", "mdrun"],
                "input":
                {
                    "parameters": "<read_tpr_hash>.parameters",
                    "simulation_state": "<read_tpr_hash>.simulation_state",
                    "microstate": "<read_tpr_hash>.microstate",
                    "potential": ["<ensemble_restraint_hash>.interface.potential"]
                }
            }
            "<ensemble_restraint_hash>":
            {
                "label": "ensemble_restraint_1",
                "operation": ["myplugin", "ensemble_restraint"],
                "input":
                {
                    "params": {<key-value pairs...>},
                },
                "interface":
                {
                    "potential":
                    {
                        "meta": { "resource": { "type": "gromacs.restraint",
                                                "shape": [1] } }
                    }
                }
            }
        }
    }

.. rubric:: Example

Illustrate the implementation of the command line wrapper.

The :py:mod:`gmxapi` Python package contains a helper :py:func:`gmxapi.commandline_operation`
that was implemented in terms of more strictly defined operations.
The :py:func:`gmxapi.commandline.cli` operation is aware only of an arbitrarily
long array of command line arguments. The wrapper script constructs the
necessary graph elements and data flow to give the user experience of files
being consumed and produced, though these files are handled in the framework
only as strings and string futures.

Graph node structure example::

    {
        "version": "gmxapi_graph_0_2",
        "elements":
        {
            "<filemap_hash1>": {
                "operation": ["gmxapi", "make_map"],
                "input": {
                    "-f": ["some_filename"],
                    "-t": ["filename1", "filename2"]
                },
                "output": {
                    "file": "gmxapi.Mapping"
                }
            },
            "<cli_op_hash1>": {
                "label": "exe1",
                "operation": ["gmxapi", "cli"],
                "input": {
                    "executable": ["some_executable"], # list length gives data edge width
                    "arguments": [],
                    "input_file_arguments": "<filemap_hash1>",
                },
                "output": {
                    "file": "gmxapi.Mapping"
                }
            },
            "<filemap_hash2>: {
                "label": "exe1_output_files",
                "operation": ["gmxapi", make_map"],
                "input": {
                    "-in1": "<cli_op_hash1>.output.file.-o",
                    "-in2": ["static_fileB"],
                    "-in3": ["arrayfile1", "arrayfile2"] # matches dimensionality of inputs
                }
            },
            "<cli_op_hash2>": {
                "label": "exe2",
                "namespace": "gmxapi",
                "operation": ["gmxapi", "commandline"],
                "input": {
                    "executable": [],
                    "arguments": [],
                    "input_file_arguments": "<filemap_hash2>"
                }
            }
        }
    }

.. rubric:: Example

Subgraph specification and use. Illustrate the toy example of the subgraph test.

The :py:mod:`gmxapi.test` module contains the following code::

    import gmxapi as gmx

    @gmx.function_wrapper(output={'data': float})
    def add_float(a: float, b: float) -> float:
        return a + b

    @gmx.function_wrapper(output={'data': bool})
    def less_than(lhs: float, rhs: float) -> bool:
        return lhs < rhs

    def test_subgraph_function():
        subgraph = gmx.subgraph(variables={'float_with_default': 1.0, 'bool_data': True})
        with subgraph:
            # Define the update for float_with_default to come from an add_float operation.
            subgraph.float_with_default = add_float(subgraph.float_with_default, 1.).output.data
            subgraph.bool_data = less_than(lhs=subgraph.float_with_default, rhs=6.).output.data
        operation_instance = subgraph()
        operation_instance.run()
        assert operation_instance.values['float_with_default'] == 2.

        loop = gmx.while_loop(operation=subgraph, condition=subgraph.bool_data)
        handle = loop()
        assert handle.output.float_with_default.result() == 6

This could be serialized with something like the following, by separating the
concrete primary work graph from the abstract graph defining the data flow in
the subgraph. Note that a subgraph description is a special case of the
description of a fused operation, which we may need to explore when considering
how Context implementations may support dispatching between environments that
warrant different sorts of optimizations.

.. note::

    We should also consider the Google "protocol buffer" and gRPC syntax and semantics.

::

    {
        "concrete_graph_<hash>":
        {
            "version": "gmxapi_graph_0_2",
            "elements":
            {
                "while_loop_<hash>":
                {
                    "namespace": "gmxapi",
                    "operation": "while_loop",
                    "input":
                    {
                        "operation": ".abstact_graph_<hash>"
                    },
                    "depends": [".abstract_graph_<hash>.interface.bool_data"],
                    "output":
                    {
                        "float_with_default": "gmxapi.Float64",
                        "bool_data": "gmxapi.Bool"
                    }
                }
            }
        "abstract_graph_<hash>":
            {
                "input":
                {
                    "float_with_default": 1.0,
                    "bool_data": True
                },
                "output":
                {
                    "float_with_default": "add_float_<hash>.output.data",
                    "bool_data": "less_than_<hash>.output.data"
                },
                "elements":
                {
                    "less_than_<hash>":
                    {
                        "namespace": "gmxapi.test",
                        "operation": "less_than",
                        "input":
                        {
                            "lhs": "add_float_<hash>.output.data",
                            "rhs": [[6.]]
                        },
                        "output":
                        {
                            "data": "gmxapi.Bool"
                        }
                    },
                    "add_float_<hash>":
                    {
                        "namespace": "gmxapi.test",
                        "operation": "add_float",
                        "input":
                        {
                            "a": ".abstract_graph_<hash>.float_with_default",
                            "b": [[1.]]
                        }
                        "output":
                        {
                            "data": "gmxapi.Float64"
                        }
                    }
                }
            }
        }
    }
