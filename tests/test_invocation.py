from scalems.invocation import get_backend_tuple


def test_get_backend():
    for backend_name in ('scalems.local', 'scalems.radical'):
        module_name, module = get_backend_tuple(backend_name)
        assert module_name == backend_name
        assert module.__name__ == module_name
    del backend_name, module, module_name

    import scalems.local
    import scalems.radical

    for backend_module in (scalems.local, scalems.radical):
        module_name, module = get_backend_tuple(backend_module)
        assert module is backend_module
        assert module_name == backend_module.__name__
