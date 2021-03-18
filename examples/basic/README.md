# Basic examples

Scripts in this directory can be helpful to test basic interaction with the
scalems package and to test different execution modes.

These scripts assume that the scalems package is already installed.

For a clean test environment (or if there are any problems getting scalems installed),
consider Docker. Refer to the [Dockerfile] contents for usage help.

Example:

    docker build -t example .
    docker run --rm -ti ex1 bash -c \
        'cd $EXAMPLE && . env38/bin/activate && python -m scalems.local echo.py hi there && ls * && cat */stdout'
    docker run --rm -ti example bash -c \
        'cd $EXAMPLE && . env38/bin/activate && python echo_detail.py hi there && ls */*txt'
