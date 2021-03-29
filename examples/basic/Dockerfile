# Summary:
# docker build -t scalems/ex_basic . && \
#    docker run --rm -ti ex_basic bash -c \
#        'cd $EXAMPLE && \
#        . env38/bin/activate && \
#        python -m scalems.local echo.py hi there && \
#        ls * && cat */stdout'
#
# By default, the Docker image is built with scalems from the `master` branch.
# Specify the REF build argument for a specific branch or commit ID.
# E.g. Build with
#     docker build -t ex_basic --build-arg REF=74f20c26b1e453c958cdc678b2155da0f8d3f184 .
#
# The example script(s) is/are always copied from the `docker build` context
# (the directory specified by the final positional argument on the `docker build` command line).
#
# Run with
#     docker run --rm -ti ex_basic bash
#     $ cd $EXAMPLE
#     $ . ./env38/bin/activate
#     $ python -m scalems.local echo.py hi there
# Note that, because of the `--rm` flag, the container will be removed when you
# exit the containerized shell.

FROM scalems/radicalpilot

ENV EXAMPLE=/home/rp/scalems_example

RUN mkdir $EXAMPLE && \
    chown rp:radical $EXAMPLE

USER rp

RUN cd $EXAMPLE && \
    python3 -m venv env38 && \
    . env38/bin/activate && \
    pip install --upgrade pip setuptools wheel

ARG REF=master

RUN . $EXAMPLE/env38/bin/activate && \
    pip install --upgrade scalems@git+https://github.com/SCALE-MS/scale-ms.git@$REF

COPY --chown=rp:radical *.py $EXAMPLE/

# Alternatively, get a specific version of the script:
#RUN cd $EXAMPLE && \
#    wget https://raw.githubusercontent.com/SCALE-MS/scale-ms/$REF/examples/basic/echo.py

CMD ["/usr/bin/bash"]
