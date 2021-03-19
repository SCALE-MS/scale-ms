# Summary:
# docker build -t scalems/ex_basic_gmxapi -f Dockerfile ../.. && docker run --rm -ti scalems/ex_basic_gmxapi bash -c 'cd $EXAMPLE && . ./env38/bin/activate && cd examples/basic_gmxapi && . ~/gromacs2021/bin/GMXRC && python -m scalems.local single_sim.py'
#
# Build with, e.g.
#     docker build -t scalems/ex_basic_gmxapi .
#
# Note that the image takes a while to build the first time. To try to use build
# cache from an existing image, try:
#     docker pull scalems/radicalpilot
#     docker pull scalems/ex_basic_gmxapi
#     docker build -t scalems/ex_basic_gmxapi --cache-from scalems/ex_basic_gmxapi ../..
#
# Example usage:
#     docker run --rm -ti scalems/ex_basic_gmxapi bash
#     $ . gromacs2021/bin/GMXRC
#     $ cd $EXAMPLE
#     $ . ./env38/bin/activate
#     $ cd examples/basic_gmxapi
#     $ python -m scalems.local single_sim.py
# Note that, because of the `--rm` flag, the container will be removed when you
# exit the containerized shell.

FROM scalems/radicalpilot

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-suggests --no-install-recommends \
        apt-utils \
        build-essential \
        cmake \
        git \
        libblas-dev \
        libfftw3-dev \
        liblapack-dev \
        libxml2-dev \
        make \
        ninja-build \
        software-properties-common \
        vim \
        wget \
        zlib1g-dev \
        && \
    rm -rf /var/lib/apt/lists/*

ENV EXAMPLE=/home/rp/scalems_example

RUN mkdir $EXAMPLE && \
    chown rp:radical $EXAMPLE

USER rp

# Note that Docker may not play nicely with rdtscp so we disable it.
RUN mkdir /tmp/gromacs && \
    cd /tmp/gromacs && \
    wget ftp://ftp.gromacs.org/gromacs/gromacs-2021.1.tar.gz && \
    tar xvf gromacs-2021.1.tar.gz && \
    mkdir build && \
    cd build && \
    cmake ../gromacs-2021.1 \
        -G Ninja \
        -DCMAKE_INSTALL_PREFIX=/home/rp/gromacs2021 \
        -DGMX_USE_RDTSCP=OFF \
        && \
    cmake --build . --target install && \
    cd && \
    rm -rf /tmp/gromacs

RUN cd $EXAMPLE && \
    python3 -m venv env38 && \
    . env38/bin/activate && \
    pip install --upgrade pip setuptools wheel

RUN cd $EXAMPLE && \
    . env38/bin/activate && \
    . ~/gromacs2021/bin/GMXRC && \
    pip install --upgrade gmxapi

ENV REF=gmxapi-example

RUN . $EXAMPLE/env38/bin/activate && \
    pip install --upgrade scalems@git+https://github.com/SCALE-MS/scale-ms.git@$REF

COPY --chown=rp:radical examples/basic_gmxapi/*.py $EXAMPLE/examples/basic_gmxapi/
COPY --chown=rp:radical testdata $EXAMPLE/testdata

CMD ["/usr/bin/bash"]
