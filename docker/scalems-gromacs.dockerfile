# Extend base scalems testing image with a gromacs installation.
#
# Summary:
#     docker build -t scalems/rp-complete -f rp-complete.dockerfile .
#     docker build -t scalems/gromacs -f scalems-gromacs.dockerfile ..
#     docker run --rm -ti -u rp scalems/gromacs bash
#     # or
#     docker run --rm --name scalems_test -d scalems/gromacs
#
# Before building this image, pull or build the `rp-complete` image from `rp-complete.dockerfile`
#     docker build -t scalems/rp-complete -f rp-complete.dockerfile .
#
# Note that the image takes a while to build the first time. To try to use build
# cache from an existing image, try:
#     docker pull scalems/rp-complete
#     docker pull scalems/gromacs
#     docker build -t scalems/gromacs --cache-from scalems/gromacs -f scalems-gromacs.dockerfile ..
#
# Example usage---LAMMPS+Python only:
#     docker run --rm -ti -u rp scalems/gromacs bash
#     $ . ./rp-venv/bin/activate
#     $ $RPVENV/bin/lmp ...
#
# Example usage---GROMACS+Python only:
#     docker run --rm -ti -u rp scalems/gromacs bash
#     $ . ./rp-venv/bin/activate
#     $ python
#     >>> import gmxapi as gmx
#
# Example usage with RP availability:
# The mongodb server needs to be running, so start the container, wait for mongodb to start,
# and then launch a shell as an additional process.
#
# 1. Launch the container (as root, so that the mongod can be started).
# 2. Wait a few seconds for the MongoDB service to start.
# 3. Exec the tests in the container.
#
#     docker run --rm --name scalems_test -d scalems/gromacs
#     sleep 3
#     docker exec -ti -u rp scalems_test bash -c ". rp-venv/bin/activate && python -m pytest scalems/tests --rp-resource=local.localhost"
#     docker exec -ti -u rp scalems_test bash -c ". rp-venv/bin/activate && python -m scalems.radical --resource=local.localhost --venv /home/rp/rp-venv scalems/examples/basic/echo.py hi there"
#     docker exec -ti -u rp scalems_test bash -c 'cat 0*0/stdout'
#     docker kill scalems_test

# Prerequisite: build base image from rp-complete.dockerfile
ARG TAG=latest
FROM scalems/rp-complete:$TAG

USER root

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-suggests --no-install-recommends \
        apt-utils \
        build-essential \
        cmake \
        libfftw3-dev \
        git \
        libopenmpi-dev \
        make \
        ninja-build \
        pkg-config \
        software-properties-common \
        vim \
        wget \
        && \
    rm -rf /var/lib/apt/lists/*

USER rp

WORKDIR /home/rp

RUN $RPVENV/bin/pip install --upgrade pip setuptools wheel
RUN $RPVENV/bin/pip install --upgrade cmake
RUN $RPVENV/bin/pip install mpi4py

ARG BRANCH=release-2022
RUN . $RPVENV/bin/activate && \
    cd $HOME && \
        git clone \
            --depth=1 \
            -b $BRANCH \
            https://gitlab.com/gromacs/gromacs.git \
            gromacs-src && \
        cd gromacs-src && \
            pwd && \
            rm -rf build && \
            mkdir build && \
            cd build && \
                cmake -G Ninja \
                    -DGMX_THREAD_MPI=ON \
                    -DGMX_INSTALL_LEGACY_API=ON \
                    -DGMX_USE_RDTSCP=OFF \
                    -DCMAKE_INSTALL_PREFIX=$RPVENV/gromacs \
                    .. && \
                cmake --build . --target install

RUN $RPVENV/bin/pip install -r $HOME/gromacs-src/python_packaging/requirements-test.txt

ARG GMXAPI_REF="gmxapi"
RUN . $RPVENV/gromacs/bin/GMXRC && $RPVENV/bin/pip install $GMXAPI_REF

COPY --chown=rp:radical requirements-testing.txt scalems/requirements-testing.txt
# Note: RCT stack may not install correctly unless venv is actually "activated".
RUN . $RPVENV/bin/activate && pip install --upgrade -r scalems/requirements-testing.txt

COPY --chown=rp:radical . scalems
RUN $RPVENV/bin/pip install --no-deps scalems/

# Try to update the testdata submodule if it is missing or out of date.
# If there are files in testdata, but it is not tracked as a git submodule,
# then we should not overwrite it. Unfortunately, I don't think there is really
# a way for us to report this condition during docker build.
# (The `echo` below will not be seen on the terminal.)
RUN cd scalems && \
    git submodule update --init --merge || \
        echo "testdata has untracked changes. Skipping submodule update."

# The current rp and scalems packages should now be available to the rp user in /home/rp/rp-venv

#ENV REF=master
#
#RUN . $EXAMPLE/env38/bin/activate && \
#    pip install --upgrade scalems@git+https://github.com/SCALE-MS/scale-ms.git@$REF

#COPY --chown=rp:radical examples/basic_gmxapi/*.py $EXAMPLE/examples/basic_gmxapi/
#COPY --chown=rp:radical testdata $EXAMPLE/testdata

# Restore the user for the default entry point (the mongodb server)
USER mongodb