# Extend base scalems testing image with a lammps installation.
#
# Summary:
#     docker build -t scalems/lammps -f scalems-lammps.dockerfile ..
#     docker run --rm -ti scalems/lammps bash
#
# Before building this image, pull or build the `rp-complete` image from `rp-complete.dockerfile`
#     docker build -t scalems/rp-complete -f rp-complete.dockerfile .
#
# Note that the image takes a while to build the first time. To try to use build
# cache from an existing image, try:
#     docker pull scalems/rp-complete
#     docker pull scalems/lammps
#     docker build -t scalems/lammps --cache-from scalems/lammps -f scalems-lammps.dockerfile ..
#
# Example usage (simple):
#     docker run --rm -ti scalems/lammps bash
#     $ . ./rp-venv/bin/activate
#     $ $HOME/.local/bin/lmp ...
#
# Example usage with RP availability:
# The mongodb server needs to be running, so start the container, wait for mongodb to start,
# and then launch a shell as an additional process.
#
# 1. Launch the container (as root, so that the mongod can be started).
# 2. Wait a few seconds for the MongoDB service to start.
# 3. Exec the tests in the container.
#
#     docker run --rm --name scalems_test -u root -d scalems/lammps
#     sleep 3
#     docker exec -ti scalems_test bash -c ". rp-venv/bin/activate && python -m pytest scalems/tests --rp-resource=local.localhost"
#     docker exec -ti scalems_test bash -c ". rp-venv/bin/activate && python -m scalems.radical --resource=local.localhost --venv /home/rp/rp-venv scalems/examples/basic/echo.py hi there"
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

RUN /home/rp/rp-venv/bin/pip install --upgrade pip setuptools wheel
RUN /home/rp/rp-venv/bin/pip install --upgrade cmake

# Patch release will have a path like lammps-27May2021
RUN . $HOME/rp-venv/bin/activate && \
    mkdir /tmp/lammps && \
    cd /tmp/lammps && \
    wget https://download.lammps.org/tars/lammps.tar.gz && \
    tar xvf lammps.tar.gz && \
    cd lammps-* && \
    mkdir build && \
    cd build && \
    cmake ../cmake -G Ninja \
        -DPKG_KSPACE=yes \
        -DPKG_MOLECULE=yes \
        -DPKG_MPIIO=yes \
        -DPKG_PYTHON=yes \
        -DPKG_REPLICA=yes \
        -DPKG_MISC=yes \
        -DPKG_GPU=yes \
	-DPKG_EXTRA-DUMP=yes \
        -DPKG_COMPRESS=yes \
	-DBUILD_SHARED_LIBS=on \
	-DLAMMPS_EXCEPTIONS=on \
	-DCMAKE_INSTALL_PREFIX=$VIRTUAL_ENV \
        && \
    cmake --build . && \
    cmake --build . --target install && \
    rm -rf /tmp/lammps && \
    echo 'export LD_LIBRARY_PATH=$VIRTUAL_ENV/lib:$LD_LIBRARY_PATH' >> $HOME/rp-venv/bin/activate

COPY --chown=rp:radical . scalems

RUN . $HOME/rp-venv/bin/activate && ./rp-venv/bin/pip install --upgrade -r scalems/requirements-testing.txt
RUN . $HOME/rp-venv/bin/activate && ./rp-venv/bin/pip install scalems/
RUN . $HOME/rp-venv/bin/activate && ./rp-venv/bin/pip install mpi4py

# The current rp and scalems packages should now be available to the rp user in /home/rp/rp-venv

#ENV REF=master
#
#RUN . $EXAMPLE/env38/bin/activate && \
#    pip install --upgrade scalems@git+https://github.com/SCALE-MS/scale-ms.git@$REF

#COPY --chown=rp:radical examples/basic_gmxapi/*.py $EXAMPLE/examples/basic_gmxapi/
#COPY --chown=rp:radical testdata $EXAMPLE/testdata

