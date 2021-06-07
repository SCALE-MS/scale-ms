# Extend base scalems testing image with a lammps installation.
#
# Summary:
#     docker build -t scalems/lammps -f scalems-lammps.dockerfile ../..
#     docker run --rm -ti scalems/lammps bash
#
# Note that the image takes a while to build the first time. To try to use build
# cache from an existing image, try:
#     docker pull scalems/radicalpilot
#     docker pull scalems/lammps
#     docker build -t scalems/lammps --cache-from scalems/lammps ../..
#
# Example usage:
#     docker run --rm -ti scalems/lammps bash
#     $ . ./rp-venv/bin/activate
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
        -DPKG_COMPRESS=yes \
        && \
    cmake --build . && \
    cmake --build . --target install && \
    rm -rf /tmp/lammps

#ENV REF=master
#
#RUN . $EXAMPLE/env38/bin/activate && \
#    pip install --upgrade scalems@git+https://github.com/SCALE-MS/scale-ms.git@$REF

#COPY --chown=rp:radical examples/basic_gmxapi/*.py $EXAMPLE/examples/basic_gmxapi/
#COPY --chown=rp:radical testdata $EXAMPLE/testdata

