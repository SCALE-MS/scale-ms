# When a container is launched from this image with no arguments,
# the entry point script will launch and initialize a MongoDB instance.
# It will take a few seconds to be ready for connections, after which
# radical.pilot will be able to use pymongo to connect.
# Example:
#     docker build -t scalems/rp-complete -f rp-complete.dockerfile .
#     docker run --rm --name rp_test -d scalems/rp-complete
#     # Either use '-d' with 'run' or issue the 'exec' in a separate terminal
#     # after waiting a few seconds for the DB to be ready to accept connections.
#     docker exec -ti -u rp rp_test bash -c "cd ~/radical.pilot && ~/rp-venv/bin/python -m pytest tests"
#     # The examples need the venv to be activated in order to find supporting
#     # shell scripts on the default PATH. The current working directory also
#     # needs to be writable.
#     docker exec -ti -u rp rp_test bash -c "cd && . /home/rp/rp-venv/bin/activate && python radical.pilot/examples/00*"
#     # If '-d' was used with 'run', you can just kill the container when done.
#     docker kill rp_test
#
# Optional: Specify a git ref for radical.pilot when building the image with the RPREF build arg. (Default v1.5.7)
#     docker build -t scalems/rp-complete -f rp-complete.dockerfile --build-arg RPREF=master .
#

FROM mongo:bionic
# Reference https://github.com/docker-library/mongo/blob/master/4.2/Dockerfile

USER root

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get -yq --no-install-suggests --no-install-recommends install apt-utils build-essential software-properties-common && \
    apt-get install -y --no-install-recommends \
        curl \
        dnsutils \
        gcc \
        git \
        iputils-ping \
        language-pack-en \
        locales \
        openmpi-bin \
        openssh-server \
        python3.8-dev \
        python3-venv \
        tox \
        vim \
        wget && \
    rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8 && \
    update-locale LANG=en_US.UTF-8

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
        python3.8-venv \
         && \
    rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

RUN groupadd radical && useradd -g radical -s /bin/bash -m rp

USER rp

WORKDIR /home/rp

RUN python3.8 -m venv rp-venv

RUN rp-venv/bin/pip install --upgrade \
        pip \
        setuptools \
        wheel && \
    rp-venv/bin/pip install --upgrade \
        coverage \
        flake8 \
        'mock==2.0.0' \
        mpi \
        netifaces \
        ntplib \
        pylint \
        pymongo \
        pytest \
        pytest-asyncio \
        python-hostlist \
        setproctitle

# Get repository for example and test files and to simplify RPREF build argument.
# Note that GitHub may have a source directory name suffix that does not exactly
# match the branch or tag name, so we use a glob to try to normalize the name.
ARG RPREF="v1.6.6"
#ARG RPREF="project/scalems"
# Note: radical.pilot does not work properly with an "editable install"
RUN git clone -b $RPREF --depth=3 https://github.com/radical-cybertools/radical.pilot.git && \
    . ~rp/rp-venv/bin/activate && \
    cd ~rp/radical.pilot && \
    pip install --no-cache-dir --no-build-isolation .


# Allow RADICAL Pilot to provide more useful behavior during testing,
# such as mocking missing resources from the resource specification.
ENV RADICAL_DEBUG="True"
RUN echo export RADICAL_DEBUG=$RADICAL_DEBUG >> ~rp/.profile

USER root

# Note that the following environment variables have special meaning to the
# `mongo` Docker container entry point script.
ENV MONGO_INITDB_ROOT_USERNAME=root
ENV MONGO_INITDB_ROOT_PASSWORD=password

# Set the environment variable that Radical Pilot uses to find its MongoDB instance.
# Radical Pilot assumes the user is defined in the same database as in the URL.
# The Docker entry point creates users in the "admin" database, so we can just
# tell RP to use the same.
# Note that the default mongodb port number is 27017.
ENV RADICAL_PILOT_DBURL="mongodb://$MONGO_INITDB_ROOT_USERNAME:$MONGO_INITDB_ROOT_PASSWORD@localhost:27017/admin"

RUN echo "export RADICAL_PILOT_DBURL=$RADICAL_PILOT_DBURL" >> /etc/profile

# Set user "rp" password to "rp".
RUN echo "rp\nrp" | passwd rp

USER mongodb
