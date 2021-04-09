# This Dockerfile is used to create an image for the "radicalpilot" service in
# the stack.yml docker-compose file in this directory.
# When a container is launched from this image with no arguments, the container
# will run an sshd daemon.
# Example:
#     docker build -t scalems/radicalpilot -f radicalpilot.dockerfile .

FROM ubuntu:focal

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
        vim && \
    rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8 && \
    update-locale LANG=en_US.UTF-8


RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# Reference https://docs.docker.com/engine/examples/running_ssh_service/
RUN mkdir /var/run/sshd

# SSH login fix. Otherwise user is kicked off after login
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

ENV NOTVISIBLE "in users profile"
RUN echo "export VISIBLE=now" >> /etc/profile

EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]


RUN groupadd radical && useradd -g radical -s /bin/bash -m rp
USER rp

WORKDIR /home/rp
RUN python3 -m venv rp-venv

RUN rp-venv/bin/pip install --no-cache-dir --upgrade \
        pip \
        setuptools \
        wheel && \
    rp-venv/bin/pip install --no-cache-dir --upgrade \
        build \
        coverage \
        flake8 \
        'mock==2.0.0' \
        netifaces \
        ntplib \
        pylint \
        pymongo \
        pytest \
        pytest-asyncio \
        python-hostlist \
        setproctitle

RUN . ~rp/rp-venv/bin/activate && \
    pip install --no-cache-dir --upgrade \
        'radical.saga>=1.5.2' \
        'radical.utils>=1.5.2'

RUN  mkdir -p ~/.radical/pilot/configs

COPY --chown=rp:radical resource_local.json /home/rp/.radical/pilot/configs

ARG RPREF="project/scalems"

# Note: radical.pilot does not work properly with an "editable install"
#RUN (cd ~rp && \
#    . ~rp/rp-venv/bin/activate && \
#    git clone -b $RPREF --depth=3 https://github.com/radical-cybertools/radical.pilot.git && \
#    cd radical.pilot && \
#    pip install -e . \
#    )

WORKDIR /home/rp
RUN . ~rp/rp-venv/bin/activate && \
    pip uninstall -y radical.pilot && \
    pip install --no-cache-dir --upgrade "git+https://github.com/radical-cybertools/radical.pilot.git@${RPREF}#egg=radical.pilot"

# WARNING!!! Security risk!
# Allow rp user to trivially ssh into containers created from this image.
RUN mkdir ~rp/.ssh && \
    ssh-keygen -f ~rp/.ssh/id_rsa -t rsa -N '' && \
    cp ~rp/.ssh/id_rsa.pub ~/.ssh/authorized_keys && \
    cat /etc/ssh/ssh_host_ecdsa_key.pub | awk '{print "localhost " $1 " " $2}' > ~/.ssh/known_hosts && \
    cat /etc/ssh/ssh_host_ecdsa_key.pub | awk '{print "compute " $1 " " $2}' >> ~/.ssh/known_hosts && \
    cat /etc/ssh/ssh_host_ecdsa_key.pub | awk '{print "login " $1 " " $2}' >> ~/.ssh/known_hosts

# If using the PyCharm debug server, install the pydevd-pycharm package corresponding to the IDE version,
# and set up the IDE to sync the project with an agreed-upon container directory: `/tmp/pycharm_scalems`
ARG PYCHARM=211.6693.115
RUN . ~rp/rp-venv/bin/activate && \
    pip install --no-cache-dir pydevd-pycharm~=$PYCHARM && \
    mkdir /tmp/pycharm_scalems


USER root

# Set the environment variable that Radical Pilot uses to find its MongoDB instance.
# Radical Pilot assumes the user is defined in the same database as in the URL.
# The Docker entry point creates users in the "admin" database, so we can just
# tell RP to use the same. The username and password are configured in the env
# passed to the mongo container in stack.yml. The service name from stack.yml
# also determines the URL host name.
# Note that the default mongodb port number is 27017.
ENV RADICAL_PILOT_DBURL="mongodb://root:password@mongo:27017/admin"

RUN echo "export RADICAL_PILOT_DBURL=$RADICAL_PILOT_DBURL" >> /etc/profile

# Set user "rp" password to "rp".
RUN echo "rp\nrp" | passwd rp

USER mongodb