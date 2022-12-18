# This Dockerfile is used to create an image for the "radicalpilot" service in
# the docker-compose.yml docker-compose file in this directory.
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
        iputils-ping \
        language-pack-en \
        locales \
        && \
    rm -rf /var/lib/apt/lists/*

RUN locale-gen en_US.UTF-8 && \
    update-locale LANG=en_US.UTF-8

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
        gcc \
        git \
        libopenmpi-dev \
        openmpi-bin \
        openssh-server \
        rsync \
        vim \
        wget && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get -y --no-install-recommends install \
        libmpich-dev && \
    rm -rf /var/lib/apt/lists/*

# Note that mpic++ can be configured with `update-alternatives` for openmpi or mpich.
# See https://stackoverflow.com/a/66538359/5351807
# The mpic++ (both mpic++.openmpi and mpic++.mpich) uses g++ by default
# (and so is not affected by alternatives for "c++").
# mpic++.openmpi can be redirected with environment variables (e.g. OMPI_CXX=clang++ mpic++ ...).
# It is not clear whether mpic++.mpich can be similarly configured, and it might be better
# to do fancy tool chain manipulation through Spack instead of the Ubuntu system tools.

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive \
    apt-get install -y \
        python3.8-dev \
        python3.9-dev \
        python3.8-venv \
        python3.9-venv \
        python-dev-is-python3 \
        tox && \
    rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 8
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 9

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

RUN  mkdir -p ~/.radical/pilot/configs

COPY --chown=rp:radical resource_docker.json /home/rp/.radical/pilot/configs

WORKDIR /home/rp

# If using the PyCharm debug server, install the pydevd-pycharm package corresponding to the IDE version,
# and set up the IDE to sync the project with an agreed-upon container directory: `/tmp/pycharm_scalems`
#RUN . ~rp/rp-venv/bin/activate && \
#    pip install --no-cache-dir pydevd-pycharm
# The above is too hard to maintain. Follow the instructions from JetBrains or perform
# the above commented-out pip install manually and hope for the best.
# I most recently found the installed egg distribution in
# $HOME/Library/Application\ Support/JetBrains/Toolbox/apps/PyCharm-P/ch-0/211.7142.13/PyCharm.app/Contents/debug-eggs/
RUN mkdir /tmp/scalems_dev

#RUN ~rp/rp-venv/bin/python -m pip install \
#    --no-cache-dir --no-build-isolation --upgrade 'radical.saga==1.6.6' 'radical.utils==1.6.6'
#ARG RPREF="devel"
#RUN . ~rp/rp-venv/bin/activate && \
#    pip install --no-cache-dir --no-build-isolation --upgrade "git+https://github.com/radical-cybertools/radical.pilot.git@${RPREF}#egg=radical.pilot"
#RUN ~rp/rp-venv/bin/python -m pip install \
#    --no-cache-dir --no-build-isolation --upgrade "git+https://github.com/radical-cybertools/radical.pilot.git@scalems/stable#egg=radical.pilot"
RUN ~rp/rp-venv/bin/python -m pip install 'radical.pilot>=1.20'

# WARNING!!! Security risk!
# Allow rp user to trivially ssh into containers created from this image.
RUN mkdir ~rp/.ssh && \
    ssh-keygen -f ~rp/.ssh/id_rsa -t rsa -N '' && \
    cp ~rp/.ssh/id_rsa.pub ~/.ssh/authorized_keys && \
    cat /etc/ssh/ssh_host_ecdsa_key.pub | awk '{print "localhost " $1 " " $2}' > ~/.ssh/known_hosts && \
    cat /etc/ssh/ssh_host_ecdsa_key.pub | awk '{print "compute " $1 " " $2}' >> ~/.ssh/known_hosts && \
    cat /etc/ssh/ssh_host_ecdsa_key.pub | awk '{print "login " $1 " " $2}' >> ~/.ssh/known_hosts

# Ending the Dockerfile with a default CMD run as root triggers a warning
# with Dockerfile linters, but is necessary to start the sshd.
USER root

# Set the environment variable that Radical Pilot uses to find its MongoDB instance.
# Radical Pilot assumes the user is defined in the same database as in the URL.
# The Docker entry point creates users in the "admin" database, so we can just
# tell RP to use the same. The username and password are configured in the env
# passed to the mongo container in docker-compose.yml. The service name from docker-compose.yml
# also determines the URL host name.
# Note that the default mongodb port number is 27017.
ENV RADICAL_PILOT_DBURL="mongodb://root:password@mongo:27017/admin"

RUN echo "export RADICAL_PILOT_DBURL=$RADICAL_PILOT_DBURL" >> /etc/profile

# Set user "rp" password to "rp".
RUN echo "rp\nrp" | passwd rp
