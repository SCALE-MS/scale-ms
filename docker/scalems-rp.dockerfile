# Install the `scalems` package into a RP-ready container.
#
# Before building this image, pull or build the `rp-complete` image from `rp-complete.dockerfile`
#     docker build -t scalems/rp-complete -f rp-complete.dockerfile .
#
# Usage:
#
# To build an image named `scalems-rp`, use the following Docker command while in this directory.
#     docker build -t scalems/scalems-rp -f scalems-rp.dockerfile ..
#
# The above command will use the parent directory as the "Docker build context" so that
# the git repository contents are avaialble to the docker build script.
#
# Warning: The `mongo:focal` base image sets the HOME environment variable to /data/db for its own purposes,
#    which breaks the meaning of `~` when executing with `-u rp`. Explicitly use `~rp` or use `--env HOME=/home/rp`
#    in the `docker exec` command line.
#
# To run the `scalems` tests:
# 1. Launch the container (as root, so that the mongod can be started).
# 2. Wait a few seconds for the MongoDB service to start.
# 3. Exec the tests in the container.
#
#     docker run --rm --name scalems_test -u root -d scalems/scalems-rp
#     sleep 3
#     docker exec -ti -u rp -e HOME=/home/rp scalems_test bash -c ". rp-venv/bin/activate && python -m pytest scalems/tests -s"
#     docker kill scalems_test

# Prerequisite: build base image from rp-complete.dockerfile
ARG TAG=latest
FROM scalems/rp-complete:$TAG

USER rp
WORKDIR /home/rp

RUN HOME=/home/rp ./rp-venv/bin/pip install --no-cache-dir --upgrade pip setuptools

COPY --chown=rp:radical . scalems

RUN HOME=/home/rp ./rp-venv/bin/pip install --no-cache-dir --upgrade -r scalems/requirements-testing.txt
RUN HOME=/home/rp ./rp-venv/bin/pip install --no-cache-dir scalems/
# The current rp and scalems packages should now be available to the rp user in /home/rp/rp-venv

USER mongodb
