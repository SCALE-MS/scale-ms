# Install the `scalems` package into a RP-ready container.
#
# Before building this image, build the `rp-complete` image from `rp-complete.dockerfile`
#     docker build -t rp-complete -f rp-complete.dockerfile .
#
# Usage:
#
# To build an image named `scalems-rp`, use the following Docker command while in this directory.
#     docker build -t scalems-rp -f scalems-rp.dockerfile ..
#
# The above command will use the parent directory as the "Docker build context" so that
# the git repository contents are avaialble to the docker build script.
#
# To run the `scalems` tests:
# 1. Launch the container (as root, so that the mongod can be started).
# 2. Wait a few seconds for the MongoDB service to start.
# 3. Exec the tests in the container.
#
#     docker run --rm --name rp_test -u root -d scalems-rp
#     # sleep 3
#     docker exec -ti rp_test rp-venv/bin/python -m pytest scalems/tests
#     docker kill rp_test

# Prerequisite: build base image from rp-complete.dockerfile
FROM rp-complete

USER rp
WORKDIR /home/rp

COPY --chown=rp:radical . scalems

RUN ./rp-venv/bin/pip install scalems/