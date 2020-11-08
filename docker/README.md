# Docker recipes

## SCALE-MS testing

Full testing of the `scalems` Python package requires dispatching
work through an execution manager. We use RADICAL Pilot.

The RP environment is non-trivial to set up, so we use a containerized
set of services. For simplicity, the RP-based SCALE-MS testing occurs
in a Docker container based on a RP container (see below).

A container with the `scalems` package and test scripts is built from
`scalems-rp.dockerfile`. Refer to comments in that file for details.

## RADICAL Pilot environment

Containerized RADICAL Pilot testing can be performed two ways.
Either the (required) MongoDB server can run in the same container
as RP, or in a separate service container.

## Monolithic container

`rp-complete.dockerfile` provides a recipe for a complete container
to run MongoDB and the RP stack. Note that the MongoDB instance can
take a few seconds to start up. The easiest way to use the container
is to `run` the container's default mongod service, wait a few moments,
and then `exec` RP scripts or a shell in the container. Refer to the
comments in the file for more information.

## Docker Compose stack

`stack.yml` provides a recipe for `docker-compose` (or more elaborate
container-based service cluster). The stack relies on two public
container images (`mongo:bionic` for the database service and
`mongo-express` for a database admin console) and a custom image
(to be built locally) providing the sshd service and login environment.
See `radicalpilot.dockerfile` for instructions on
building the (required) `radicalpilot` container image.

One service container is launched with each of these three images.
Services in the stack are named *mongo*, *mongo-express*, and *login*, respectively.
By default, the resulting container names are prefixed by `docker_` and suffixed
with monotonic integers. Thus, the container of interest for running
RP scripts will be `docker_login_1`.

See `stack.yml` for more information.
