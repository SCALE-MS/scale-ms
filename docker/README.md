# Docker recipes

## SCALE-MS testing

Full testing of the `scalems` Python package requires dispatching
work through an execution manager. We use RADICAL Pilot.

The RP environment is non-trivial to set up, so we use a containerized
set of services. For simplicity, the RP-based SCALE-MS testing occurs
in a Docker container based on a RP container (see below).

A container with the `scalems` package and test scripts is built from
`scalems-rp.dockerfile`. Refer to comments in [that file](scalems-rp.dockerfile) for details.

## RADICAL Pilot environment

Containerized RADICAL Pilot testing can be performed two ways.
Either the (required) MongoDB server can run in the same container
as RP, or in a separate service container.

The `scalems/radicalpilot` image includes additional resource definitions
`local.github`, `local.tunnel`, `docker.login`, and `docker.compute`.
See the `resource_*.json` files in this directory for details.

## Monolithic container

`rp-complete.dockerfile` provides a recipe for a complete container
to run MongoDB and the RP stack. Note that the MongoDB instance can
take a few seconds to start up. The easiest way to use the container
is to `run` the container's default mongod service, wait a few moments,
and then `exec` RP scripts or a shell in the container. Refer to the
comments in [rp-complete.dockerfile](rp-complete.dockerfile) for more information.

## Docker Compose stack

`compose.yml` provides a recipe for `docker-compose` (or more elaborate
container-based service cluster). The stack relies on two public
container images (`mongo:bionic` for the database service and, optionally,
`mongo-express` for a database admin console) and a custom image
(to be built locally) providing the sshd service and login environment.
See `radicalpilot.dockerfile` for instructions on
building the (required) `radicalpilot` container image.

In the simplest case, one service container each is launched for the services
*mongo* (`mongo:bionic`),
*mongo-express* (`mongo-express` image),
*login* (`scalems/radicalpilot`), and
*compute* (`scalems/radicalpilot`).
Unlike with the `docker run` or `docker start` commands,
`docker compose` commands use the service names defined in the YAML configuration
file to interact with the launched containers.
(It is somewhat antithetical to think of individual containers as having persistent
identities with "swarms" and such.)
E.g.

    docker compose up
    docker compose exec -u rp login bash
    docker compose down

See [docker-compose.yml](docker-compose.yml) for more information.
