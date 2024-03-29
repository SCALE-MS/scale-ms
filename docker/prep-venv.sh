# Source this scriptlet to set up a fresh 'login' service container.
#
# Note that this script assumes you have already copied the repository to
# /tmp/scalems_dev in the docker container.
# E.g.
#     docker compose -f docker/docker-compose.yml cp . login:/tmp/scalems_dev
#     docker compose -f docker/docker-compose.yml exec -u root login bash -c "chown -R rp:radical /tmp/scalems_dev"
#
# Add a local private/public key to container authorized keys
keyfile=${1:-${HOME}/.ssh/id_rsa}
pubkeyfile="${keyfile}.pub"
cat $pubkeyfile | ssh -p 2345 rp@127.0.0.1 bash -c "cat - >> ~/.ssh/authorized_keys"

# Make sure the container host key is recognized.
# Note: Periodic cleanup of localhost:2345 becomes necessary.
# E.g. cp ~/.ssh/known_hosts ~/.ssh/known_hosts.old && grep -v '\[127.0.0.1\]:2345' ~/.ssh/known_hosts.old > ~/.ssh/known_hosts && chmod 700 ~/.ssh/known_hosts
hostkey=$(ssh-keyscan -p 2345 127.0.0.1)
grep -q "$hostkey" ~/.ssh/known_hosts || echo $hostkey >> ~/.ssh/known_hosts

# Pre-authenticate for subsequent ssh access.
eval "$(ssh-agent -s)"
ssh-add $keyfile

# Log in to container and install software.
ssh -p 2345 rp@127.0.0.1 '. /home/rp/rp-venv/bin/activate ; \
pip install --upgrade pip setuptools wheel pydevd-pycharm && \
pip uninstall -y radical.pilot radical.saga radical.utils && \
pip install -r /tmp/scalems_dev/requirements-testing.txt && \
pip install -e /tmp/scalems_dev'

export RADICAL_PILOT_DBURL="mongodb://root:password@127.0.0.1:27017/admin"
