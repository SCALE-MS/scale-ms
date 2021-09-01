# Source this scriptlet to set up a fresh 'login' service container.
#
# Add a local private/public key to container authorized keys
keyfile=${1:-${HOME}/.ssh/id_rsa}
pubkeyfile="${keyfile}.pub"
cat $pubkeyfile | ssh -p 2345 rp@127.0.0.1 bash -c "cat - >> ~/.ssh/authorized_keys"

# Make sure the container host key is recognized.
# Note: Periodic cleanup of localhost:2345 becomes necessary.
# E.g. grep -v '\[localhost\]:2345' ~/.ssh/known_hosts.old > ~/.ssh/known_hosts && chmod 500 ~/.ssh/known_hosts
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
