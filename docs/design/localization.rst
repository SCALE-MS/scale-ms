==================================
Managing locality and localization
==================================

A core goal of the project is to optimize remote execution of dependent tasks by
reducing unnecessary data transfers. This is to be achieved in two principle ways:
* get the data and the computation to the same place efficiently
* only transfer the data to where it is actually needed

We want to move logic to where the data is, when possible, rather than transfer
data to central locations to evaluate control flow.

We also want to use our knowledge of the place(s) that data exists to let tasks
get their input in the most direct way.

When data transfers *are* necessary (between sites or between the client and execution site),
we want to perform them asynchronously, opportunistically, early, and (preferably)
without spending wall time from our Pilot job allocation.

The challenge
=============

We want a universal file object reference that encapsulates or can be correlated
with information on where the object actually exists, where it is needed, how to
get the file to where it is needed, and how to represent it when it has been
localized.

We can initially focus on the common and simple cas of HPC filesystems that are
accessible via ``sftp``, and which should be fairly well supported by the RCT stack.

We need to describe the utilities available for interacting with RP data staging
representations, Task working directories, and RCT filesystem interfaces.

.. seealso::

    Notes from the following documents should probably be assimilated here.

    * :issue:`129`
    * :issue:`75`
    * `Using rp.misc.util.get_resource_fs_url to construct staging directives or path arguments for Tasks. #2723 <https://github.com/radical-cybertools/radical.pilot/issues/2723>`__
    * `Offline read access to resource definition. #2668 <https://github.com/radical-cybertools/radical.pilot/issues/2668>`__
    * `Request for clearer behavior/documentation in *stage_in* directive targets. #2590 <https://github.com/radical-cybertools/radical.pilot/issues/2590>`__
    * `machconf documentation inaccurate / incomplete #2588 <https://github.com/radical-cybertools/radical.pilot/issues/2588>`__

Scenarios
=========

We will need to record additional information and (probably) impose some constraints
on the ways we construct staging directives in order to robustly track files
between tasks and across sessions.

Find the actual path of the filesystem endpoint
-----------------------------------------------

Note that `radical.saga.filesystem.Directory` has several layers of base classes
but the inherited method documentation is not duplicated.

.. code-block:: python

    from radical.pilot.utils.misc import get_resource_fs_url
    import radical.saga as rs
    rs.filesystem.Directory(get_resource_fs_url(...)).get_cwd()

In practice, ``get_cwd()`` might not be available.

However, the filesystem endpoint is only used to determine an access method,
and the path behavior is usually not relevant. In practice, derived paths are
usually representable by equating the path component of a URL with the absolute
path in the filesystem associated with a connection through the job endpoint.

.. seealso::

    * `radical.pilot.utils.misc.get_resource_fs_url`
    * https://github.com/radical-cybertools/radical.pilot/issues/2668#issuecomment-1236184810
    * https://github.com/radical-cybertools/radical.pilot/issues/2668#issuecomment-1239956265

Determine the actual filesystem paths of Tasks
----------------------------------------------

The Task working directory is the :py:attr:`radical.pilot.Task.sandbox`.
``task:///`` URIs and relative paths can be interpreted with respect to
the sandbox path, which is determined during task submission on the client,
and which exists by the time the Task reaches **some state**.

.. warning:: Sandbox representation may be wrong for raptor tasks: https://github.com/radical-cybertools/radical.pilot/issues/2802

from a Task
^^^^^^^^^^^

.. code-block:: python

    this_task_path = os.getcwd()

Anything else would need to be provided by the client or raptor Master task,
or inferred in a hacky way by inspecting parent directories.

In particular, there is no way to know which RP Resource the Pilot is running under.

In general, Tasks do not have access to the RP Session under which they are running,
so they know less about other Tasks than the client
(with the exception of raptor Master tasks).

from the client
^^^^^^^^^^^^^^^

.. code-block:: python

    task_dir_url = radical.utils.Url(task.sandbox)
    assert task_dir_url.scheme == 'file'
    task_dir = task_dir_url.path
    # Same for `pilot.sandbox`

.. warning::

    RP casts absolute paths in staging directives to ``file:///`` URIs,
    which are always considered in the client filesystem. This is different
    than the treatment of relative paths, which are evaluated relative to the
    client working directory or the task working directory, depending on the
    staging directive and whether the path occurs in the ``source`` or
    ``target`` field.

    Andre thought we resolved this, and that ``endpoint:///`` is essentially implicit.
    `Issue 2848 <https://github.com/radical-cybertools/radical.pilot/issues/2848>`__ in RP.

Get a portable URI for an RP URI
--------------------------------

This is not guaranteed possible, depending on the form of the filesystem endpoint
and the configuration of services on the computing resource.
RP uses `radical.saga.filesystem.Directory` to navigate from the filesystem endpoint
to the Session working directory, which may not be representable in a derived URI.
In practice, URLs, like ``task:///``, ``pilot:///``, *etc*, are usually easily
converted to absolute paths that can be meaningfully applied as the *path*
component of a URL derived from the filesystem endpoint.

.. note:: Sandbox URLs are constructed from the ``filesystem_endpoint`` URL and
    ``default_workdir``, so we can anticipate the ``Url.scheme`` and expect it to stay consistent.

.. code-block:: python

    from radical.saga import Url

    task_dir_url = Url(task.sandbox)
    task_dir = task_dir_url.path
    resource = task.pilot.description.resource
    if task_dir_url.scheme == 'file':
        # Find a reproducible way to regain access to the path.
        try:
            url_base = get_resource_fs_url(resource, schema='ssh')
        except KeyError:
            logging.warning('sftp URL not available')
        task_dir_url = Url(url_base)
        task_dir_url.path = task_dir
    # if local.localhost or otherwise non-public: error. we can't know whether/how to reconnect.
    # The best we can do is record the RP resource that was used.
    routed = True
    try:
        hostname, aliaslist, ipaddrlist = socket.gethostbyname_ex(task_dir_url.host)
        if not any([ipaddress.ip_address(addr).is_global for addr in ipaddrlist]):
            routed = False
    except OSError as e:
        routed = False
    if not routed:
        logging.warning(f'{task_dir_url.host} for {resource} is not a publicly routed host.')
    # Note that RP defers the user and authentication details to external tools,
    # so we don't really know exactly what is needed to reestablish a connection.
    # This may not always be enough, but it is everything we can know
    {
        'resource': resource,
        'path': task_dir,
        'url': str(task_dir_url)
    }

Place a file and track it.
--------------------------

Take a local filesystem path.
Transfer the file to the execution site (in a directory known to be writable by the user),
and locally record the remote location.

For simplicity, it is reasonable to use a directory generated during a Session,
as long as we can resolve it independently of the Session.
Otherwise, we would need to independently evaluate both ``filesystem_endpoint``
and ``default_remote_workdir`` to find a path that we know is readable and
writable by an RP Session.

We can defer the responsibility to the user for pre-Session staging, as long as
we have an easy way for users to represent remote resources.
Sufficient representation should be (resource label, absolute path),
with the caveat that the resource label uniquely identifies the resource and
the definition is available in all relevant cases.
For instance, we *could* warn if the host (in the job or filesystem endpoint)
does not resolve to a public/routed IP address, but even this should only be a warning.

Example 1: Stage with a Task

.. code-block:: python

    src = pathlib.Path(my_file).resolve()
    id: bytes = scalems.file.describe_file(src).fingerprint()
    reference = (
        id,
        {
            'name': src.name,
            'site': socket.gethostname(),  # WARNING: this is not guaranteed resolvable or usable in any way.
            'path': str(src)
        }
        )
    task_description = rp.TaskDescription()
    ...
    task_description.stage_in.append(
        'source': str(src),
        'target': src.name,
        'action': rp.TRANSFER
    )
    task = task_manager.submit(task_description)
    reference.update({
        'site': task.pilot.description.resource,
        'path': Url(task.sandbox).path
    })

Example 2: Use a session-level file store.

.. code-block:: python

    ...

Confirm the file's existence and properties from a Task.

.. code-block:: python

    ...

Confirm the file's existence from the client.

.. code-block:: python

    ...

Get the filesystem path for use in a Task input.

.. code-block:: python

    ...

Transfer the file to another execution site, or back to the client.

NOTE: Likely requires multiple 2-factor auths, for which we don't have a good
scheme to automate.

.. code-block:: python

    ...

Refer to one Task's file from another Task
------------------------------------------
in Python
^^^^^^^^^

During a RP Session, it may be more convenient to use RP objects to symbolically
reference a file produced by one Task and consumed by another.
If not, a pure Path representation should be sufficient.

.. code-block:: python

    ...

on the remote command line
^^^^^^^^^^^^^^^^^^^^^^^^^^

Produce a string (or `os.fsencode`) filesystem path for a `FileReference` suitable
for a Task's run time environment (such as for use as a command line argument).

.. code-block:: python

    ...

Refer to a Task's file past the end of the Session or outside of the Pilot
--------------------------------------------------------------------------

For simplicity, assume we can use
`radical.saga <https://radicalsaga.readthedocs.io/>`__, `radical.utils`, or any other packages,
but that we want to retrieve the file independently of the Pilot under which it is created.

.. code-block:: python

    ...
