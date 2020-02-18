import google.protobuf

from scalems._grpc.runtime_pb2 import Command, WorkerNote
from scalems._grpc.runtime_pb2_grpc import WorkerServicer

# Ref: https://developers.google.com/protocol-buffers/docs/reference/python-generated#enum
from scalems._grpc.runtime_pb2 import InstructionEnum

def handle_request(command: Command):
    # Ref: https://googleapis.dev/python/protobuf/latest/google/protobuf/message.html#google.protobuf.message.Message
    assert isinstance(command, google.protobuf.message.Message)
    try:
        if command.HasField('instruction'):
            assert hasattr(command, 'instruction')
            if command.instruction == InstructionEnum.PLACE:
                assert command.HasField('node')
                node = command.node
                assert node.HasField('uuid')
                uuid = node.uuid  # type: bytes
                assert len(uuid) == 32
                yield WorkerNote(message='Hello, %s!' % uuid.hex())
        else:
            # 'instruction' is empty.
            pass
    except ValueError:
        # `command` is not a Command, or we are accessing fields not defined for Command.
        pass

class Worker(WorkerServicer):
    """Service the messages received over the RPC channel.

    Note: the initial implementation uses a single thread per channel. Though
    commands and responses are theoretically asynchronous, in practice they are
    not, so a command to cancel a running operation is not yet possible. We
    should relax this constraint, but it is not clear whether we should
    (a) support asynchronous servicing by the Worker (multiple server threads),
    or (b) allow concurrent dispatching within the worker (internally-managed
    threading). I expect the former would be preferable, if possible, since
    there is already an assumption of a thread pool for the *server* bound to the
    Worker instance, but the concurrency model for Workers needs further discussion.
    """
    def Update(self, request, context):
        """
        Apply updates to the worker.

        Place data, issue commands, and schedule work.

        Overrides the generated code in the base class.
        """
        # Accept a series of requests. Yield zero or more responses to each request.
        for command in request:
            for response in handle_request(command):
                yield response
        # Finish session. Send any final messages to the client.
        # (Not yet needed...)
        #yield ...
