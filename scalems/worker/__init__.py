from typing import Iterator

import google.protobuf
from google.protobuf.empty_pb2 import Empty
from google.longrunning.operations_pb2 import CancelOperationRequest, DeleteOperationRequest, ListOperationsRequest, ListOperationsResponse, GetOperationRequest, Operation
import grpc

from scalems._grpc.operations_pb2_grpc import add_OperationsServicer_to_server, OperationsServicer
from scalems._grpc.runtime_pb2 import Command, WorkerNote
from scalems._grpc.runtime_pb2_grpc import add_WorkerServicer_to_server, WorkerServicer

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

                # Place the note and get the resource name.
                yield WorkerNote(note=WorkerNote.Note.RESOURCE_NAME, name=uuid.hex())
        else:
            # 'instruction' is empty.
            pass
    except ValueError:
        # `command` is not a Command, or we are accessing fields not defined for Command.
        pass

class Worker(WorkerServicer):
    """Service the messages received over the RPC channel.

    Note: the initial implementation uses a single thread per channel.

    Though commands and responses are theoretically asynchronous,
    in practice they are not, so a command to cancel a running operation
    is not yet possible.

    We should relax this constraint, but it is not clear whether we should
    (a) support asynchronous servicing by the Worker (multiple server threads),
    or (b) allow concurrent dispatching within the worker (internally-managed
    threading). I expect the former would be preferable, if possible, since
    there is already an assumption of a thread pool for the *server* bound to the
    Worker instance, but the concurrency model for Workers needs further discussion.

    Furthermore, there are various reasons (especially data buffering optimizations)
    for which it makes sense to implement the servicer in C++ instead of Python.
    """
    def Update(self, request: Iterator[Command], context: grpc.ServicerContext) -> Iterator[WorkerNote]:
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

class LroServicer(OperationsServicer):
    """Servicer for Long Running Operations.

    Complies with Google API Long-Running Operation interface.
    """

    def ListOperations(self, request: ListOperationsRequest, context: grpc.ServicerContext) -> ListOperationsResponse:
        return super().ListOperations(request, context)

    def GetOperation(self, request: GetOperationRequest, context: grpc.ServicerContext) -> Operation:
        return super().GetOperation(request, context)

    def DeleteOperation(self, request: DeleteOperationRequest, context: grpc.ServicerContext) -> Empty:
        return super().DeleteOperation(request, context)

    def CancelOperation(self, request: CancelOperationRequest, context: grpc.ServicerContext) -> Empty:
        return super().CancelOperation(request, context)

    # google.longrunning.operations.WaitOperationRequest is missing from googleapis-common-protos 1.51.0
    # def WaitOperation(self, request, context) -> Operation:
    #     return super().WaitOperation(request, context)

def set_up_server(server: grpc.Server):
    assert isinstance(server, grpc.Server)
    add_WorkerServicer_to_server(Worker(), server)
    # add_OperationsServicer_to_server(LroServicer(), server)
