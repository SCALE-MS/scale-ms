"""Test the gRPC control and data interface for graph execution.

Tests are intended to be run with pytest, but the service must be running.

We could use a pytest fixture to start up and shut down of a service, but we
would need to manage the fork and maintenance of a subprocess with `subprocess`
or `multiprocessing`.

Alternatively, we could implement these tests in terms of Docker, Flask, or
something else. (Feedback welcome.)

Usage:

    From the base of the respository, start the server::
        python -m scalems.worker

    Put the server into the background, or open a separate terminal.
    Run the tests::
        python -m pytest ./tests

    Stop the server. Bring it to the foreground or switch to the original
    terminal and issue a INT (e.g. hit Control-C).

"""

import grpc

from scalems._grpc.runtime_pb2_grpc import WorkerStub
from scalems._grpc.runtime_pb2 import Command, InstructionEnum

def test_connection():
    def generate_messages():
        command_message = Command(instruction=InstructionEnum.PLACE)
        command_message.node.uuid = ('a'*32).encode('utf-8')
        yield command_message

    with grpc.insecure_channel('localhost:50051') as channel:
        stub = WorkerStub(channel)
        response = stub.Update(generate_messages())
        for note in response:
            print("Client received: " + note.node.uuid.hex())
