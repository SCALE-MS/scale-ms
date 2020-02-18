"""scalems.worker module execution entry point.

Launch a SCALE-MS RPC worker with ``python -m scalems.worker``
"""


from concurrent import futures
import logging

import grpc

from scalems.worker import Worker
from scalems._grpc.runtime_pb2_grpc import add_WorkerServicer_to_server


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    add_WorkerServicer_to_server(Worker(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
