"""scalems.worker module execution entry point.

Launch a SCALE-MS RPC worker with ``python -m scalems.worker``
"""


from concurrent import futures
import logging

import grpc

from scalems.worker import set_up_server


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    set_up_server(server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
