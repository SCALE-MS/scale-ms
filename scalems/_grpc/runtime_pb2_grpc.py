# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from scalems._grpc import runtime_pb2 as scalems_dot___grpc_dot_runtime__pb2


class WorkerStub(object):
  """Interface exported by the server.
  """

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.Update = channel.stream_stream(
        '/scalems_runtime.Worker/Update',
        request_serializer=scalems_dot___grpc_dot_runtime__pb2.Command.SerializeToString,
        response_deserializer=scalems_dot___grpc_dot_runtime__pb2.WorkerNote.FromString,
        )


class WorkerServicer(object):
  """Interface exported by the server.
  """

  def Update(self, request_iterator, context):
    """A bidirectional streaming RPC.

    Accepts a stream of Commands that direct worker execution,
    while receiving updated graph state or other messages.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_WorkerServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'Update': grpc.stream_stream_rpc_method_handler(
          servicer.Update,
          request_deserializer=scalems_dot___grpc_dot_runtime__pb2.Command.FromString,
          response_serializer=scalems_dot___grpc_dot_runtime__pb2.WorkerNote.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'scalems_runtime.Worker', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
