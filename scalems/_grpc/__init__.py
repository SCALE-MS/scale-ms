"""gRPC details for SCALE-MS worker."""

# Note that operations_pb2_grpc.py is borrowed from the googleapis project on github.
# TODO: Comply with licensing agreement.
# Note: It may not be sufficient to rely on the `googleapis-common-protos` package
# for the client stub package. We may have to borrow a bunch of .proto files and
# build our own Python core gRPC packages.

# TODO: Reimplement in C++ so that we can take advantage of more optimal data buffering.
# See, for example, CodedInputStream