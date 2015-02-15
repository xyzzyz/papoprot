from rpc_pb2 import RPCRequest, RPCResponse, RPCPubSub
from txzmq import ZmqEndpoint, ZmqFactory, ZmqREPConnection

import google.protobuf
_varintDecoder = google.protobuf.internal.decoder._VarintDecoder((1 << 32) - 1)
_varintEncoderInternal = google.protobuf.internal.encoder._VarintEncoder()
def _varintEncoder(n):
    return _varintEncoderInternal(lambda x: x, n)

def _SerializeWithLength(message):
    s = message.SerializeToString()
    return _varintEncoder(len(s))+s

def rpc_method(cls):
    def wrapper(f):
        f.request_class = cls
        return f
    return wrapper

class RPCError(Exception): pass

class RPCServer(ZmqREPConnection):
    def gotMessage(self, messageId, message):
        print "gotMessage"
        (rpc_request_len, rpc_request_off) = _varintDecoder(message, 0)
        request = RPCRequest()
        request.ParseFromString(
            message[rpc_request_off:rpc_request_off+rpc_request_len])

        (app_request_len, app_request_off) = (
            _varintDecoder(message, rpc_request_off+rpc_request_len))
        app_request_str = message[app_request_off:]
        assert len(app_request_str) == app_request_len
        handler = getattr(self, request.method)
        app_request = handler.request_class()
        app_request.ParseFromString(app_request_str)

        try:
            app_response = handler(app_request)

            response = RPCResponse()
            response.status = RPCResponse.OK
            response_str = response.SerializeToString()

            rpc_response_str = (_SerializeWithLength(response) +
                                _SerializeWithLength(app_response))
            self.reply(messageId, rpc_response_str)
        except RPCError, e:
            response = RPCResponse()
            response.status = RPCResponse.APP_ERROR
            response.status_info = e.message
            rpc_response_str = _SerializeWithLength(response)
            self.reply(messageId, rpc_response_str)
        except Exception, e:
            response = RPCResponse()
            response.status = RPCResponse.RPC_ERROR
            response.status_info = "Internal error when handling request"
            rpc_response_str = _SerializeWithLength(response)
            self.reply(messageId, rpc_response_str)
            raise

