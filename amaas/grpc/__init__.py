import threading
import logging
import grpc
import os
import re

from .protos import scan_pb2
from .protos import scan_pb2_grpc
from .exception import RateLimitExceededError

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
logger.setLevel(LOG_LEVEL)
logger.propagate = False


class Pipeline:
    """
    Class to allow a single element pipeline between producer and consumer.
    """

    def __init__(self):
        self.message = 0
        self.producer_lock = threading.Lock()
        self.consumer_lock = threading.Lock()
        self.consumer_lock.acquire()

    def get_message(self):
        self.consumer_lock.acquire()
        message = self.message
        self.producer_lock.release()
        return message

    def set_message(self, message):
        self.producer_lock.acquire()
        self.message = message
        self.consumer_lock.release()


def generate_messages(pipeline, file_name, stats):
    with open(file_name, mode="rb") as f:
        while True:
            message = pipeline.get_message()

            if message.stage == scan_pb2.STAGE_INIT:
                logger.debug("stage INIT")
                yield message
            elif message.stage == scan_pb2.STAGE_RUN:
                assert message.cmd == scan_pb2.CMD_RETR

                logger.debug(
                    f"stage RUN, try to read {message.length} at offset {message.offset}")

                f.seek(message.offset)
                chunk = f.read(message.length)

                message = scan_pb2.C2S(
                    stage=scan_pb2.STAGE_RUN,
                    file_name=None,
                    rs_size=0,
                    offset=f.tell(),
                    chunk=chunk)

                stats["total_upload"] = stats.get(
                    "total_upload", 0) + len(chunk)
                yield message
            elif message.stage == scan_pb2.STAGE_FINI:
                assert message.cmd == scan_pb2.CMD_QUIT
                logger.debug("final stage, quit generating C2S messages...")
                break
            else:
                logger.debug("unknown stage.....!!!")
                break


def init(host, api_key=None, enable_tls=False, ca_cert=None):
    # TBD: The API key handling logic here is slightly different from what's been
    # implemented in the Golang SDK. For Golang, it assumes the user is firstly
    # general public, non-dev users, using production service. So if API key parameter
    # is left empty, the SDK will return an error. But the logic implemented here
    # is that if the parameter is empty, it will just rely on the server to determine
    # whether or not that's ok. If the server is a production server, then it will
    # return an error indicating that authentication couldn't be completed, at which
    # time the user might realize he/she needs to provide a key or a valid key. On
    # the other hand, if the server is some internal dev server that doesn't require
    # authentication, then it will just work fine when no API key is specified--
    # which is better for devs. With the Golang SDK, no API key is specified, or an
    # empty string is passed in, dev would need to set an extra environment variable
    # to suppress the error checking by the Golang Client SDK, so some additional
    # inconvenience. Will discuss this among team members to reach consensus and
    # change either this implementation or the implementation for Golang and TS.

    call_creds = None
    if api_key:
        match = re.search(r'ey[^.]+.ey[^.]+.ey[^.]+', api_key)
        if match:
            str = 'Bearer ' + api_key
        else:
            str = 'ApiKey ' + api_key
        call_creds = grpc.metadata_call_credentials(GrpcAuth(str))

    if enable_tls:
        if ca_cert:
            # Bring Your Own Certificate case
            with open(ca_cert, 'rb') as f:
                ssl_creds = grpc.ssl_channel_credentials(f.read())
        else:
            ssl_creds = grpc.ssl_channel_credentials()

        # if authentication is necessary, combined call_creds with ssl_creds.
        # Otherwise, just use ssl_creds for channel credentials.
        if call_creds is None:
            creds = ssl_creds
        else:
            creds = grpc.composite_channel_credentials(ssl_creds, call_creds)

        channel = grpc.secure_channel(host, creds)
    else:
        channel = grpc.insecure_channel(host)

    return channel


class GrpcAuth(grpc.AuthMetadataPlugin):
    def __init__(self, key):
        self._key = key

    def __call__(self, context, callback):
        callback((('authorization', self._key),), None)


def init_by_region(region, api_key, enable_tls=True, ca_cert=None):
    mapping = {
        'us-1': 'antimalware.us-1.cloudone.trendmicro.com:443',
        'in-1': 'antimalware.in-1.cloudone.trendmicro.com:443',
        'de-1': 'antimalware.de-1.cloudone.trendmicro.com:443',
        'sg-1': 'antimalware.sg-1.cloudone.trendmicro.com:443',
        'au-1': 'antimalware.au-1.cloudone.trendmicro.com:443',
        'jp-1': 'antimalware.jp-1.cloudone.trendmicro.com:443',
        'gb-1': 'antimalware.gb-1.cloudone.trendmicro.com:443',
        'ca-1': 'antimalware.ca-1.cloudone.trendmicro.com:443',
        'ae-1': 'antimalware.ae-1.cloudone.trendmicro.com:443',
        'trend-us-1': 'antimalware.trend-us-1.cloudone.trendmicro.com:443',
        # This is only for internal dev - can be removed later
        'dev': 'test.amaastest.com:50051'
    }

    host = mapping[region]
    return init(host, api_key, enable_tls, ca_cert)


def quit(handle):
    handle.close()


def scan_file(file_name, channel):
    stub = scan_pb2_grpc.ScanStub(channel)
    pipeline = Pipeline()
    stats = {}
    result = None

    try:
        file_size = os.stat(file_name)
        responses = stub.Run(generate_messages(pipeline, file_name, stats))

        message = scan_pb2.C2S(stage=scan_pb2.STAGE_INIT,
                               file_name=os.path.basename(file_name),
                               rs_size=file_size.st_size,
                               offset=0,
                               chunk=None)

        pipeline.set_message(message)

        for response in responses:
            if response.cmd == scan_pb2.CMD_RETR:
                pipeline.set_message(response)
            elif response.cmd == scan_pb2.CMD_QUIT:
                result = response.result
                pipeline.set_message(response)
                logger.debug("receive QUIT, exit loop...\n")
                break
            else:
                logger.debug("unknown command...")
                break

        total_upload = stats.get("total_upload", 0)
        logger.debug(f"total upload {total_upload} bytes")

    except grpc.RpcError as rpc_error:
        if "429" in str(rpc_error):
            raise RateLimitExceededError()
        else:
            raise
    except Exception:
        raise

    return result
