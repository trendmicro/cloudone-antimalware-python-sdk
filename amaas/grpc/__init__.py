import threading
import logging
from typing import BinaryIO

import grpc
import os
import re
import io

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


def generate_messages(pipeline: Pipeline, data_reader: BinaryIO, stats: dict) -> None:
    while True:
        message = pipeline.get_message()

        if message.stage == scan_pb2.STAGE_INIT:
            logger.debug("stage INIT")
            yield message
        elif message.stage == scan_pb2.STAGE_RUN:
            assert message.cmd == scan_pb2.CMD_RETR

            logger.debug(
                f"stage RUN, try to read {message.length} at offset {message.offset}")

            data_reader.seek(message.offset)
            chunk = data_reader.read(message.length)

            message = scan_pb2.C2S(
                stage=scan_pb2.STAGE_RUN,
                file_name=None,
                rs_size=0,
                offset=data_reader.tell(),
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
    }

    host = mapping[region]
    return init(host, api_key, enable_tls, ca_cert)


def quit(handle):
    handle.close()


def scan_data(data_reader: BinaryIO, size: int, identifier: str, channel: grpc.Channel) -> str:
    stub = scan_pb2_grpc.ScanStub(channel)
    pipeline = Pipeline()
    stats = {}
    result = None

    try:
        responses = stub.Run(generate_messages(pipeline, data_reader, stats))
        message = scan_pb2.C2S(stage=scan_pb2.STAGE_INIT,
                               file_name=identifier,
                               rs_size=size,
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


def scan_file(file_name: str, channel: grpc.Channel) -> str:
    f = open(file_name, "rb")
    fid = os.path.basename(file_name)
    n = os.stat(file_name).st_size
    return scan_data(f, n, fid, channel)


def scan_buffer(bytes_buffer: bytes, uid: str, channel: grpc.Channel) -> str:
    f = io.BytesIO(bytes_buffer)
    return scan_data(f, len(bytes_buffer), uid, channel)
