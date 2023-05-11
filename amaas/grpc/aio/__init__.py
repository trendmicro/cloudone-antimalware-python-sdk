import io
import os
from typing import BinaryIO

import grpc
import logging
import re

from ..protos import scan_pb2
from ..protos import scan_pb2_grpc
from ..exception import RateLimitExceededError

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
logger.setLevel(LOG_LEVEL)
logger.propagate = False


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

        channel = grpc.aio.secure_channel(host, creds)
    else:
        channel = grpc.aio.insecure_channel(host)

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


async def quit(handle):
    await handle.close()


# https://github.com/grpc/grpc/blob/91083659fa88c938779dd41e57a7f97981b6c9a1/src/python/grpcio_tests/tests_aio/unit/channel_test.py#L180


async def scan_data(data_reader: BinaryIO, size: int, identifier: str, channel: grpc.Channel) -> str:
    stub = scan_pb2_grpc.ScanStub(channel)
    stats = {}
    result = None
    try:
        call = stub.Run()

        request = scan_pb2.C2S(stage=scan_pb2.STAGE_INIT,
                               file_name=identifier,
                               rs_size=size,
                               offset=0,
                               chunk=None)

        await call.write(request)

        while True:
            response = await call.read()
            if response.cmd == scan_pb2.CMD_RETR:
                assert response.stage == scan_pb2.STAGE_RUN
                logger.debug(
                    f"stage RUN, try to read {response.length} at offset {response.offset}")

                data_reader.seek(response.offset)
                chunk = data_reader.read(response.length)

                request = scan_pb2.C2S(
                    stage=scan_pb2.STAGE_RUN,
                    file_name=None,
                    rs_size=0,
                    offset=data_reader.tell(),
                    chunk=chunk)

                stats["total_upload"] = stats.get(
                    "total_upload", 0) + len(chunk)
                await call.write(request)
            elif response.cmd == scan_pb2.CMD_QUIT:
                assert response.stage == scan_pb2.STAGE_FINI
                result = response.result
                logger.debug("receive QUIT, exit loop...\n")
                break
            else:
                logger.debug("unknown command...")
                break

        await call.done_writing()

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


async def scan_file(file_name: str, channel: grpc.Channel) -> str:
    f = open(file_name, "rb")
    fid = os.path.basename(file_name)
    n = os.stat(file_name).st_size
    return await scan_data(f, n, fid, channel)


async def scan_buffer(bytes_buffer: bytes, uid: str, channel: grpc.Channel) -> str:
    f = io.BytesIO(bytes_buffer)
    return await scan_data(f, len(bytes_buffer), uid, channel)
