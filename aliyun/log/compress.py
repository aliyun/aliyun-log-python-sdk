
import zlib
from enum import Enum
from .logexception import LogException
from .util import Util
import six
import lz4.block


def lz_decompress(raw_size, data):
    return lz4.block.decompress(data, uncompressed_size=raw_size)


def lz_compresss(data):
    return lz4.block.compress(data)[4:]


class CompressType(Enum):
    UNCOMPRESSED = 1
    LZ4 = 2

    @staticmethod
    def default_compress_type():
        return CompressType.LZ4

    def __str__(self):
        if self == CompressType.LZ4:
            return 'lz4'
        return ''


class Compressor():
    @staticmethod
    def compress(data, compress_type):
        # type: (bytes, CompressType) -> bytes
        if compress_type == CompressType.LZ4:
            return lz_compresss(data)

        if compress_type == CompressType.UNCOMPRESSED:
            return data

        raise LogException('UnknownCompressType',
                           'Unknown compress type: ' + compress_type)

    @staticmethod
    def decompress(data, raw_size, compress_type):
        # type: (bytes, int, CompressType) -> bytes
        if compress_type == CompressType.LZ4:
            return lz_decompress(raw_size, data)

        if compress_type == CompressType.UNCOMPRESSED:
            return data

        raise LogException('UnknownCompressType',
                           'Unknown compress type: ' + compress_type)

    @staticmethod
    def decompress_response(header, response):
        # type: (dict, bytes) -> bytes
        compress_type_str = Util.h_v_td(
            header, 'x-log-compresstype', '').lower()

        if compress_type_str == '':
            return response

        raw_size = int(Util.h_v_td(header, 'x-log-bodyrawsize', 0))
        if raw_size == 0:
            return six.b('')

        if compress_type_str == 'lz4':
            return Compressor.decompress(response, raw_size, CompressType.LZ4)

        if compress_type_str in ('gzip', 'deflate'):
            return zlib.decompress(response)

        raise LogException('UnknownCompressType', 'Unknown compress type: ' +
                           compress_type_str, resp_header=header, resp_body=response)
