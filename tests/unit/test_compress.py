from aliyun.log.compress import Compressor, CompressType


def test_lz4():
    text = b'sadsadsa189634o2??ASBKHD'
    compressed = Compressor.compress(text, CompressType.LZ4)
    raw_size = len(text)
    uncompressed = Compressor.decompress(
        compressed, raw_size, CompressType.LZ4)

    assert text == uncompressed, "The decompressed data does not match the original"
