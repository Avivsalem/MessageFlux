import json
from abc import ABCMeta, abstractmethod
from io import BytesIO
from typing import BinaryIO, Dict, Any, Tuple, Optional

from baseservice.utils import json_safe_encoder


class FileSystemSerializerBase(metaclass=ABCMeta):

    @abstractmethod
    def serialize(self, data: BinaryIO, headers: Optional[Dict[str, Any]] = None) -> BinaryIO:
        raise NotImplementedError()

    @abstractmethod
    def deserialize(self, stream: BinaryIO) -> Tuple[BinaryIO, Dict[str, Any]]:
        raise NotImplementedError()


class DefaultFileSystemSerializer(FileSystemSerializerBase):
    def serialize(self, data: BinaryIO, headers: Optional[Dict[str, Any]] = None) -> BinaryIO:
        if not headers:
            headers = {}
        headers_bytes = json.dumps(headers, default=json_safe_encoder).encode()
        result = BytesIO()
        result.write(headers_bytes)
        result.write(b'\n')
        result.write(data.read())
        result.seek(0)
        return result

    def deserialize(self, stream: BinaryIO) -> Tuple[BinaryIO, Dict[str, Any]]:
        first_line = stream.readline()
        rest = stream.read()
        headers = json.loads(first_line.decode())
        data = BytesIO(rest)
        return data, headers


class NoHeadersFileSystemSerializer(FileSystemSerializerBase):
    def serialize(self, data: BinaryIO, headers: Optional[Dict[str, Any]] = None) -> BinaryIO:
        return data

    def deserialize(self, stream: BinaryIO) -> Tuple[BinaryIO, Dict[str, Any]]:
        return BytesIO(stream.read()), {}
