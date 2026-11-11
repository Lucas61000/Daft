from __future__ import annotations

import io
import json
import tarfile
import uuid
from typing import TYPE_CHECKING

from daft.datatype import DataType
from daft.dependencies import pafs
from daft.filesystem import _resolve_paths_and_filesystem, get_protocol_from_path
from daft.io.sink import DataSink, WriteResult
from daft.recordbatch.micropartition import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from daft.daft import IOConfig
    from daft.file import File


def _read_file_bytes(value: File) -> bytes:
    with value.open() as file:
        return file.read()


def _member_encoder(dtype: DataType, column: str) -> Callable[[object], bytes]:
    if dtype.is_binary() or dtype.is_fixed_size_binary():
        return bytes
    if dtype.is_string():
        return lambda value: value.encode("utf-8")
    if dtype.is_numeric():
        return lambda value: str(value).encode("utf-8")
    if dtype.is_file():
        return _read_file_bytes
    if dtype.is_struct() or dtype.is_list() or dtype.is_fixed_size_list() or dtype.is_map():
        return lambda value: json.dumps(value).encode("utf-8")
    raise ValueError(
        f"WebDataset column {column!r} has unsupported type {dtype}. Supported types are binary, "
        "string, numeric, file, and JSON-like (struct, list, map) columns."
    )


def _member_columns(schema: Schema) -> list[tuple[str, Callable[[object], bytes]]]:
    encoders = []
    for field in schema:
        if field.name == "__url__":
            continue
        if field.name == "__key__":
            if not field.dtype.is_string():
                raise ValueError(f"WebDataset column '__key__' must be a string, received {field.dtype}")
            continue
        if not field.name or "/" in field.name:
            raise ValueError(
                f"Invalid WebDataset column name {field.name!r}: column names become TAR member "
                "suffixes and must be non-empty and contain no '/'"
            )
        encoders.append((field.name, _member_encoder(field.dtype, field.name)))
    return encoders


def _validate_key(key: object) -> str:
    if not isinstance(key, str) or not key:
        raise ValueError(f"WebDataset sample keys must be non-empty strings, received {key!r}")
    if "." in key or key.startswith("/") or ".." in key.split("/"):
        raise ValueError(
            f"Invalid WebDataset sample key {key!r}: keys must not contain '.', start with '/', "
            "or contain '..' path components"
        )
    return key


class WebDatasetSink(DataSink[str]):
    """DataSink for writing DataFrames as uncompressed WebDataset TAR shards.

    Each row becomes one sample whose columns are written as TAR members named
    ``{key}.{column}``. See :meth:`daft.DataFrame.write_webdataset` for details.
    """

    def __init__(
        self,
        path: str,
        io_config: IOConfig | None = None,
        max_samples_per_shard: int | None = None,
    ) -> None:
        if max_samples_per_shard is not None and max_samples_per_shard <= 0:
            raise ValueError(f"max_samples_per_shard must be greater than zero, received {max_samples_per_shard}")
        self._path = path
        self._io_config = io_config
        self._max_samples_per_shard = max_samples_per_shard

    def name(self) -> str:
        return "WebDataset Write"

    def schema(self) -> Schema:
        return Schema.from_pydict(
            {
                "path": DataType.string(),
                "bytes_written": DataType.int64(),
                "rows_written": DataType.int64(),
            }
        )

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[str]]:
        [root_dir], fs = _resolve_paths_and_filesystem(self._path, io_config=self._io_config)
        root_dir = root_dir.rstrip("/") or "/"
        if isinstance(fs, pafs.LocalFileSystem):
            fs.create_dir(root_dir, recursive=True)
        protocol = get_protocol_from_path(self._path)

        shard_uuid = uuid.uuid4()
        shard_index = 0
        sample_index = 0
        shard_path = ""
        shard_samples = 0
        archive: tarfile.TarFile | None = None
        stream: pafs.NativeFile | None = None

        def flush() -> WriteResult[str]:
            nonlocal archive, stream, shard_index, shard_samples
            assert archive is not None and stream is not None
            archive.close()
            bytes_written = stream.tell()
            stream.close()
            result_path = shard_path if protocol == "file" else f"{protocol}://{shard_path}"
            result = WriteResult(result_path, bytes_written=bytes_written, rows_written=shard_samples)
            archive = None
            stream = None
            shard_index += 1
            shard_samples = 0
            return result

        for part in micropartitions:
            if len(part) == 0:
                continue
            schema = part.schema()
            encoders = _member_columns(schema)
            has_key = "__key__" in schema.column_names()
            for row in part.to_pylist():
                if archive is None:
                    shard_path = f"{root_dir}/{shard_uuid}-{shard_index}.tar"
                    stream = fs.open_output_stream(shard_path)
                    archive = tarfile.open(fileobj=stream, mode="w")
                key = _validate_key(row["__key__"]) if has_key else f"{sample_index:06d}"
                for name, encode in encoders:
                    value = row[name]
                    if value is None:
                        continue
                    data = encode(value)
                    info = tarfile.TarInfo(f"{key}.{name}")
                    info.size = len(data)
                    archive.addfile(info, io.BytesIO(data))
                shard_samples += 1
                sample_index += 1
                if self._max_samples_per_shard is not None and shard_samples >= self._max_samples_per_shard:
                    yield flush()
            if self._max_samples_per_shard is None:
                yield flush()

        if archive is not None:
            yield flush()

    def finalize(self, write_results: list[WriteResult[str]]) -> MicroPartition:
        if not write_results:
            return MicroPartition.empty(self.schema())
        return MicroPartition.from_pydict(
            {
                "path": [result.result for result in write_results],
                "bytes_written": [result.bytes_written for result in write_results],
                "rows_written": [result.rows_written for result in write_results],
            }
        )
