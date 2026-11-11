from __future__ import annotations

import io
import tarfile
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

import daft
from daft import DataType, MediaType


class _QuietHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        pass


def _write_tar(path: Path, members: list[tuple[str, bytes]]) -> None:
    with tarfile.open(path, "w") as archive:
        for name, data in members:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


def _write_sparse_tar(path: Path) -> None:
    info = tarfile.TarInfo("sample.bin")
    info.type = tarfile.GNUTYPE_SPARSE
    info.size = 4
    header = bytearray(info.tobuf(format=tarfile.GNU_FORMAT))
    header[386:398] = b"00000002000\0"
    header[398:410] = b"00000000004\0"
    header[483:495] = b"00000004000\0"
    header[148:156] = b"        "
    checksum = sum(header)
    header[148:156] = f"{checksum:06o}\0 ".encode()
    path.write_bytes(header + b"data" + bytes(508 + 1024))


@pytest.fixture
def webdataset_path(tmp_path: Path) -> Path:
    path = tmp_path / "samples.tar"
    _write_tar(
        path,
        [
            ("000001.jpg", b"first-image"),
            ("000001.json", b'{"caption": "first", "score": 1}'),
            ("000001.txt", b"first caption"),
            ("000001.cls", b"7"),
            ("000001.mp4", b"first-video"),
            ("000001.wav", b"first-audio"),
            ("000001.npy", b"first-array"),
            ("000002.jpg", b"second-image"),
            ("000002.json", b'{"caption": "second", "score": 2}'),
            ("000002.txt", b"second caption"),
            ("000002.cls", b"9"),
            ("000002.mp4", b"second-video"),
            ("000002.wav", b"second-audio"),
            ("000002.npy", b"second-array"),
        ],
    )
    return path


@pytest.fixture
def webdataset_http_url(webdataset_path: Path) -> str:
    handler = partial(_QuietHTTPRequestHandler, directory=str(webdataset_path.parent))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        yield f"http://127.0.0.1:{server.server_port}/{webdataset_path.name}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_read_webdataset(webdataset_path: Path) -> None:
    df = daft.read_webdataset(str(webdataset_path))

    assert df.schema()["jpg"].dtype == DataType.file(MediaType.image())
    assert df.schema()["mp4"].dtype == DataType.file(MediaType.video())
    assert df.schema()["wav"].dtype == DataType.file(MediaType.audio())
    assert df.schema()["npy"].dtype == DataType.file()
    assert df.schema()["txt"].dtype == DataType.string()
    assert df.schema()["cls"].dtype == DataType.int64()

    result = df.collect().to_pydict()
    assert result["__key__"] == ["000001", "000002"]
    assert result["__url__"] == [webdataset_path.as_uri()] * 2
    assert result["json"] == [
        {"caption": "first", "score": 1},
        {"caption": "second", "score": 2},
    ]
    assert result["txt"] == ["first caption", "second caption"]
    assert result["cls"] == [7, 9]

    images = result["jpg"]
    assert all(isinstance(image, daft.ImageFile) for image in images)
    with images[0].open() as image:
        assert image.read() == b"first-image"
    with images[1].open() as image:
        assert image.read() == b"second-image"

    assert all(isinstance(video, daft.VideoFile) for video in result["mp4"])
    assert all(isinstance(audio, daft.AudioFile) for audio in result["wav"])
    assert all(type(array) is daft.File for array in result["npy"])
    with result["mp4"][0].open() as video:
        assert video.read() == b"first-video"
    with result["wav"][0].open() as audio:
        assert audio.read() == b"first-audio"
    with result["npy"][0].open() as array:
        assert array.read() == b"first-array"


def test_read_webdataset_http_range_references(webdataset_http_url: str) -> None:
    result = daft.read_webdataset(webdataset_http_url).select("__key__", "jpg").collect().to_pydict()

    assert result["__key__"] == ["000001", "000002"]
    assert result["jpg"][0].path == webdataset_http_url
    assert result["jpg"][0].position is not None
    assert result["jpg"][0]._inner.size() == len(b"first-image")


def test_read_webdataset_projection(webdataset_path: Path) -> None:
    result = daft.read_webdataset(str(webdataset_path)).select("txt", "jpg").collect().to_pydict()

    assert set(result) == {"txt", "jpg"}
    assert result["txt"] == ["first caption", "second caption"]
    with result["jpg"][0].open() as image:
        assert image.read() == b"first-image"


def test_read_webdataset_directory_and_multiple_shards(tmp_path: Path) -> None:
    _write_tar(tmp_path / "first.tar", [("a.txt", b"first")])
    _write_tar(tmp_path / "second.tar", [("b.txt", b"second")])

    result = daft.read_webdataset(str(tmp_path), batch_size=1).sort("__key__").collect().to_pydict()

    assert result["__key__"] == ["a", "b"]
    assert result["txt"] == ["first", "second"]


def test_read_webdataset_preserves_multipart_suffixes(tmp_path: Path) -> None:
    path = tmp_path / "multipart.tar"
    _write_tar(
        path,
        [
            ("nested/sample.en.txt", b"caption"),
            ("nested/sample.jpg", b"image"),
        ],
    )

    result = daft.read_webdataset(str(path)).collect().to_pydict()

    assert result["__key__"] == ["nested/sample"]
    assert result["en.txt"] == ["caption"]
    with result["jpg"][0].open() as image:
        assert image.read() == b"image"


def test_read_webdataset_missing_inferred_field_is_null(tmp_path: Path) -> None:
    path = tmp_path / "missing.tar"
    _write_tar(
        path,
        [
            ("a.jpg", b"image-a"),
            ("a.txt", b"caption-a"),
            ("b.jpg", b"image-b"),
        ],
    )

    result = daft.read_webdataset(str(path)).sort("__key__").collect().to_pydict()

    assert result["txt"] == ["caption-a", None]


def test_read_webdataset_rejects_field_missing_from_inferred_schema(tmp_path: Path) -> None:
    path = tmp_path / "inconsistent-fields.tar"
    members = [(f"{index:06d}.txt", b"caption") for index in range(6)]
    members.append(("000005.json", b'{"caption": "late"}'))
    _write_tar(path, members)

    with pytest.raises(ValueError, match="was not present during schema inference"):
        daft.read_webdataset(str(path)).collect()


def test_read_webdataset_rejects_incompatible_json_schema(tmp_path: Path) -> None:
    path = tmp_path / "inconsistent-json.tar"
    members = [(f"{index:06d}.json", b'{"caption": "consistent"}') for index in range(5)]
    members.append(("000005.json", b'{"different": "field"}'))
    _write_tar(path, members)

    with pytest.raises(ValueError, match="does not match its inferred schema"):
        daft.read_webdataset(str(path)).collect()


def test_read_webdataset_rejects_duplicate_fields(tmp_path: Path) -> None:
    path = tmp_path / "duplicate.tar"
    _write_tar(path, [("a.txt", b"first"), ("a.txt", b"second")])

    with pytest.raises(ValueError, match="Duplicate WebDataset field"):
        daft.read_webdataset(str(path))


def test_read_webdataset_rejects_compressed_tar(tmp_path: Path) -> None:
    path = tmp_path / "samples.tar.gz"
    with tarfile.open(path, "w:gz") as archive:
        info = tarfile.TarInfo("a.txt")
        info.size = 5
        archive.addfile(info, io.BytesIO(b"hello"))

    with pytest.raises(ValueError, match="Compressed WebDataset shards are not supported"):
        daft.read_webdataset(str(path))


def test_read_webdataset_rejects_sparse_tar_member(tmp_path: Path) -> None:
    path = tmp_path / "sparse.tar"
    _write_sparse_tar(path)

    with pytest.raises(ValueError, match="Sparse TAR member 'sample.bin'"):
        daft.read_webdataset(str(path))


@pytest.mark.parametrize("batch_size", [0, -1])
def test_read_webdataset_rejects_invalid_batch_size(webdataset_path: Path, batch_size: int) -> None:
    with pytest.raises(ValueError, match="batch_size must be greater than zero"):
        daft.read_webdataset(str(webdataset_path), batch_size=batch_size)


def test_write_webdataset_round_trip(webdataset_path: Path, tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    source = daft.read_webdataset(str(webdataset_path)).sort("__key__").collect().to_pydict()

    manifest = daft.read_webdataset(str(webdataset_path)).write_webdataset(str(output_dir)).to_pydict()

    assert len(manifest["path"]) == 1
    assert manifest["rows_written"] == [2]
    assert manifest["bytes_written"][0] > 0
    assert manifest["path"][0].endswith(".tar")
    assert Path(manifest["path"][0]).exists()

    result = daft.read_webdataset(str(output_dir)).sort("__key__").collect().to_pydict()
    assert result["__key__"] == source["__key__"]
    assert result["txt"] == source["txt"]
    assert result["cls"] == source["cls"]
    assert result["json"] == source["json"]
    for column in ("jpg", "mp4", "wav", "npy"):
        for written_file, source_file in zip(result[column], source[column]):
            with written_file.open() as file:
                written_bytes = file.read()
            with source_file.open() as file:
                source_bytes = file.read()
            assert written_bytes == source_bytes


def test_write_webdataset_members(tmp_path: Path) -> None:
    df = daft.from_pydict(
        {
            "__key__": ["a", "b"],
            "__url__": ["ignored", "ignored"],
            "jpg": [b"image-a", b"image-b"],
            "txt": ["caption-a", "caption-b"],
            "cls": [1, 2],
            "json": [{"x": 1}, {"x": 2}],
        }
    )
    output_dir = tmp_path / "output"
    manifest = df.write_webdataset(str(output_dir)).to_pydict()

    assert manifest["rows_written"] == [2]
    shard_path = Path(manifest["path"][0])
    with tarfile.open(shard_path) as archive:
        assert archive.getnames() == [
            "a.jpg",
            "a.txt",
            "a.cls",
            "a.json",
            "b.jpg",
            "b.txt",
            "b.cls",
            "b.json",
        ]
        assert archive.extractfile("a.jpg").read() == b"image-a"
        assert archive.extractfile("a.txt").read() == b"caption-a"
        assert archive.extractfile("a.cls").read() == b"1"
        assert archive.extractfile("a.json").read() == b'{"x": 1}'


def test_write_webdataset_generates_keys(tmp_path: Path) -> None:
    df = daft.from_pydict({"txt": ["a", "b", "c"]})
    output_dir = tmp_path / "output"
    df.write_webdataset(str(output_dir))

    result = daft.read_webdataset(str(output_dir)).sort("__key__").collect().to_pydict()
    assert result["__key__"] == ["000000", "000001", "000002"]
    assert result["txt"] == ["a", "b", "c"]


def test_write_webdataset_skips_null_members(tmp_path: Path) -> None:
    df = daft.from_pydict({"__key__": ["a", "b"], "txt": ["hello", None], "jpg": [b"image-a", b"image-b"]})
    output_dir = tmp_path / "output"
    manifest = df.write_webdataset(str(output_dir)).to_pydict()

    with tarfile.open(manifest["path"][0]) as archive:
        assert archive.getnames() == ["a.txt", "a.jpg", "b.jpg"]

    result = daft.read_webdataset(str(output_dir)).sort("__key__").collect().to_pydict()
    assert result["txt"] == ["hello", None]


def test_write_webdataset_max_samples_per_shard(tmp_path: Path) -> None:
    df = daft.from_pydict({"txt": [str(index) for index in range(5)]})
    output_dir = tmp_path / "output"
    manifest = df.write_webdataset(str(output_dir), max_samples_per_shard=2).to_pydict()

    assert sorted(manifest["rows_written"]) == [1, 2, 2]
    assert len(manifest["path"]) == 3
    assert len(set(manifest["path"])) == 3

    result = daft.read_webdataset(str(output_dir)).sort("txt").collect().to_pydict()
    assert result["txt"] == [str(index) for index in range(5)]


def test_write_webdataset_empty_dataframe(tmp_path: Path) -> None:
    df = daft.from_pydict({"txt": []})
    output_dir = tmp_path / "output"
    manifest = df.write_webdataset(str(output_dir)).to_pydict()

    assert manifest == {"path": [], "bytes_written": [], "rows_written": []}


def test_write_webdataset_rejects_unsupported_type(tmp_path: Path) -> None:
    df = daft.from_pydict({"x": [True, False]})
    with pytest.raises(RuntimeError, match="unsupported type"):
        df.write_webdataset(str(tmp_path / "output"))


@pytest.mark.parametrize("key", ["a.b", "", "/abs", "../escape"])
def test_write_webdataset_rejects_invalid_keys(tmp_path: Path, key: str) -> None:
    df = daft.from_pydict({"__key__": [key], "txt": ["x"]})
    with pytest.raises(RuntimeError, match="WebDataset sample key"):
        df.write_webdataset(str(tmp_path / "output"))


def test_write_webdataset_rejects_null_key(tmp_path: Path) -> None:
    df = daft.from_pydict({"__key__": ["valid", None], "txt": ["x", "y"]})
    with pytest.raises(RuntimeError, match="WebDataset sample key"):
        df.write_webdataset(str(tmp_path / "output"))


def test_write_webdataset_rejects_non_string_key_column(tmp_path: Path) -> None:
    df = daft.from_pydict({"__key__": [1, 2], "txt": ["x", "y"]})
    with pytest.raises(RuntimeError, match="'__key__' must be a string"):
        df.write_webdataset(str(tmp_path / "output"))


def test_write_webdataset_rejects_invalid_column_name(tmp_path: Path) -> None:
    df = daft.from_pydict({"nested/txt": ["x"]})
    with pytest.raises(RuntimeError, match="Invalid WebDataset column name"):
        df.write_webdataset(str(tmp_path / "output"))


@pytest.mark.parametrize("max_samples_per_shard", [0, -1])
def test_write_webdataset_rejects_invalid_max_samples_per_shard(tmp_path: Path, max_samples_per_shard: int) -> None:
    df = daft.from_pydict({"txt": ["x"]})
    with pytest.raises(ValueError, match="max_samples_per_shard must be greater than zero"):
        df.write_webdataset(str(tmp_path / "output"), max_samples_per_shard=max_samples_per_shard)
