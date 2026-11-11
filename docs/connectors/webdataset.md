# WebDataset

[WebDataset](https://github.com/webdataset/webdataset) stores multimodal samples
as consecutive members of TAR shards. Daft reads these shards with
[`daft.read_webdataset()`][daft.read_webdataset] and writes them with
[`df.write_webdataset()`][daft.DataFrame.write_webdataset].

## Reading shards

Pass a TAR file, directory, glob, or list of paths:

```python
import daft

df = daft.read_webdataset("/datasets/images/*.tar")
```

Each row represents one sample. Consecutive members with the same prefix are
grouped together, and the suffix becomes the column name. For example,
`000001.jpg`, `000001.json`, and `000001.txt` produce one row with `jpg`,
`json`, and `txt` columns. Daft also includes:

- `__key__`: the sample prefix
- `__url__`: the shard containing the sample

JSON, text, and class/index sidecars are decoded eagerly. Image, audio, video,
and other binary members are represented as lazy [`File`][daft.File]
references into their TAR shard. Selecting only metadata columns therefore
avoids reading media payloads:

```python
metadata = df.select("__key__", "json", "txt")
images = df.select("__key__", "jpg")
```

## Writing shards

Write a DataFrame as WebDataset TAR shards with
[`df.write_webdataset()`][daft.DataFrame.write_webdataset]:

```python
df = daft.from_pydict(
    {
        "jpg": [b"image-a", b"image-b"],
        "txt": ["caption-a", "caption-b"],
    }
)
df.write_webdataset("/datasets/output")
```

Each row becomes one sample, and each column becomes a TAR member named
`{key}.{column}`. The sample key comes from the `__key__` column if present;
otherwise, keys are generated as zero-padded row indices. A `__url__` column is
ignored, and null values are written as missing members that read back as nulls.

Columns are encoded by type: binary as raw bytes, strings as UTF-8, numbers as
their string representation, [`File`][daft.File] as the referenced bytes, and
struct/list/map columns as JSON. This means a DataFrame read with
`daft.read_webdataset()` can be written back out without materializing the media
payloads yourself.

By default, one shard is written per micropartition. Pass
`max_samples_per_shard` to control shard sizes:

```python
df.write_webdataset("/datasets/output", max_samples_per_shard=1000)
```

The call returns a DataFrame with one row per written shard, containing the
shard `path`, `bytes_written`, and `rows_written`.

## Reading from Hugging Face

Use `format="webdataset"` with
[`daft.read_huggingface()`][daft.read_huggingface]:

```python
df = daft.read_huggingface(
    "laion/conceptual-captions-12m-webdataset",
    format="webdataset",
)
```

You can also read explicit Hugging Face paths:

```python
df = daft.read_webdataset(
    "hf://datasets/laion/conceptual-captions-12m-webdataset/data/*.tar"
)
```

## Format requirements

Daft currently requires:

- Uncompressed `.tar` shards, because lazy member reads use byte offsets.
- Non-sparse TAR members, because sparse file contents are not stored in one
  contiguous byte range.
- Consistent member suffixes and JSON shapes across shards.

Schema inference uses the first five samples in the first shard. If a later
sample introduces an incompatible suffix or JSON shape, Daft raises an error
instead of silently discarding data.
