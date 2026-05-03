# Region Converter

Rust command-line tool for converting Minecraft Java Edition region saves between:

- `mca` (`.mca`)
  > Minecraft's default save format, supported by all servers.

- `linear` (`.linear`)
  > A compressed format, supported by [LinearPurpur](https://github.com/StupidCraft/LinearPurpur), [Kaiiju](https://github.com/KaiijuMC/Kaiiju), [LeavesMC](https://github.com/LeavesMC/Leaves) and [Luminol](https://github.com/LuminolMC/Luminol).

- `blinear_v2` (`.b_linear`)
  > The next-generation compression format, a reimplemented linear, created by [Luminol](https://github.com/LuminolMC/Luminol).

- `blinear_v3` (`.b_linear`)
  > The third generation of blinear, supported by [Luminol](https://github.com/LuminolMC/Luminol), provides better performance and stability.

## Features

- Parallel conversion with a configurable worker count
- Automatic use of available CPU threads when `--threads` is not set, capped to the discovered region-file count to avoid idle workers
- Compression level control for compressed target formats
- Works on Windows and Linux
- Reads classic `linear` v1/v2 files and modern `linear v3` files
- Writes `linear` output in the modern `linear v3` layout
- Skips corrupted chunks when the format allows chunk-level recovery
- Fails corrupted whole-region inputs without producing partial garbage
- Accepts one or more world directories, region directories, or single region files
- Supports `--info` mode for detailed save statistics without conversion
- Prints a concise conversion summary before work starts
- Shows live progress in a single line with completed regions, successful chunks, discarded chunks, warning counts, and chunk throughput
- Writes each completed region file to the output path immediately instead of waiting for the whole batch to finish

## Build

```bash
cargo build --release
```

## Usage

```bash
cargo run --release -- \
  --to blinear-v3 \
  --output /path/to/output \
  /path/to/world
```

### Common options

```text
--from <auto|mca|linear|blinear-v2|blinear-v3>
--to <mca|linear|blinear-v2|blinear-v3>
--output <PATH>
--info
--threads <N>
--compression-level <LEVEL>
```

### Examples

Convert a world directory to `blinear_v3`:

```bash
cargo run --release -- \
  --to blinear-v3 \
  --output /data/out \
  /data/world
```

Convert multiple region directories at once:

```bash
cargo run --release -- \
  --to mca \
  --threads 16 \
  --compression-level 6 \
  --output /data/out \
  /data/world/region \
  /data/world_nether/region
```

Convert with a fixed source format instead of auto-detection:

```bash
cargo run --release -- \
  --from blinear-v2 \
  --to linear \
  --output /data/out \
  /data/world
```

Convert a single region file:

```bash
cargo run --release -- \
  --to mca \
  --output /data/out \
  /data/world/region/r.0.0.linear
```

Inspect a save without converting:

```bash
cargo run --release -- --info /data/world
```

Inspect a single region file:

```bash
cargo run --release -- --info /data/world/region/r.0.0.b_linear
```

## Runtime output

Before conversion starts, the CLI prints:

- input paths
- output path
- source-format mode and target format
- effective worker-thread count
- compression level
- total region-file count

During conversion, the CLI shows live progress including:

- completed region files versus total region files
- successful chunk count and discarded chunk count
- warnings seen so far
- chunks per second

With `--info`, the CLI prints:

- input type for each path (`world directory`, `region directory`, or `region file`)
- total storage size
- detected storage-format breakdown
- region-file count and readable/failed region counts
- decoded chunk count, discarded chunks, and warnings
- per-file details when the input itself is a single region file

## Compression levels

- `mca`: zlib `0..=9`
- `linear`, `blinear_v2`, `blinear_v3`: zstd `1..=22`

Default compression level is `6`.

## Input discovery

If an input path is a supported region file, it is treated as a single-file input.

If an input directory directly contains region files, it is treated as a region directory.

If it does not, the converter searches recursively and treats the input as a world directory. Any nested directory that directly contains supported region files is converted, which covers layouts such as:

- `world/region`
- `world/DIM-1/region`
- `world/DIM1/region`

## Output layout

- directory inputs are written under `--output/<input-folder-name>__<hash>/...`
- world inputs keep their internal relative region-directory structure inside that folder
- region-directory inputs write their region files directly inside that folder
- single region-file inputs are written directly under `--output`
- the hash suffix is stable for each source path, which keeps same-named directories from colliding across different runs

## Corruption handling

- Broken chunks are skipped with warnings when the format has enough structure to recover the rest of the region.
- Broken whole-region payloads fail that region file and leave other region files running.
- The process exits with a non-zero status if warnings or errors were encountered.

## Notes

- `linear` input compatibility covers classic linear v1/v2 and the newer linear v3 layout from the referenced server implementation.
- `linear` output is written as linear v3.
- `blinear_v2` and `blinear_v3` are implemented from the referenced server-side format behavior and validated against the sample files.

