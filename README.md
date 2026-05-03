# region_converter

Rust command-line tool for converting Minecraft Java Edition region saves between:

- `mca` (`.mca`)
- `linear` (`.linear`)
- `blinear_v2` (`.b_linear`)
- `blinear_v3` (`.b_linear`)

## Features

- Parallel conversion with a configurable worker count
- Automatic use of available CPU threads when `--threads` is not set, capped to the discovered region-file count to avoid idle workers
- Compression level control for compressed target formats
- Works on Windows and Linux
- Skips corrupted chunks when the format allows chunk-level recovery
- Fails corrupted whole-region inputs without producing partial garbage
- Accepts one or more world directories or region directories
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
--threads <N>
--compression-level <LEVEL>
```

### Examples

Convert a world directory to `blinear_v3`:

```bash
cargo run --release -- \
  --to blinear-v3 \
  --output /data/out/world \
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

## Compression levels

- `mca`: zlib `0..=9`
- `linear`, `blinear_v2`, `blinear_v3`: zstd `1..=22`

Default compression level is `6`.

## Input discovery

If an input directory directly contains region files, it is treated as a region directory.

If it does not, the converter searches recursively and treats the input as a world directory. Any nested directory that directly contains supported region files is converted, which covers layouts such as:

- `world/region`
- `world/DIM-1/region`
- `world/DIM1/region`

## Output layout

For a single input:

- single region directory input: files are written directly under `--output`
- single world directory input: region subdirectories are recreated under `--output`

For multiple inputs:

- each input gets its own mount directory under `--output`
- world inputs keep their internal relative region-directory structure

## Corruption handling

- Broken chunks are skipped with warnings when the format has enough structure to recover the rest of the region.
- Broken whole-region payloads fail that region file and leave other region files running.
- The process exits with a non-zero status if warnings or errors were encountered.

## Notes

- `linear` support targets the classic linear v1/v2 layout used by the Python reference converter.
- `blinear_v2` and `blinear_v3` are implemented from the referenced server-side format behavior and validated against the sample files in `reference/`.
