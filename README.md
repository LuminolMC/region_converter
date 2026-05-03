# region_converter

Rust command-line tool for converting Minecraft Java Edition region saves between:

- `mca` (`.mca`)
- `linear` (`.linear`)
- `blinear_v2` (`.b_linear`)
- `blinear_v3` (`.b_linear`)

## Features

- Parallel conversion with a configurable worker count
- Automatic use of all available CPU threads when `--threads` is not set
- Compression level control for compressed target formats
- Works on Windows and Linux
- Skips corrupted chunks when the format allows chunk-level recovery
- Fails corrupted whole-region inputs without producing partial garbage
- Accepts one or more world directories or region directories

## Build

```bash
cargo build --release
```

## Usage

```bash
cargo run --release -- \
  --to blinear_v3 \
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
  --to blinear_v3 \
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
  --from blinear_v2 \
  --to linear \
  --output /data/out \
  /data/world
```

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
