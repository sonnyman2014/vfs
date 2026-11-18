# Read-only lookup index for immutable base images and snapshots (CoFS)

Refs #25. Tracked by HIP-1342. The library is `pkg/cofs`, the CLI benchmark
runner is `tools/cofsbench`, and the live `vfs.Volume` integration tests are in
`cofs_test.go` and `pkg/cofs/index_test.go`.

## 1. Primary reference, summary, and license audit

* **Primary paper:** CoFS — minimal-perfect-hash read-only metadata indexing for
  immutable container base images and snapshots, 24th USENIX Conference on File
  and Storage Technologies (FAST 2026).
* **Summary:** Sandbox environments (`hanzoai/visor` mounting `hanzoai/vfs` over
  `hanzoai/s3`) clone a read-only base image (OS userland, compiler toolchains,
  shared language runtimes) beneath a thin per-sandbox mutable workspace.
  Whereas mutable workspaces require dynamic tree updates, base images and
  snapshots are immutable: indexing their metadata once into a compact minimal
  perfect hash table eliminates repeated multi-level tree descents, negative
  `ENOENT` search stalls (`PATH`, `LD_LIBRARY_PATH`, `node_modules`), and cold
  metadata-shard round trips on sandbox startup.
* **License and clean-room audit:** Everything in `pkg/cofs`, `tools/cofsbench`,
  `cofs_test.go`, and `docs/cofs.md` is an independent, clean-room Apache-2.0
  Go implementation written from first principles using only the Go standard
  library, `github.com/zeebo/blake3` (already in `go.mod`), and `hanzoai/vfs`.
  `go.mod` and `go.sum` are untouched, and all packages build cleanly with
  `CGO_ENABLED=0` on `linux/amd64` and `darwin/arm64`.

## 2. Upstream lookup cost on immutable `vfs` snapshots

In `ops.go` (`Resolve`, `Lookup`) and `tree.go` (`get`), resolving a `D`-segment
path such as `/usr/lib/tc-00/packages/pkg-0000/mod-000.go` (`D = 6`) executes
`Stat(RootInode)` followed by `D` `Lookup(dir, name)` calls. Because `[tInode]`
(`[1][ino]`) and `[tEntry]` (`[2][dirIno][name]`) occupy disjoint prefixes of
the prolly tree, each segment performs two root-to-leaf tree walks (`1 + 2D =
13` tree queries). On a freshly cloned sandbox:

1. Cold lookups issue serial S3 `GET`s for uncached 64 KiB `meta/` shards and
   decrypt each shard through post-quantum `Hybrid` (`X25519 + ML-KEM-768`).
2. `ENOENT` misses during `PATH` and module-resolution scans still walk down to
   `[tEntry]` leaf shards to confirm absence.
3. Even when warm in `shards.cache`, `Resolve` acquires `Volume.mu.RLock` `7`
   times, runs ~`26-39` binary searches over `[][]byte` node slices, and decodes
   `Inode` varint tags on every hop.

## 3. Architecture (`pkg/cofs`)

1. **CHD + Singleton-Direct Minimal Perfect Hash (`mphf.go`):**
   * Maps `n` distinct keys bijectively onto `[0, n)` (`100.0%` slot utilization,
     zero empty slots) using keyed 192-bit `BLAKE3` bucket/displacement hashes
     derived from a snapshot seed.
   * Multi-key buckets (`lambda = 4`) are displaced in descending size order
     while ~`30%` of slots remain free; singleton buckets encode their assigned
     free slot directly via `directSlotBit`, avoiding coupon-collector tail
     probing during `Build` and guaranteeing single-probe `O(1)` lookup.
2. **Exact, Zero-False-Positive Verification & Zero Path-String Duplication (`index.go`):**
   * `entryMPH` indexes `(parentIno, name)` -> `(entryIdx, fp32)` backed by a
     single flat `nameArena []byte` and contiguous CSR directory slices
     (`[dirStart : dirStart+dirCount]`), serving `Lookup(dir, name)` and
     `ReadDir(dir)` in `O(1)` with exact `(parentIno, name)` equality checks
     (`0.00%` false positives across adversarial collisions).
   * `pathMPH` indexes canonical clean paths `"/a/b/c"` -> `entryIdx` and
     verifies path segments right-to-left up `parentEntry` against `nameArena`
     with **zero extra path-arena bytes** and zero heap allocations.
3. **Encrypted Extension Persistence (`SPECIFICATION 3.1`):**
   * `Index.SaveToVolume` and `LoadFromVolume` store the BLAKE3-checksummed
     binary index under `Volume.PutExt(ctx, "cofs", snapshotKey, raw)` (`ext.go`).
   * Every byte crosses `Volume.Cipher` (Rule 1: zero plaintext filenames or
     paths in backend storage), and `head` / `meta/` / `pack/` formats remain
     completely unchanged.
4. **Strict Base-Image vs Mutable-Workspace Separation (`layered.go`):**
   * `LayeredResolver` serves unmodified base-image paths from `cofs.Index` and
     routes dirty paths and directory/file whiteouts to the mutable workspace
     `vfs.Volume` without ever rebuilding the immutable base MPHF.

## 4. Reproducing

```sh
go test -race -count=1 ./pkg/cofs
go test -race -count=1 -run '^TestCoFS' .
go run ./tools/cofsbench
go run ./tools/cofsbench -full
go run ./tools/cofsbench -json
```
