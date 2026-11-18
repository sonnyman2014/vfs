# Directory mutation coalescing for metadata storms (DJFS)

Refs #20. Tracked by HIP-1342. The library is `pkg/djfs`, the CLI benchmark
runner is `tools/djfsbench`, and the live `vfs.Volume` and `journal.Journal`
integration tests are in `djfs_test.go` and `pkg/djfs/coalesce_test.go`.

## 1. Primary reference, summary, and license audit

* **Primary paper:** *"DJFS: Providing Highly Scalable and Crash-Consistent
  Metadata Performance via Directory-Level Journaling"*, 23rd USENIX Conference
  on File and Storage Technologies (FAST 2025).
* **Summary:** Metadata-intensive workloads (`npm ci` / `pnpm install`,
  `mdtest`, `Varmail`, build-tree staging) concentrate bursts of short-lived
  creates, writes, renames, and unlinks inside hot directories while still
  requiring a single crash-consistent global ordering across cross-directory
  renames. Partitioning incoming namespace mutations into per-directory delta
  tables stamped with a global sequence number (LSN) and tracking explicit
  cross-directory rename dependencies allows each commit epoch to algebraically
  cancel ephemeral states in memory before touching persistent tree structures.
* **License and clean-room audit:** Everything in `pkg/djfs`, `tools/djfsbench`,
  `djfs_test.go`, and `docs/djfs.md` is an independent, clean-room Apache-2.0
  Go implementation written from first principles against `hanzoai/vfs`'s
  prolly Merkle tree, `journal.Journal`, and post-quantum `Hybrid` cipher. Zero
  external prototype code was copied or imported; `go.mod` and `go.sum` are
  untouched, and all packages build cleanly with `CGO_ENABLED=0` on
  `linux/amd64` and `darwin/arm64`.

## 2. Upstream root-cause finding in `hanzoai/vfs`

Auditing `volume.go`, `ops.go`, `overlay.go`, `tree.go`, and `shard.go`
identifies two compounding sources of object-store write amplification during
metadata storms:

1. **Phantom tombstones re-encrypt untouched prolly shards (`overlay.go:55`,
   `tree.go:315`, `shard.go:110`):** When an ephemeral file or `.staging/`
   directory is created (`v.over.put`) and subsequently removed (`v.over.del`)
   before `Flush`, `overlay` retains `onode{key: k, del: true}` even when `k`
   never existed in the committed `v.root` prolly tree. During `Flush`,
   `tree.pass()` seeks `v.root` to the 64 KiB leaf shard covering `k` and
   re-cuts and re-seals that leaf. Because `vfs.Hybrid` (X25519 + ML-KEM-768)
   is randomized encryption, re-sealing an unchanged plaintext node produces a
   new ciphertext and a new BLAKE3 `ref`, forcing `above()` to rewrite every
   ancestor shard up to `head` (`TestDJFSEliminatesPhantomTombstoneShardWrites`
   proves this directly on a live `Volume`).
2. **Staging churn and intermediate sync flushes (`npm ci` / `Varmail`):**
   Package installers extract tarballs into `.staging/<pkg>/...`, sync, rename
   into `node_modules/<pkg>/...`, write/delete `.lock.tmp` files, and unlink
   `.staging/`. Flushing during staging seals intermediate prolly shards for
   ephemeral entries and rewrites parent directory `Inode` records (`Mtime` /
   `Ctime`) repeatedly (`5.3-5.7` S3 `PUT`s per surviving file in PR #8).

## 3. Architecture & algebraic coalescing rules (`pkg/djfs`)

`pkg/djfs.Coalescer` sits in front of `vfs.Volume` (paired with
`journal.Journal` via `djfs.Session` for NVMe-speed group `fsync` and
asynchronous S3 publication):

* **64-way sharded lock hierarchy (deadlock-free):** `epochMu` (shared for
  workers, exclusive for `SealEpoch`) -> `dirShards[0..63]` (strictly ascending
  shard index) -> `inoShards[0..63]` (strictly ascending shard index) ->
  `depMu`. Single-directory mutations touch only one directory shard; cross-dir
  `Rename` locks `(min(s,d), max(s,d))` and records a `CrossDirDep{LSN, SrcDir,
  DstDir}` edge so both directories seal atomically in the same global commit
  cut.
* **Exact algebraic reductions at `SealEpoch()`:**
  * `Create(d, x) -> WriteInline* -> Remove(d, x)` (ephemeral birth + death):
    **`0` entry mutations, `0` inode mutations, `0` tombstones.**
  * `Create(d1, x) -> Rename(d1, x, ..., dk, y)` (staging-to-final rename chain):
    collapses to **`1` net `EntryMut` at `(dk, y)`** and **`0` mutations in
    `d1..dk-1`**.
  * `M` directory mutations inside directory `d` within one epoch collapse `M`
    parent `Inode` timestamp updates into **`1` parent update**.
  * Crash recovery (`ReplayOp` over Castagnoli-CRC32 journal frames) sorts
    recovered uncommitted records by global `LSN` before replay, preserving one
    deterministic global commit order.

## 4. Falsifiable negative control

When a workload performs **100% permanent monotonic file creates** (`0%`
deletes, `0%` renames, single final `Flush`), zero entries or inodes cancel
(`CancelledInodes = 0`). `benchNegativeControl` verifies that DJFS writes the
exact same shard/head count as raw `vfs.Volume` with bounded in-memory map
bookkeeping overhead.

## 5. Reproducing

```sh
go test -race -count=1 ./pkg/djfs
go test -race -count=1 -run '^TestDJFS' .
go run ./tools/djfsbench
go run ./tools/djfsbench -full
go run ./tools/djfsbench -json
```
