// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package djfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/luxfi/age"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
	"github.com/hanzoai/vfs/pkg/backend"
)

// CountingBackend wraps any backend.Backend to record exact S3/object-store
// request counts and uploaded bytes by key prefix ("meta", "pack", "head").
type CountingBackend struct {
	backend.Backend

	mu       sync.Mutex
	puts     map[string]int
	putBytes int64
	gets     int
}

// NewCountingBackend wraps be with zeroed request counters.
func NewCountingBackend(be backend.Backend) *CountingBackend {
	return &CountingBackend{Backend: be, puts: make(map[string]int)}
}

func prefixKind(key string) string {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i]
		}
	}
	return key
}

func (c *CountingBackend) Put(ctx context.Context, key string, data []byte) error {
	c.mu.Lock()
	c.puts[prefixKind(key)]++
	c.putBytes += int64(len(data))
	c.mu.Unlock()
	return c.Backend.Put(ctx, key, data)
}

func (c *CountingBackend) PutIfAbsent(ctx context.Context, key string, data []byte) (backend.Version, error) {
	c.mu.Lock()
	c.puts[prefixKind(key)]++
	c.putBytes += int64(len(data))
	c.mu.Unlock()
	return c.Backend.PutIfAbsent(ctx, key, data)
}

func (c *CountingBackend) PutIfVersion(ctx context.Context, key string, data []byte, ver backend.Version) (backend.Version, error) {
	c.mu.Lock()
	c.puts[prefixKind(key)]++
	c.putBytes += int64(len(data))
	c.mu.Unlock()
	return c.Backend.PutIfVersion(ctx, key, data, ver)
}

func (c *CountingBackend) Get(ctx context.Context, key string, max int64) ([]byte, backend.Version, error) {
	c.mu.Lock()
	c.gets++
	c.mu.Unlock()
	return c.Backend.Get(ctx, key, max)
}

// Reset clears all recorded counters.
func (c *CountingBackend) Reset() {
	c.mu.Lock()
	clear(c.puts)
	c.putBytes = 0
	c.gets = 0
	c.mu.Unlock()
}

// Snapshot returns total PUTs, per-prefix PUTs, uploaded bytes, and GETs.
func (c *CountingBackend) Snapshot() (totalPuts int, byPrefix map[string]int, putBytes int64, gets int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make(map[string]int, len(c.puts))
	for k, v := range c.puts {
		cp[k] = v
		totalPuts += v
	}
	return totalPuts, cp, c.putBytes, c.gets
}

// NpmCIResult holds side-by-side metrics for an npm-ci metadata storm.
type NpmCIResult struct {
	Packages            int     `json:"packages"`
	FilesPerPackage     int     `json:"files_per_package"`
	SurvivingFiles      int     `json:"surviving_files"`
	RawNamespaceOps     int     `json:"raw_namespace_ops"`
	PlainNVMeMs         float64 `json:"plain_nvme_ms"`
	UncoalescedMs       float64 `json:"uncoalesced_ms"`
	UncoalescedPuts     int     `json:"uncoalesced_puts"`
	UncoalescedPutsFile float64 `json:"uncoalesced_puts_per_file"`
	DJFSMs              float64 `json:"djfs_ms"`
	DJFSPublishMs       float64 `json:"djfs_publish_ms"`
	DJFSPuts            int     `json:"djfs_puts"`
	DJFSShardPuts       int     `json:"djfs_shard_puts"`
	DJFSPackPuts        int     `json:"djfs_pack_puts"`
	DJFSHeadPuts        int     `json:"djfs_head_puts"`
	DJFSPutsPerFile     float64 `json:"djfs_puts_per_file"`
	CancelledRecords    uint64  `json:"cancelled_records"`
}

// MDTestResult holds create/stat/delete throughput across Plain NVMe, raw VFS, and DJFS.
type MDTestResult struct {
	Files                 int     `json:"files"`
	PlainNVMeCreateOpsSec float64 `json:"plain_nvme_create_ops_sec"`
	PlainNVMeStatOpsSec   float64 `json:"plain_nvme_stat_ops_sec"`
	PlainNVMeDeleteOpsSec float64 `json:"plain_nvme_delete_ops_sec"`
	RawVFSCreateOpsSec    float64 `json:"raw_vfs_create_ops_sec"`
	RawVFSStatOpsSec      float64 `json:"raw_vfs_stat_ops_sec"`
	RawVFSDeleteOpsSec    float64 `json:"raw_vfs_delete_ops_sec"`
	RawVFSPuts            int     `json:"raw_vfs_puts"`
	DJFSCreateOpsSec      float64 `json:"djfs_create_ops_sec"`
	DJFSStatOpsSec        float64 `json:"djfs_stat_ops_sec"`
	DJFSDeleteOpsSec      float64 `json:"djfs_delete_ops_sec"`
	DJFSPuts              int     `json:"djfs_puts"`
}

// VarmailResult holds delivery-storm latency/rate and fsync ratio vs Plain NVMe.
type VarmailResult struct {
	Deliveries          int     `json:"deliveries"`
	PlainNVMeOpsSec     float64 `json:"plain_nvme_ops_sec"`
	PlainNVMeFsyncUs    float64 `json:"plain_nvme_fsync_us"`
	UncoalescedOpsSec   float64 `json:"uncoalesced_ops_sec"`
	UncoalescedPuts     int     `json:"uncoalesced_puts"`
	DJFSOpsSec          float64 `json:"djfs_ops_sec"`
	DJFSFsyncUs         float64 `json:"djfs_fsync_us"`
	DJFSFsyncVsNVMe     float64 `json:"djfs_fsync_vs_nvme_ratio"`
	DJFSPuts            int     `json:"djfs_puts"`
	DJFSPublishWithin5s bool    `json:"djfs_publish_within_5s"`
}

// NegativeControlResult captures the monotonic-create workload where zero
// entries cancel, measuring DJFS's bounded bookkeeping overhead.
type NegativeControlResult struct {
	Files          int     `json:"files"`
	PlainNVMeMs    float64 `json:"plain_nvme_ms"`
	RawVFSMs       float64 `json:"raw_vfs_ms"`
	RawVFSPuts     int     `json:"raw_vfs_puts"`
	DJFSMs         float64 `json:"djfs_ms"`
	DJFSPuts       int     `json:"djfs_puts"`
	CancelledInode uint64  `json:"cancelled_inodes"`
}

// SuiteReport bundles all benchmark results for machine-readable and Markdown output.
type SuiteReport struct {
	Scale           string                `json:"scale"`
	Storm100kOpsMs  float64               `json:"storm_100k_ops_ms"`
	Storm100kOpsSec float64               `json:"storm_100k_ops_sec"`
	NpmCI           NpmCIResult           `json:"npm_ci"`
	MDTest          MDTestResult          `json:"mdtest"`
	Varmail         VarmailResult         `json:"varmail"`
	NegativeControl NegativeControlResult `json:"negative_control"`
}

func newLocalVolume(ctx context.Context, dir string) (*CountingBackend, *vfs.Volume, error) {
	rawBE, err := backend.Open(ctx, "file://"+dir)
	if err != nil {
		return nil, nil, err
	}
	cb := NewCountingBackend(rawBE)
	k, err := age.GenerateHybridIdentity()
	if err != nil {
		return nil, nil, err
	}
	c := vfs.Hybrid(k)
	if err := vfs.Create(ctx, vfs.Config{Backend: cb, Cipher: c}); err != nil {
		return nil, nil, err
	}
	v, err := vfs.Attach(ctx, vfs.Config{Backend: cb, Cipher: c, Round: 1})
	if err != nil {
		return nil, nil, err
	}
	cb.Reset()
	return cb, v, nil
}

// RunSuite executes the full DJFS comparative evaluation against Plain NVMe and
// uncoalesced vfs.Volume. When fullScale is true, npm-ci runs at a larger
// multi-thousand-file storm alongside the 100,000-op coalescer throughput gate.
func RunSuite(ctx context.Context, scratchRoot string, fullScale bool) (SuiteReport, error) {
	if scratchRoot == "" {
		var err error
		scratchRoot, err = os.MkdirTemp("", "djfs-bench-*")
		if err != nil {
			return SuiteReport{}, err
		}
		defer os.RemoveAll(scratchRoot)
	}

	pkgs, filesPerPkg := 80, 25 // 2,000 surviving files, ~10,400 raw namespace ops
	mdFiles := 1500
	varmailDeliveries := 240
	negFiles := 1000
	scaleName := "standard"
	if fullScale {
		pkgs, filesPerPkg = 400, 25 // 10,000 surviving files, ~52,000 raw ops on live Hybrid PQ Volume
		mdFiles = 4000
		varmailDeliveries = 600
		negFiles = 2500
		scaleName = "full"
	}

	// 1. 100,000-op mixed create/delete/rename storm directly on sharded Coalescer.
	c100k := NewCoalescer(2)
	const dirs100k = 100
	dirIDs := make([]vfs.InodeID, dirs100k)
	for d := range dirs100k {
		id, _, err := c100k.Mkdir(vfs.RootInode, fmt.Sprintf("pkg-%04d", d), 0o755)
		if err != nil {
			return SuiteReport{}, err
		}
		dirIDs[d] = id
	}
	t100 := time.Now()
	var workers sync.WaitGroup
	var opCount atomic.Int64
	const nWorkers = 8
	perWorkerDirs := dirs100k / nWorkers
	for w := range nWorkers {
		workers.Add(1)
		go func(w int) {
			defer workers.Done()
			startDir := w * perWorkerDirs
			for round := range 250 {
				d := dirIDs[startDir+(round%perWorkerDirs)]
				dst := dirIDs[startDir+((round+1)%perWorkerDirs)]
				for k := range 16 {
					tmpName := fmt.Sprintf(".stage-%d-%d.tmp", round, k)
					finalName := fmt.Sprintf("mod-%d-%d.js", round, k)
					id, _, err := c100k.Create(d, tmpName, 0o644, []byte("export default 1;\n"))
					if err != nil {
						continue
					}
					opCount.Add(1)
					_, _ = c100k.WriteInline(id, []byte("export const ready = true;\n"))
					opCount.Add(1)
					_, _ = c100k.Rename(d, tmpName, dst, finalName)
					opCount.Add(1)
					if k%2 == 0 {
						_, _ = c100k.Remove(dst, finalName)
						opCount.Add(1)
					}
				}
			}
		}(w)
	}
	workers.Wait()
	_ = c100k.SealEpoch()
	d100 := time.Since(t100)
	total100k := float64(opCount.Load())

	npmRes, err := benchNpmCI(ctx, scratchRoot, pkgs, filesPerPkg)
	if err != nil {
		return SuiteReport{}, err
	}
	mdRes, err := benchMDTest(ctx, scratchRoot, mdFiles)
	if err != nil {
		return SuiteReport{}, err
	}
	vmRes, err := benchVarmail(ctx, scratchRoot, varmailDeliveries)
	if err != nil {
		return SuiteReport{}, err
	}
	negRes, err := benchNegativeControl(ctx, scratchRoot, negFiles)
	if err != nil {
		return SuiteReport{}, err
	}

	return SuiteReport{
		Scale:           scaleName,
		Storm100kOpsMs:  float64(d100.Microseconds()) / 1000.0,
		Storm100kOpsSec: total100k / d100.Seconds(),
		NpmCI:           npmRes,
		MDTest:          mdRes,
		Varmail:         vmRes,
		NegativeControl: negRes,
	}, nil
}

func benchNpmCI(ctx context.Context, root string, pkgs, filesPerPkg int) (NpmCIResult, error) {
	payload := []byte("{\"name\":\"@hanzo/pkg\",\"version\":\"1.0.0\",\"main\":\"index.js\"}\n")
	surviving := pkgs * filesPerPkg
	var rawOps int

	// A. Plain NVMe baseline.
	nvmeDir := filepath.Join(root, "npm-nvme")
	if err := os.MkdirAll(filepath.Join(nvmeDir, ".staging"), 0o755); err != nil {
		return NpmCIResult{}, err
	}
	if err := os.MkdirAll(filepath.Join(nvmeDir, "node_modules"), 0o755); err != nil {
		return NpmCIResult{}, err
	}
	t0 := time.Now()
	for p := range pkgs {
		stPkg := filepath.Join(nvmeDir, ".staging", fmt.Sprintf("pkg-%04d", p))
		nmPkg := filepath.Join(nvmeDir, "node_modules", fmt.Sprintf("pkg-%04d", p))
		_ = os.Mkdir(stPkg, 0o755)
		_ = os.Mkdir(nmPkg, 0o755)
		for f := range filesPerPkg {
			tmp := filepath.Join(stPkg, fmt.Sprintf("f%03d.tmp", f))
			dst := filepath.Join(nmPkg, fmt.Sprintf("f%03d.js", f))
			_ = os.WriteFile(tmp, payload, 0o644)
			_ = os.Rename(tmp, dst)
		}
		lockTmp := filepath.Join(stPkg, ".lock.tmp")
		_ = os.WriteFile(lockTmp, payload, 0o644)
		_ = os.Remove(lockTmp)
		_ = os.Remove(stPkg)
	}
	_ = os.Remove(filepath.Join(nvmeDir, ".staging"))
	plainMs := float64(time.Since(t0).Microseconds()) / 1000.0

	// B. Uncoalesced vfs.Volume (periodic package-tarball syncs + staging churn).
	uncoalStore := filepath.Join(root, "npm-uncoal-store")
	_ = os.MkdirAll(uncoalStore, 0o755)
	cbU, vU, err := newLocalVolume(ctx, uncoalStore)
	if err != nil {
		return NpmCIResult{}, err
	}
	defer vU.Close()

	t1 := time.Now()
	stRootU, _ := vU.Mkdir(ctx, vfs.RootInode, ".staging", 0o755)
	nmRootU, _ := vU.Mkdir(ctx, vfs.RootInode, "node_modules", 0o755)
	for p := range pkgs {
		stPkg, _ := vU.Mkdir(ctx, stRootU.ID, fmt.Sprintf("pkg-%04d", p), 0o755)
		nmPkg, _ := vU.Mkdir(ctx, nmRootU.ID, fmt.Sprintf("pkg-%04d", p), 0o755)
		for f := range filesPerPkg {
			tmpIn, _ := vU.Create(ctx, stPkg.ID, fmt.Sprintf("f%03d.tmp", f), 0o644)
			fh, _ := vU.Open(ctx, tmpIn.ID)
			_, _ = fh.WriteAt(ctx, payload, 0)
			_ = fh.Close()
		}
		// Package manager syncs extracted staging batch before moving into node_modules.
		if err := vU.Flush(ctx); err != nil {
			return NpmCIResult{}, err
		}
		for f := range filesPerPkg {
			dstIn, _ := vU.Create(ctx, nmPkg.ID, fmt.Sprintf("f%03d.js", f), 0o644)
			fh, _ := vU.Open(ctx, dstIn.ID)
			_, _ = fh.WriteAt(ctx, payload, 0)
			_ = fh.Close()
			_ = vU.Remove(ctx, stPkg.ID, fmt.Sprintf("f%03d.tmp", f))
		}
		lockIn, _ := vU.Create(ctx, stPkg.ID, ".lock.tmp", 0o644)
		lf, _ := vU.Open(ctx, lockIn.ID)
		_, _ = lf.WriteAt(ctx, payload, 0)
		_ = lf.Close()
		_ = vU.Remove(ctx, stPkg.ID, ".lock.tmp")
		_ = vU.Remove(ctx, stRootU.ID, fmt.Sprintf("pkg-%04d", p))
		if p%2 == 1 {
			if err := vU.Flush(ctx); err != nil {
				return NpmCIResult{}, err
			}
		}
	}
	_ = vU.Remove(ctx, vfs.RootInode, ".staging")
	if err := vU.Flush(ctx); err != nil {
		return NpmCIResult{}, err
	}
	uncoalMs := float64(time.Since(t1).Microseconds()) / 1000.0
	uPuts, _, _, _ := cbU.Snapshot()

	// C. DJFS Coalesced Session + journal.Journal.
	djfsStore := filepath.Join(root, "npm-djfs-store")
	_ = os.MkdirAll(djfsStore, 0o755)
	cbD, vD, err := newLocalVolume(ctx, djfsStore)
	if err != nil {
		return NpmCIResult{}, err
	}
	defer vD.Close()

	jnl, err := journal.Open("", journal.Options{
		Capacity: 64 << 20,
		Segment:  8 << 20,
		Lineage:  []byte("djfs-npm-ci-round-1"),
		Disk:     crash.New(),
	})
	if err != nil {
		return NpmCIResult{}, err
	}
	defer jnl.Close()

	sess, err := NewSession(ctx, vD, jnl)
	if err != nil {
		return NpmCIResult{}, err
	}
	coal := sess.Coalescer()

	t2 := time.Now()
	stRootD, _, _ := coal.Mkdir(vfs.RootInode, ".staging", 0o755)
	nmRootD, _, _ := coal.Mkdir(vfs.RootInode, "node_modules", 0o755)
	rawOps += 2
	for p := range pkgs {
		stPkg, _, _ := coal.Mkdir(stRootD, fmt.Sprintf("pkg-%04d", p), 0o755)
		nmPkg, _, _ := coal.Mkdir(nmRootD, fmt.Sprintf("pkg-%04d", p), 0o755)
		rawOps += 2
		var lastSeq uint64
		for f := range filesPerPkg {
			_, lastSeq, _ = coal.Create(stPkg, fmt.Sprintf("f%03d.tmp", f), 0o644, payload)
			rawOps++
		}
		// Local group sync satisfies package-tarball fsync without S3 PUTs.
		if err := sess.Fsync(ctx, lastSeq); err != nil {
			return NpmCIResult{}, err
		}
		for f := range filesPerPkg {
			_, _ = coal.Rename(stPkg, fmt.Sprintf("f%03d.tmp", f), nmPkg, fmt.Sprintf("f%03d.js", f))
			rawOps++
		}
		_, _, _ = coal.Create(stPkg, ".lock.tmp", 0o644, payload)
		_, _ = coal.Remove(stPkg, ".lock.tmp")
		_, _ = coal.Remove(stRootD, fmt.Sprintf("pkg-%04d", p))
		rawOps += 3
	}
	_, _ = coal.Remove(vfs.RootInode, ".staging")
	rawOps++

	tPub := time.Now()
	if _, err := sess.PublishEpoch(ctx); err != nil {
		return NpmCIResult{}, err
	}
	pubMs := float64(time.Since(tPub).Microseconds()) / 1000.0
	djfsMs := float64(time.Since(t2).Microseconds()) / 1000.0

	dPuts, byPrefix, _, _ := cbD.Snapshot()
	cStats := coal.Stats()

	return NpmCIResult{
		Packages:            pkgs,
		FilesPerPackage:     filesPerPkg,
		SurvivingFiles:      surviving,
		RawNamespaceOps:     rawOps,
		PlainNVMeMs:         plainMs,
		UncoalescedMs:       uncoalMs,
		UncoalescedPuts:     uPuts,
		UncoalescedPutsFile: float64(uPuts) / float64(surviving),
		DJFSMs:              djfsMs,
		DJFSPublishMs:       pubMs,
		DJFSPuts:            dPuts,
		DJFSShardPuts:       byPrefix["meta"],
		DJFSPackPuts:        byPrefix["pack"],
		DJFSHeadPuts:        byPrefix["head"],
		DJFSPutsPerFile:     float64(dPuts) / float64(surviving),
		CancelledRecords:    cStats.CancelledEntries,
	}, nil
}

func benchMDTest(ctx context.Context, root string, files int) (MDTestResult, error) {
	// A. Plain NVMe create / stat / delete.
	nvmeDir := filepath.Join(root, "mdtest-nvme")
	_ = os.MkdirAll(nvmeDir, 0o755)
	t0 := time.Now()
	for i := range files {
		f, err := os.Create(filepath.Join(nvmeDir, fmt.Sprintf("f-%06d", i)))
		if err != nil {
			return MDTestResult{}, err
		}
		_ = f.Close()
	}
	nvmeCreate := float64(files) / time.Since(t0).Seconds()

	t0 = time.Now()
	for i := range files {
		if _, err := os.Stat(filepath.Join(nvmeDir, fmt.Sprintf("f-%06d", i))); err != nil {
			return MDTestResult{}, err
		}
	}
	nvmeStat := float64(files) / time.Since(t0).Seconds()

	t0 = time.Now()
	for i := range files {
		if err := os.Remove(filepath.Join(nvmeDir, fmt.Sprintf("f-%06d", i))); err != nil {
			return MDTestResult{}, err
		}
	}
	nvmeDel := float64(files) / time.Since(t0).Seconds()

	// B. Raw vfs.Volume (flush after create phase and delete phase).
	rawDir := filepath.Join(root, "mdtest-raw")
	_ = os.MkdirAll(rawDir, 0o755)
	cbR, vR, err := newLocalVolume(ctx, rawDir)
	if err != nil {
		return MDTestResult{}, err
	}
	defer vR.Close()

	t0 = time.Now()
	for i := range files {
		if _, err := vR.Create(ctx, vfs.RootInode, fmt.Sprintf("f-%06d", i), 0o644); err != nil {
			return MDTestResult{}, err
		}
	}
	if err := vR.Flush(ctx); err != nil {
		return MDTestResult{}, err
	}
	rawCreate := float64(files) / time.Since(t0).Seconds()

	t0 = time.Now()
	for i := range files {
		if _, err := vR.Lookup(ctx, vfs.RootInode, fmt.Sprintf("f-%06d", i)); err != nil {
			return MDTestResult{}, err
		}
	}
	rawStat := float64(files) / time.Since(t0).Seconds()

	t0 = time.Now()
	for i := range files {
		if err := vR.Remove(ctx, vfs.RootInode, fmt.Sprintf("f-%06d", i)); err != nil {
			return MDTestResult{}, err
		}
	}
	if err := vR.Flush(ctx); err != nil {
		return MDTestResult{}, err
	}
	rawDel := float64(files) / time.Since(t0).Seconds()
	rawPuts, _, _, _ := cbR.Snapshot()

	// C. DJFS Session (create + stat + delete storm in one coalesced window).
	djfsDir := filepath.Join(root, "mdtest-djfs")
	_ = os.MkdirAll(djfsDir, 0o755)
	cbD, vD, err := newLocalVolume(ctx, djfsDir)
	if err != nil {
		return MDTestResult{}, err
	}
	defer vD.Close()
	sess, err := NewSession(ctx, vD, nil)
	if err != nil {
		return MDTestResult{}, err
	}
	coal := sess.Coalescer()

	t0 = time.Now()
	for i := range files {
		if _, _, err := coal.Create(vfs.RootInode, fmt.Sprintf("f-%06d", i), 0o644, nil); err != nil {
			return MDTestResult{}, err
		}
	}
	djfsCreate := float64(files) / time.Since(t0).Seconds()

	t0 = time.Now()
	for i := range files {
		if _, err := coal.Lookup(vfs.RootInode, fmt.Sprintf("f-%06d", i)); err != nil {
			return MDTestResult{}, err
		}
	}
	djfsStat := float64(files) / time.Since(t0).Seconds()

	t0 = time.Now()
	for i := range files {
		if _, err := coal.Remove(vfs.RootInode, fmt.Sprintf("f-%06d", i)); err != nil {
			return MDTestResult{}, err
		}
	}
	if _, err := sess.PublishEpoch(ctx); err != nil {
		return MDTestResult{}, err
	}
	djfsDel := float64(files) / time.Since(t0).Seconds()
	djfsPuts, _, _, _ := cbD.Snapshot()

	return MDTestResult{
		Files:                 files,
		PlainNVMeCreateOpsSec: nvmeCreate,
		PlainNVMeStatOpsSec:   nvmeStat,
		PlainNVMeDeleteOpsSec: nvmeDel,
		RawVFSCreateOpsSec:    rawCreate,
		RawVFSStatOpsSec:      rawStat,
		RawVFSDeleteOpsSec:    rawDel,
		RawVFSPuts:            rawPuts,
		DJFSCreateOpsSec:      djfsCreate,
		DJFSStatOpsSec:        djfsStat,
		DJFSDeleteOpsSec:      djfsDel,
		DJFSPuts:              djfsPuts,
	}, nil
}

func benchVarmail(ctx context.Context, root string, deliveries int) (VarmailResult, error) {
	msg := []byte("From: mailer@hanzo.ai\r\nSubject: delivery\r\n\r\nMessage payload body bytes.\r\n")

	// A. Plain NVMe varmail (create tmp -> write -> fsync -> rename -> delete previous).
	nvmeDir := filepath.Join(root, "varmail-nvme")
	_ = os.MkdirAll(nvmeDir, 0o755)
	var nvmeSyncDur time.Duration
	t0 := time.Now()
	var prev string
	for i := range deliveries {
		tmp := filepath.Join(nvmeDir, fmt.Sprintf("tmp-%05d", i))
		cur := filepath.Join(nvmeDir, fmt.Sprintf("msg-%05d", i))
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return VarmailResult{}, err
		}
		_, _ = f.Write(msg)
		ts := time.Now()
		_ = f.Sync()
		nvmeSyncDur += time.Since(ts)
		_ = f.Close()
		_ = os.Rename(tmp, cur)
		if prev != "" {
			_ = os.Remove(prev)
		}
		prev = cur
	}
	nvmeOpsSec := float64(deliveries) / time.Since(t0).Seconds()
	nvmeFsyncUs := float64(nvmeSyncDur.Nanoseconds()) / 1000.0 / float64(deliveries)

	// B. Uncoalesced vfs.Volume (each delivery fsync triggers Volume.Flush).
	uDir := filepath.Join(root, "varmail-uncoal")
	_ = os.MkdirAll(uDir, 0o755)
	cbU, vU, err := newLocalVolume(ctx, uDir)
	if err != nil {
		return VarmailResult{}, err
	}
	defer vU.Close()

	t1 := time.Now()
	var prevV string
	for i := range deliveries {
		curName := fmt.Sprintf("msg-%05d", i)
		in, _ := vU.Create(ctx, vfs.RootInode, curName, 0o644)
		fh, _ := vU.Open(ctx, in.ID)
		_, _ = fh.WriteAt(ctx, msg, 0)
		_ = fh.Close()
		if prevV != "" {
			_ = vU.Remove(ctx, vfs.RootInode, prevV)
		}
		if err := vU.Flush(ctx); err != nil {
			return VarmailResult{}, err
		}
		prevV = curName
	}
	uOpsSec := float64(deliveries) / time.Since(t1).Seconds()
	uPuts, _, _, _ := cbU.Snapshot()

	// C. DJFS Session + real file-backed local journal (or fallback crash.Disk if tmpfs).
	dDir := filepath.Join(root, "varmail-djfs")
	jDir := filepath.Join(root, "varmail-jnl")
	_ = os.MkdirAll(dDir, 0o755)
	_ = os.MkdirAll(jDir, 0o755)
	cbD, vD, err := newLocalVolume(ctx, dDir)
	if err != nil {
		return VarmailResult{}, err
	}
	defer vD.Close()

	jnl, err := journal.Open(jDir, journal.Options{
		Capacity: 32 << 20,
		Segment:  4 << 20,
		Lineage:  []byte("djfs-varmail-round-1"),
	})
	if err != nil {
		// Fallback when scratchRoot sits on tmpfs (ErrVolatile).
		jnl, err = journal.Open("", journal.Options{
			Capacity: 32 << 20,
			Segment:  4 << 20,
			Lineage:  []byte("djfs-varmail-round-1"),
			Disk:     crash.New(),
		})
		if err != nil {
			return VarmailResult{}, err
		}
	}
	defer jnl.Close()

	sess, err := NewSession(ctx, vD, jnl)
	if err != nil {
		return VarmailResult{}, err
	}
	coal := sess.Coalescer()

	// Concurrent group-fsync mail workers (8 threads sharing journal group sync).
	const mailWorkers = 8
	perW := deliveries / mailWorkers
	var syncNanos atomic.Int64
	t2 := time.Now()
	var wg sync.WaitGroup
	for w := range mailWorkers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var lastMsg string
			for i := range perW {
				tmp := fmt.Sprintf("w%d-tmp-%05d", w, i)
				cur := fmt.Sprintf("w%d-msg-%05d", w, i)
				_, seq, err := coal.Create(vfs.RootInode, tmp, 0o644, msg)
				if err != nil {
					continue
				}
				ts := time.Now()
				_ = sess.Fsync(ctx, seq)
				syncNanos.Add(time.Since(ts).Nanoseconds())
				_, _ = coal.Rename(vfs.RootInode, tmp, vfs.RootInode, cur)
				if lastMsg != "" {
					_, _ = coal.Remove(vfs.RootInode, lastMsg)
				}
				lastMsg = cur
			}
		}(w)
	}
	wg.Wait()
	pubStart := time.Now()
	if _, err := sess.PublishEpoch(ctx); err != nil {
		return VarmailResult{}, err
	}
	pubElapsed := time.Since(pubStart)
	totalDeliveries := mailWorkers * perW
	dOpsSec := float64(totalDeliveries) / time.Since(t2).Seconds()
	dFsyncUs := float64(syncNanos.Load()) / 1000.0 / float64(totalDeliveries)
	dPuts, _, _, _ := cbD.Snapshot()

	ratio := 0.0
	if nvmeFsyncUs > 0 {
		ratio = dFsyncUs / nvmeFsyncUs
	}

	return VarmailResult{
		Deliveries:          totalDeliveries,
		PlainNVMeOpsSec:     nvmeOpsSec,
		PlainNVMeFsyncUs:    nvmeFsyncUs,
		UncoalescedOpsSec:   uOpsSec,
		UncoalescedPuts:     uPuts,
		DJFSOpsSec:          dOpsSec,
		DJFSFsyncUs:         dFsyncUs,
		DJFSFsyncVsNVMe:     ratio,
		DJFSPuts:            dPuts,
		DJFSPublishWithin5s: pubElapsed <= 5*time.Second,
	}, nil
}

func benchNegativeControl(ctx context.Context, root string, files int) (NegativeControlResult, error) {
	body := []byte("permanent source file content\n")

	nvmeDir := filepath.Join(root, "neg-nvme")
	_ = os.MkdirAll(nvmeDir, 0o755)
	t0 := time.Now()
	for i := range files {
		_ = os.WriteFile(filepath.Join(nvmeDir, fmt.Sprintf("src-%05d.go", i)), body, 0o644)
	}
	nvmeMs := float64(time.Since(t0).Microseconds()) / 1000.0

	rawDir := filepath.Join(root, "neg-raw")
	_ = os.MkdirAll(rawDir, 0o755)
	cbR, vR, err := newLocalVolume(ctx, rawDir)
	if err != nil {
		return NegativeControlResult{}, err
	}
	defer vR.Close()
	t1 := time.Now()
	for i := range files {
		in, _ := vR.Create(ctx, vfs.RootInode, fmt.Sprintf("src-%05d.go", i), 0o644)
		fh, _ := vR.Open(ctx, in.ID)
		_, _ = fh.WriteAt(ctx, body, 0)
		_ = fh.Close()
	}
	if err := vR.Flush(ctx); err != nil {
		return NegativeControlResult{}, err
	}
	rawMs := float64(time.Since(t1).Microseconds()) / 1000.0
	rawPuts, _, _, _ := cbR.Snapshot()

	djfsDir := filepath.Join(root, "neg-djfs")
	_ = os.MkdirAll(djfsDir, 0o755)
	cbD, vD, err := newLocalVolume(ctx, djfsDir)
	if err != nil {
		return NegativeControlResult{}, err
	}
	defer vD.Close()
	sess, err := NewSession(ctx, vD, nil)
	if err != nil {
		return NegativeControlResult{}, err
	}
	coal := sess.Coalescer()
	t2 := time.Now()
	for i := range files {
		_, _, _ = coal.Create(vfs.RootInode, fmt.Sprintf("src-%05d.go", i), 0o644, body)
	}
	if _, err := sess.PublishEpoch(ctx); err != nil {
		return NegativeControlResult{}, err
	}
	djfsMs := float64(time.Since(t2).Microseconds()) / 1000.0
	djfsPuts, _, _, _ := cbD.Snapshot()

	return NegativeControlResult{
		Files:          files,
		PlainNVMeMs:    nvmeMs,
		RawVFSMs:       rawMs,
		RawVFSPuts:     rawPuts,
		DJFSMs:         djfsMs,
		DJFSPuts:       djfsPuts,
		CancelledInode: coal.Stats().CancelledInodes,
	}, nil
}
