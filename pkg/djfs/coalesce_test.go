// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package djfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
	"github.com/hanzoai/vfs/pkg/djfs"
)

func TestCoalesceAlgebraicCancellation(t *testing.T) {
	c := djfs.NewCoalescer(2)

	// 1. Ephemeral create + write + remove -> 0 net entry/inode mutations.
	tmpID, _, err := c.Create(vfs.RootInode, "ephemeral.tmp", 0o644, []byte("draft"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteInline(tmpID, []byte("updated draft")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Remove(vfs.RootInode, "ephemeral.tmp"); err != nil {
		t.Fatal(err)
	}

	// 2. Multi-hop cross-directory rename chain of an ephemeral file -> 1 net create at final destination.
	stageDir, _, err := c.Mkdir(vfs.RootInode, ".staging", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	dstDir, _, err := c.Mkdir(vfs.RootInode, "pkg", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	fID, _, err := c.Create(stageDir, "a.tmp", 0o644, []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Rename(stageDir, "a.tmp", stageDir, "b.tmp"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Rename(stageDir, "b.tmp", dstDir, "index.js"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteInline(fID, []byte("v2-final")); err != nil {
		t.Fatal(err)
	}
	// Remove now-empty ephemeral .staging directory.
	if _, err := c.Remove(vfs.RootInode, ".staging"); err != nil {
		t.Fatal(err)
	}

	plan := c.SealEpoch()
	if len(plan.Entries) != 2 {
		t.Fatalf("got %d net entry mutations (%+v), want 2 (mkdir pkg + create pkg/index.js)", len(plan.Entries), plan.Entries)
	}
	if len(plan.Inodes) != 2 {
		t.Fatalf("got %d net inode mutations (%+v), want 2 (pkg dir + index.js)", len(plan.Inodes), plan.Inodes)
	}
	for _, em := range plan.Entries {
		if em.Del {
			t.Fatalf("unexpected delete tombstone in coalesced plan: %+v", em)
		}
	}
	st := c.Stats()
	if st.CancelledInodes != 2 {
		t.Fatalf("cancelled inodes = %d, want 2 (ephemeral.tmp + .staging)", st.CancelledInodes)
	}
	if len(plan.Deps) != 1 {
		t.Fatalf("cross-dir deps = %d, want 1", len(plan.Deps))
	}
}

func TestHundredThousandOperationsMatchReferenceOracle(t *testing.T) {
	totalOps := 100_000
	if testing.Short() {
		totalOps = 25_000
	}

	c := djfs.NewCoalescer(2)
	const numDirs = 64
	dirs := make([]vfs.InodeID, numDirs)
	for i := range numDirs {
		id, _, err := c.Mkdir(vfs.RootInode, fmt.Sprintf("d%03d", i), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		dirs[i] = id
	}

	// Ground-truth oracle: map[dirInode]map[name][]byte (uncoalesced step-by-step execution).
	oracle := make(map[vfs.InodeID]map[string][]byte, numDirs)
	var liveKeys [][2]int // (dirIndex, fileSlot)
	for i := range numDirs {
		oracle[dirs[i]] = make(map[string][]byte)
	}

	rng := rand.New(rand.NewPCG(42, 1342))
	epochInterval := totalOps / 5

	for step := 1; step <= totalOps; step++ {
		opRoll := rng.IntN(100)
		switch {
		case opRoll < 45 || len(liveKeys) == 0:
			// Create
			di := rng.IntN(numDirs)
			slot := rng.IntN(120)
			name := fmt.Sprintf("f-%03d", slot)
			data := fmt.Appendf(nil, "payload-%d", step)
			if _, exists := oracle[dirs[di]][name]; exists {
				_, _, err := c.Create(dirs[di], name, 0o644, data)
				if !errors.Is(err, fs.ErrExist) {
					t.Fatalf("step %d: expected ErrExist on (%d,%s), got %v", step, di, name, err)
				}
				continue
			}
			if _, _, err := c.Create(dirs[di], name, 0o644, data); err != nil {
				t.Fatalf("step %d: Create failed: %v", step, err)
			}
			oracle[dirs[di]][name] = data
			liveKeys = append(liveKeys, [2]int{di, slot})

		case opRoll < 65:
			// WriteInline on an existing file
			pick := rng.IntN(len(liveKeys))
			di, slot := liveKeys[pick][0], liveKeys[pick][1]
			name := fmt.Sprintf("f-%03d", slot)
			if _, ok := oracle[dirs[di]][name]; !ok {
				continue
			}
			ev, err := c.Lookup(dirs[di], name)
			if err != nil {
				t.Fatalf("step %d: Lookup(%d,%s) failed: %v", step, di, name, err)
			}
			data := fmt.Appendf(nil, "rewrite-%d", step)
			if _, err := c.WriteInline(ev.ID, data); err != nil {
				t.Fatalf("step %d: WriteInline failed: %v", step, err)
			}
			oracle[dirs[di]][name] = data

		case opRoll < 85:
			// Intra- or cross-directory Rename (including overwrite-rename!)
			pick := rng.IntN(len(liveKeys))
			sdi, sslot := liveKeys[pick][0], liveKeys[pick][1]
			sname := fmt.Sprintf("f-%03d", sslot)
			sdata, ok := oracle[dirs[sdi]][sname]
			if !ok {
				liveKeys[pick] = liveKeys[len(liveKeys)-1]
				liveKeys = liveKeys[:len(liveKeys)-1]
				continue
			}
			tdi := rng.IntN(numDirs)
			tslot := rng.IntN(120)
			tname := fmt.Sprintf("f-%03d", tslot)
			if _, err := c.Rename(dirs[sdi], sname, dirs[tdi], tname); err != nil {
				t.Fatalf("step %d: Rename (%d,%s)->(%d,%s) failed: %v", step, sdi, sname, tdi, tname, err)
			}
			if !(sdi == tdi && sname == tname) {
				delete(oracle[dirs[sdi]], sname)
				oracle[dirs[tdi]][tname] = sdata
				liveKeys[pick] = [2]int{tdi, tslot}
			}

		default:
			// Remove
			pick := rng.IntN(len(liveKeys))
			di, slot := liveKeys[pick][0], liveKeys[pick][1]
			name := fmt.Sprintf("f-%03d", slot)
			liveKeys[pick] = liveKeys[len(liveKeys)-1]
			liveKeys = liveKeys[:len(liveKeys)-1]
			if _, ok := oracle[dirs[di]][name]; !ok {
				continue
			}
			if _, err := c.Remove(dirs[di], name); err != nil {
				t.Fatalf("step %d: Remove(%d,%s) failed: %v", step, di, name, err)
			}
			delete(oracle[dirs[di]], name)
		}

		if step%epochInterval == 0 {
			plan := c.SealEpoch()
			if plan.MaxLSN < plan.MinLSN && plan.MinLSN != 0 {
				t.Fatalf("non-monotonic epoch LSN window [%d, %d]", plan.MinLSN, plan.MaxLSN)
			}
		}
	}

	_ = c.SealEpoch()

	// Verify coalesced directory listings and file sizes match the uncoalesced oracle 100%.
	for i, dirID := range dirs {
		gotEnts, err := c.ReadDir(dirID)
		if err != nil {
			t.Fatalf("ReadDir(%d): %v", i, err)
		}
		wantMap := oracle[dirID]
		if len(gotEnts) != len(wantMap) {
			t.Fatalf("dir %d entry count = %d, want %d", i, len(gotEnts), len(wantMap))
		}
		for _, ge := range gotEnts {
			wb, ok := wantMap[ge.Name]
			if !ok {
				t.Fatalf("dir %d has unexpected entry %q", i, ge.Name)
			}
			ev, err := c.Lookup(dirID, ge.Name)
			if err != nil {
				t.Fatalf("Lookup(%d, %q): %v", i, ge.Name, err)
			}
			if ev.Size != uint64(len(wb)) {
				t.Fatalf("dir %d/%s size = %d, want %d", i, ge.Name, ev.Size, len(wb))
			}
		}
	}
}

func TestConcurrentMetadataStormRaceFree(t *testing.T) {
	c := djfs.NewCoalescer(2)
	const workers = 16
	dirs := make([]vfs.InodeID, workers)
	for w := range workers {
		id, _, err := c.Mkdir(vfs.RootInode, fmt.Sprintf("w-%02d", w), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		dirs[w] = id
	}

	var wg sync.WaitGroup
	stopSeal := make(chan struct{})
	var sealedEpochs int
	var sealWG sync.WaitGroup
	sealWG.Add(1)
	go func() {
		defer sealWG.Done()
		for {
			select {
			case <-stopSeal:
				return
			default:
				_ = c.SealEpoch()
				sealedEpochs++
			}
		}
	}()

	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			myDir := dirs[w]
			peerDir := dirs[(w+1)%workers]
			for i := range 200 {
				tmp := fmt.Sprintf("tmp-%d-%d", w, i)
				moved := fmt.Sprintf("moved-%d-%d", w, i)
				id, _, err := c.Create(myDir, tmp, 0o644, []byte("abc"))
				if err != nil {
					continue
				}
				_, _ = c.WriteInline(id, []byte("abcdef"))
				_, _ = c.Lookup(myDir, tmp)
				if err := func() error {
					_, err := c.Rename(myDir, tmp, peerDir, moved)
					return err
				}(); err == nil && i%2 == 0 {
					_, _ = c.Remove(peerDir, moved)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stopSeal)
	sealWG.Wait()
	_ = c.SealEpoch()

	if c.CurrentLSN() == 0 {
		t.Fatal("expected positive global LSN after concurrent storm")
	}
}

func TestOpCodecAndCrashReplay(t *testing.T) {
	ctx := context.Background()
	disk := crash.New()
	jnl, err := journal.Open("", journal.Options{
		Capacity: 16 << 20,
		Segment:  4 << 20,
		Lineage:  []byte("djfs-crash-lineage"),
		Disk:     disk,
	})
	if err != nil {
		t.Fatal(err)
	}

	c1 := djfs.NewCoalescer(2)
	var lastSeq uint64
	c1.SetAppendHook(func(op djfs.Op) (uint64, error) {
		raw := djfs.EncodeOp(op)
		seq, err := jnl.Append(ctx, raw)
		if err == nil {
			lastSeq = seq
		}
		return seq, err
	})

	d1, _, err := c1.Mkdir(vfs.RootInode, "stage", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	d2, _, err := c1.Mkdir(vfs.RootInode, "live", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	f1, _, err := c1.Create(d1, "pkg.tmp", 0o644, []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c1.WriteInline(f1, []byte("v2-durable")); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Rename(d1, "pkg.tmp", d2, "pkg.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := c1.Remove(vfs.RootInode, "stage"); err != nil {
		t.Fatal(err)
	}
	if err := jnl.Sync(ctx, lastSeq); err != nil {
		t.Fatal(err)
	}
	if err := jnl.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt byte detection check on EncodeOp/DecodeOp.
	sample := djfs.EncodeOp(djfs.Op{LSN: 7, Kind: djfs.OpCreate, Dir: 1, Name: "x", Ino: 9, Data: []byte("ok")})
	tampered := append([]byte(nil), sample...)
	tampered[len(tampered)/2] ^= 0xFF
	if _, err := djfs.DecodeOp(tampered); !errors.Is(err, djfs.ErrCorruptOp) {
		t.Fatalf("expected ErrCorruptOp on tampered record, got %v", err)
	}

	// Reopen journal on recovered disk image and replay into a fresh Coalescer.
	img := disk.Image(disk.Len(), crash.Variant{})
	jnl2, err := journal.Open("", journal.Options{
		Capacity: 16 << 20,
		Segment:  4 << 20,
		Lineage:  []byte("djfs-crash-lineage"),
		Disk:     img,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer jnl2.Close()

	c2 := djfs.NewCoalescer(2)
	if err := jnl2.Replay(func(rec journal.Record) error {
		op, err := djfs.DecodeOp(rec.Data)
		if err != nil {
			return err
		}
		return c2.ReplayOp(op)
	}); err != nil {
		t.Fatal(err)
	}

	plan1 := c1.SealEpoch()
	plan2 := c2.SealEpoch()
	if len(plan1.Entries) != len(plan2.Entries) || len(plan1.Inodes) != len(plan2.Inodes) {
		t.Fatalf("replayed plan mismatch: got (%d entries, %d inodes), want (%d, %d)",
			len(plan2.Entries), len(plan2.Inodes), len(plan1.Entries), len(plan1.Inodes))
	}
	if !bytes.Equal(plan1.Inodes[1].Inline, []byte("v2-durable")) || !bytes.Equal(plan2.Inodes[1].Inline, []byte("v2-durable")) {
		t.Fatalf("replayed inline payload = %q, want v2-durable", plan2.Inodes[1].Inline)
	}
}

func BenchmarkDJFSCoalesceStorm(b *testing.B) {
	c := djfs.NewCoalescer(2)
	dir, _, _ := c.Mkdir(vfs.RootInode, "storm", 0o755)
	dst, _, _ := c.Mkdir(vfs.RootInode, "out", 0o755)
	payload := []byte("benchmark-inline-payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tmp := fmt.Sprintf("t-%d", i&1023)
		fin := fmt.Sprintf("f-%d", i&1023)
		id, _, err := c.Create(dir, tmp, 0o644, payload)
		if err == nil {
			_, _ = c.WriteInline(id, payload)
			_, _ = c.Rename(dir, tmp, dst, fin)
			_, _ = c.Remove(dst, fin)
		}
		if i&4095 == 4095 {
			_ = c.SealEpoch()
		}
	}
}
