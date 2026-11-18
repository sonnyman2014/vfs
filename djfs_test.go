// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package vfs_test

import (
	"fmt"
	"testing"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/journal"
	"github.com/hanzoai/vfs/journal/crash"
	"github.com/hanzoai/vfs/pkg/djfs"
)

// TestDJFSEliminatesPhantomTombstoneShardWrites demonstrates a live Volume
// interaction on upstream main: creating and immediately removing ephemeral
// files in raw Volume leaves del=true tombstones in overlay, causing tree.pass
// to re-seal and PUT untouched 64 KiB prolly shards + ancestors under
// randomized post-quantum Hybrid encryption. DJFS cancels ephemeral pairs
// algebraically before Volume mutation, issuing 0 shard PUTs and 0 head PUTs.
func TestDJFSEliminatesPhantomTombstoneShardWrites(t *testing.T) {
	// 1. Raw Volume baseline with committed tree of 1,200 files.
	beRaw, _, vRaw := newMem(t)
	defer vRaw.Close()
	populate(t, vRaw, 12, 100)
	flush(t, vRaw)

	beRaw.Reset()
	stageRaw := mkdir(t, vRaw, vfs.RootInode, ".staging-storm")
	for i := range 50 {
		write(t, vRaw, stageRaw, fmt.Sprintf("tmp-%03d", i), []byte("ephemeral"))
		if err := vRaw.Remove(ctx, stageRaw, fmt.Sprintf("tmp-%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := vRaw.Remove(ctx, vfs.RootInode, ".staging-storm"); err != nil {
		t.Fatal(err)
	}
	flush(t, vRaw)
	rawPuts := beRaw.Puts()
	if rawPuts["meta"] == 0 {
		t.Fatalf("expected raw Volume to rewrite prolly shards on phantom tombstones, got %v", rawPuts)
	}

	// 2. Identical committed tree + identical ephemeral storm through djfs.Session.
	beDJFS, _, vDJFS := newMem(t)
	defer vDJFS.Close()
	populate(t, vDJFS, 12, 100)
	flush(t, vDJFS)

	sess, err := djfs.NewSession(ctx, vDJFS, nil)
	if err != nil {
		t.Fatal(err)
	}
	coal := sess.Coalescer()

	beDJFS.Reset()
	stageD, _, err := coal.Mkdir(vfs.RootInode, ".staging-storm", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 50 {
		if _, _, err := coal.Create(stageD, fmt.Sprintf("tmp-%03d", i), 0o644, []byte("ephemeral")); err != nil {
			t.Fatal(err)
		}
		if _, err := coal.Remove(stageD, fmt.Sprintf("tmp-%03d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := coal.Remove(vfs.RootInode, ".staging-storm"); err != nil {
		t.Fatal(err)
	}
	if _, err := sess.PublishEpoch(ctx); err != nil {
		t.Fatal(err)
	}
	djfsPuts := beDJFS.Puts()
	if djfsPuts["meta"] != 0 || djfsPuts["head"] != 0 || djfsPuts["pack"] != 0 {
		t.Fatalf("DJFS wrote %v on cancelled ephemeral storm, want 0 puts (raw Volume wrote %v)", djfsPuts, rawPuts)
	}
}

// TestDJFSNpmCIObjectBudgetAndExactTreeParity verifies that an npm-ci package
// extraction storm (.staging extract -> local journal fsync -> cross-dir rename
// into node_modules -> .staging cleanup -> single epoch publish) meets the
// <= 0.4 objects PUT per surviving file acceptance target and produces a
// committed Volume snapshot byte-identical to an uncoalesced reference Volume.
func TestDJFSNpmCIObjectBudgetAndExactTreeParity(t *testing.T) {
	pkgs := sized(40, 120)
	const filesPerPkg = 25
	surviving := pkgs * filesPerPkg
	body := []byte("{\"name\":\"@hanzo/mod\",\"exports\":\"./index.js\"}\n")

	// Reference Volume built directly without staging churn.
	_, _, vWant := newMem(t)
	defer vWant.Close()
	nmWant := mkdir(t, vWant, vfs.RootInode, "node_modules")
	for p := range pkgs {
		pkgDir := mkdir(t, vWant, nmWant, fmt.Sprintf("pkg-%04d", p))
		for f := range filesPerPkg {
			write(t, vWant, pkgDir, fmt.Sprintf("f%03d.js", f), body)
		}
	}
	flush(t, vWant)

	// DJFS Volume subjected to full .staging tarball extract + rename + lockfile churn.
	beGot, cGot, vGot := newMem(t)
	defer vGot.Close()

	jnl, err := journal.Open("", journal.Options{
		Capacity: 32 << 20,
		Segment:  4 << 20,
		Lineage:  []byte("djfs-npm-ci-test"),
		Disk:     crash.New(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer jnl.Close()

	sess, err := djfs.NewSession(ctx, vGot, jnl)
	if err != nil {
		t.Fatal(err)
	}
	coal := sess.Coalescer()

	beGot.Reset()
	stRoot, _, err := coal.Mkdir(vfs.RootInode, ".staging", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	nmRoot, _, err := coal.Mkdir(vfs.RootInode, "node_modules", 0o755)
	if err != nil {
		t.Fatal(err)
	}
	for p := range pkgs {
		stPkg, _, err := coal.Mkdir(stRoot, fmt.Sprintf("pkg-%04d", p), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		nmPkg, _, err := coal.Mkdir(nmRoot, fmt.Sprintf("pkg-%04d", p), 0o755)
		if err != nil {
			t.Fatal(err)
		}
		var seq uint64
		for f := range filesPerPkg {
			_, seq, err = coal.Create(stPkg, fmt.Sprintf("f%03d.tmp", f), 0o644, []byte("partial"))
			if err != nil {
				t.Fatal(err)
			}
			ev, err := coal.Lookup(stPkg, fmt.Sprintf("f%03d.tmp", f))
			if err != nil {
				t.Fatal(err)
			}
			seq, err = coal.WriteInline(ev.ID, body)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := sess.Fsync(ctx, seq); err != nil {
			t.Fatal(err)
		}
		// Zero synchronous S3 requests during local Fsync.
		if beGot.Puts()["head"] != 0 || beGot.Puts()["meta"] != 0 {
			t.Fatalf("unexpected synchronous store PUTs before PublishEpoch: %v", beGot.Puts())
		}
		for f := range filesPerPkg {
			if _, err := coal.Rename(stPkg, fmt.Sprintf("f%03d.tmp", f), nmPkg, fmt.Sprintf("f%03d.js", f)); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := coal.Create(stPkg, ".lock.tmp", 0o644, body); err != nil {
			t.Fatal(err)
		}
		if _, err := coal.Remove(stPkg, ".lock.tmp"); err != nil {
			t.Fatal(err)
		}
		if _, err := coal.Remove(stRoot, fmt.Sprintf("pkg-%04d", p)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := coal.Remove(vfs.RootInode, ".staging"); err != nil {
		t.Fatal(err)
	}

	if _, err := sess.PublishEpoch(ctx); err != nil {
		t.Fatal(err)
	}

	puts := beGot.Puts()
	totalPuts := puts["meta"] + puts["pack"] + puts["head"]
	putsPerFile := float64(totalPuts) / float64(surviving)
	t.Logf("npm-ci storm (%d surviving files): %d total PUTs (meta=%d, pack=%d, head=%d) = %.4f PUTs/file",
		surviving, totalPuts, puts["meta"], puts["pack"], puts["head"], putsPerFile)
	if putsPerFile > 0.4 {
		t.Fatalf("objects PUT per file = %.4f, exceeds 0.4 acceptance budget", putsPerFile)
	}

	ro := reopen(t, beGot, cGot)
	defer ro.Close()
	sameSnapshot(t, "npm-ci coalesced snapshot", snapshot(t, ro), snapshot(t, vWant))
}
