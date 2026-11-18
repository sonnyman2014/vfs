// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package djfs implements per-directory metadata mutation coalescing with a
// single global commit order for hanzoai/vfs (HIP-1342, Refs #20).
//
// Inspired by DJFS (USENIX FAST 2025), mutations inside each directory are
// absorbed into sharded per-directory delta tables stamped with a strictly
// monotonic global sequence number (LSN). Cross-directory renames record
// explicit causal dependencies so every commit epoch freezes an atomic,
// globally ordered cut before touching the Volume's prolly Merkle tree.
//
// Ephemeral create-delete pairs, staging subdirectories, multi-hop rename
// chains, repeated inline overwrites, and redundant parent directory mtime
// updates cancel algebraically in memory before shard sealing, eliminating
// phantom overlay tombstones that would otherwise re-encrypt untouched prolly
// leaves under randomized post-quantum Hybrid sealing.
package djfs

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanzoai/vfs"
)

const (
	numDirShards   = 64
	numInodeShards = 64
	journalMagic   = uint32(0x444A4653) // "DJFS"
)

// OpKind identifies a journaled namespace or inline-data mutation.
type OpKind uint8

const (
	OpCreate OpKind = iota + 1
	OpMkdir
	OpWriteInline
	OpRemove
	OpRename
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// Op is a single logical mutation in global LSN order, used for journal
// encoding, crash replay, and trace inspection.
type Op struct {
	LSN     uint64
	Kind    OpKind
	Dir     vfs.InodeID
	Name    string
	DstDir  vfs.InodeID
	DstName string
	Ino     vfs.InodeID
	Mode    fs.FileMode
	Data    []byte
	TimeNs  int64
}

// EntryView is the live coalesced view of a directory entry prior to Flush.
type EntryView struct {
	Name  string
	ID    vfs.InodeID
	Mode  fs.FileMode
	Mtime time.Time
	Size  uint64
}

// EntryMut is a net directory-entry change emitted when an epoch seals.
type EntryMut struct {
	Dir          vfs.InodeID
	Name         string
	Child        vfs.InodeID
	Mode         fs.FileMode
	Del          bool
	BaseReplaced bool
	BaseChild    vfs.InodeID
	BaseWasDir   bool
}

// InodeMut is a net inode or inline-payload change emitted when an epoch seals.
type InodeMut struct {
	ID          vfs.InodeID
	Mode        fs.FileMode
	IsDir       bool
	BornInEpoch bool
	Del         bool
	DataDirty   bool
	Inline      []byte
	Mtime       time.Time
}

// CrossDirDep records a cross-directory rename dependency captured inside an epoch.
type CrossDirDep struct {
	LSN    uint64
	SrcDir vfs.InodeID
	DstDir vfs.InodeID
}

// Stats summarizes raw vs coalesced mutation counts for an epoch or session.
type Stats struct {
	Epochs              uint64
	RawOps              uint64
	RawRecordMutations  uint64
	NetEntryMutations   uint64
	NetInodeMutations   uint64
	CancelledEntries    uint64
	CancelledInodes     uint64
	CollapsedParentDirs uint64
	CrossDirRenames     uint64
}

// EpochPlan is a globally ordered, algebraically reduced batch of mutations
// ready to apply to a vfs.Volume in a single commit.
type EpochPlan struct {
	EpochID       uint64
	MinLSN        uint64
	MaxLSN        uint64
	TouchedDirs   int
	ParentTouches int
	RawOps        uint64
	RawRecords    uint64
	Entries       []EntryMut
	Inodes        []InodeMut
	Deps          []CrossDirDep
}

type slotState struct {
	baseHad   bool
	baseChild vfs.InodeID
	baseMode  fs.FileMode

	present  bool
	curChild vfs.InodeID
	curMode  fs.FileMode
}

type dirDelta struct {
	ino         vfs.InodeID
	rawTouches  uint64
	lastTouchNs int64
	slots       map[string]*slotState
}

type dirShard struct {
	mu   sync.Mutex
	dirs map[vfs.InodeID]*dirDelta
}

type inoDelta struct {
	id           vfs.InodeID
	mode         fs.FileMode
	bornInEpoch  bool
	liveLinks    int32
	liveChildren atomic.Int64
	dataDirty    bool
	inline       []byte
	mtimeNs      int64
}

type inoShard struct {
	mu     sync.Mutex
	inodes map[vfs.InodeID]*inoDelta
}

// Coalescer absorbs concurrent directory and inline-file mutations into sharded
// per-directory logs while maintaining a single global commit order.
//
// Lock ordering hierarchy (strict, deadlock-free):
//
//  1. epochMu (RLock for mutations/lookups; Lock for SealEpoch/ReplayOp)
//  2. dirShards[i] in strictly ascending shard index order
//  3. inoShards[j] in strictly ascending shard index order
//  4. depMu (leaf lock for recording cross-directory causal edges)
type Coalescer struct {
	epochMu sync.RWMutex

	nextLSN   atomic.Uint64
	nextIno   atomic.Uint64
	epochSeq  atomic.Uint64
	epochMin  atomic.Uint64
	rawOps    atomic.Uint64
	rawRecs   atomic.Uint64
	crossDeps atomic.Uint64

	dirShards [numDirShards]dirShard
	inoShards [numInodeShards]inoShard

	depMu sync.Mutex
	deps  []CrossDirDep

	statsMu sync.Mutex
	totals  Stats

	onAppend func(Op) (uint64, error)
}

// NewCoalescer creates a Coalescer whose newly allocated inodes start at nextIno.
// RootInode (1) is pre-registered as an existing base directory.
func NewCoalescer(nextIno vfs.InodeID) *Coalescer {
	if nextIno <= vfs.RootInode {
		nextIno = vfs.RootInode + 1
	}
	c := &Coalescer{}
	c.nextIno.Store(uint64(nextIno))
	for i := range c.dirShards {
		c.dirShards[i].dirs = make(map[vfs.InodeID]*dirDelta)
	}
	for i := range c.inoShards {
		c.inoShards[i].inodes = make(map[vfs.InodeID]*inoDelta)
	}
	root := &inoDelta{
		id:          vfs.RootInode,
		mode:        fs.ModeDir | 0o755,
		bornInEpoch: false,
		liveLinks:   1,
		mtimeNs:     time.Now().UnixNano(),
	}
	c.inoShards[inoShardIdx(vfs.RootInode)].inodes[vfs.RootInode] = root
	c.dirShards[dirShardIdx(vfs.RootInode)].dirs[vfs.RootInode] = &dirDelta{
		ino:   vfs.RootInode,
		slots: make(map[string]*slotState),
	}
	return c
}

// SetAppendHook installs an optional synchronous hook invoked with each assigned
// global LSN before the mutation returns (used by the journal bridge).
func (c *Coalescer) SetAppendHook(fn func(Op) (uint64, error)) {
	c.epochMu.Lock()
	defer c.epochMu.Unlock()
	c.onAppend = fn
}

// CurrentLSN returns the highest LSN assigned so far.
func (c *Coalescer) CurrentLSN() uint64 {
	return c.nextLSN.Load()
}

// RegisterBaseEntry seeds a committed base entry so Coalescer knows what
// existed in the Volume's committed root before the epoch began.
func (c *Coalescer) RegisterBaseEntry(dir vfs.InodeID, name string, child vfs.InodeID, mode fs.FileMode, size uint64) {
	c.epochMu.Lock()
	defer c.epochMu.Unlock()

	if uint64(child) >= c.nextIno.Load() {
		c.nextIno.Store(uint64(child) + 1)
	}
	ds := &c.dirShards[dirShardIdx(dir)]
	dd := ds.dirs[dir]
	if dd == nil {
		dd = &dirDelta{ino: dir, slots: make(map[string]*slotState)}
		ds.dirs[dir] = dd
	}
	dd.slots[name] = &slotState{
		baseHad:   true,
		baseChild: child,
		baseMode:  mode,
		present:   true,
		curChild:  child,
		curMode:   mode,
	}

	pShard := &c.inoShards[inoShardIdx(dir)]
	pIno := pShard.inodes[dir]
	if pIno == nil {
		pIno = &inoDelta{
			id:          dir,
			mode:        fs.ModeDir | 0o755,
			bornInEpoch: false,
			liveLinks:   1,
		}
		pShard.inodes[dir] = pIno
	}
	pIno.liveChildren.Add(1)

	cShard := &c.inoShards[inoShardIdx(child)]
	cIno := cShard.inodes[child]
	if cIno == nil {
		cIno = &inoDelta{
			id:          child,
			mode:        mode,
			bornInEpoch: false,
			liveLinks:   1,
		}
		cShard.inodes[child] = cIno
	} else {
		cIno.mode = mode
		cIno.liveLinks = 1
	}
	if mode.IsDir() {
		cds := &c.dirShards[dirShardIdx(child)]
		if cds.dirs[child] == nil {
			cds.dirs[child] = &dirDelta{ino: child, slots: make(map[string]*slotState)}
		}
	}
	_ = size
}

func dirShardIdx(id vfs.InodeID) int {
	x := uint64(id) * 0x9e3779b97f4a7c15
	return int((x ^ (x >> 32)) & (numDirShards - 1))
}

func inoShardIdx(id vfs.InodeID) int {
	x := uint64(id) * 0xbf58476d1ce4e5b9
	return int((x ^ (x >> 32)) & (numInodeShards - 1))
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\x00") {
		return fmt.Errorf("%w: name %q", fs.ErrInvalid, name)
	}
	return nil
}

func (c *Coalescer) assignLSN() uint64 {
	lsn := c.nextLSN.Add(1)
	for {
		cur := c.epochMin.Load()
		if cur != 0 && cur <= lsn {
			break
		}
		if c.epochMin.CompareAndSwap(cur, lsn) {
			break
		}
	}
	return lsn
}

func (c *Coalescer) lockTwoInoShards(a, b vfs.InodeID) func() {
	sa, sb := inoShardIdx(a), inoShardIdx(b)
	if sa == sb {
		c.inoShards[sa].mu.Lock()
		return func() { c.inoShards[sa].mu.Unlock() }
	}
	if sa > sb {
		sa, sb = sb, sa
	}
	c.inoShards[sa].mu.Lock()
	c.inoShards[sb].mu.Lock()
	return func() {
		c.inoShards[sb].mu.Unlock()
		c.inoShards[sa].mu.Unlock()
	}
}

// Create records a regular file creation (and optional initial inline bytes) in dir.
func (c *Coalescer) Create(dir vfs.InodeID, name string, perm fs.FileMode, inline []byte) (vfs.InodeID, uint64, error) {
	return c.makeNode(dir, name, perm.Perm(), inline)
}

// Mkdir records a subdirectory creation in dir.
func (c *Coalescer) Mkdir(dir vfs.InodeID, name string, perm fs.FileMode) (vfs.InodeID, uint64, error) {
	return c.makeNode(dir, name, fs.ModeDir|perm.Perm(), nil)
}

func (c *Coalescer) makeNode(dir vfs.InodeID, name string, mode fs.FileMode, inline []byte) (vfs.InodeID, uint64, error) {
	if err := validName(name); err != nil {
		return 0, 0, err
	}
	c.epochMu.RLock()
	defer c.epochMu.RUnlock()

	newID := vfs.InodeID(c.nextIno.Add(1) - 1)
	nowNs := time.Now().UnixNano()

	dIdx := dirShardIdx(dir)
	cDirIdx := -1
	if mode.IsDir() {
		cDirIdx = dirShardIdx(newID)
	}
	if cDirIdx >= 0 && cDirIdx != dIdx {
		lo, hi := dIdx, cDirIdx
		if lo > hi {
			lo, hi = hi, lo
		}
		c.dirShards[lo].mu.Lock()
		c.dirShards[hi].mu.Lock()
		defer c.dirShards[hi].mu.Unlock()
		defer c.dirShards[lo].mu.Unlock()
	} else {
		c.dirShards[dIdx].mu.Lock()
		defer c.dirShards[dIdx].mu.Unlock()
	}

	unlockIno := c.lockTwoInoShards(dir, newID)

	pIno := c.inoShards[inoShardIdx(dir)].inodes[dir]
	if pIno == nil || pIno.liveLinks <= 0 {
		unlockIno()
		return 0, 0, fmt.Errorf("%w: inode %d", fs.ErrNotExist, dir)
	}
	if !pIno.mode.IsDir() {
		unlockIno()
		return 0, 0, fmt.Errorf("%w: inode %d", vfs.ErrNotDir, dir)
	}

	dd := c.dirShards[dIdx].dirs[dir]
	if dd == nil {
		dd = &dirDelta{ino: dir, slots: make(map[string]*slotState)}
		c.dirShards[dIdx].dirs[dir] = dd
	}
	st := dd.slots[name]
	if st != nil && st.present {
		unlockIno()
		return 0, 0, fmt.Errorf("%w: %q in inode %d", fs.ErrExist, name, dir)
	}
	if st == nil {
		st = &slotState{}
		dd.slots[name] = st
	}
	st.present = true
	st.curChild = newID
	st.curMode = mode
	dd.rawTouches++
	dd.lastTouchNs = nowNs

	pIno.liveChildren.Add(1)
	pIno.mtimeNs = nowNs

	var payload []byte
	dirty := false
	if !mode.IsDir() && inline != nil {
		payload = append([]byte(nil), inline...)
		dirty = true
	}
	c.inoShards[inoShardIdx(newID)].inodes[newID] = &inoDelta{
		id:          newID,
		mode:        mode,
		bornInEpoch: true,
		liveLinks:   1,
		dataDirty:   dirty,
		inline:      payload,
		mtimeNs:     nowNs,
	}
	if mode.IsDir() {
		c.dirShards[cDirIdx].dirs[newID] = &dirDelta{ino: newID, slots: make(map[string]*slotState)}
	}
	lsn := c.assignLSN()
	unlockIno()

	c.rawOps.Add(1)
	rawRec := uint64(3)
	if dirty {
		rawRec++
	}
	c.rawRecs.Add(rawRec)

	if c.onAppend != nil {
		kind := OpCreate
		if mode.IsDir() {
			kind = OpMkdir
		}
		jSeq, err := c.onAppend(Op{
			LSN:    lsn,
			Kind:   kind,
			Dir:    dir,
			Name:   name,
			Ino:    newID,
			Mode:   mode,
			Data:   payload,
			TimeNs: nowNs,
		})
		if err != nil {
			return 0, 0, err
		}
		if jSeq != 0 {
			return newID, jSeq, nil
		}
	}
	return newID, lsn, nil
}

// WriteInline updates the inline payload of a regular file within the current epoch,
// collapsing repeated writes to the same inode before shard sealing.
func (c *Coalescer) WriteInline(ino vfs.InodeID, data []byte) (uint64, error) {
	c.epochMu.RLock()
	defer c.epochMu.RUnlock()

	nowNs := time.Now().UnixNano()
	ish := &c.inoShards[inoShardIdx(ino)]
	ish.mu.Lock()
	ind := ish.inodes[ino]
	if ind == nil || ind.liveLinks <= 0 {
		ish.mu.Unlock()
		return 0, fmt.Errorf("%w: inode %d", fs.ErrNotExist, ino)
	}
	if ind.mode.IsDir() {
		ish.mu.Unlock()
		return 0, fmt.Errorf("%w: inode %d", vfs.ErrIsDir, ino)
	}
	cp := append([]byte(nil), data...)
	ind.inline = cp
	ind.dataDirty = true
	ind.mtimeNs = nowNs
	lsn := c.assignLSN()
	ish.mu.Unlock()

	c.rawOps.Add(1)
	c.rawRecs.Add(2)

	if c.onAppend != nil {
		jSeq, err := c.onAppend(Op{
			LSN:    lsn,
			Kind:   OpWriteInline,
			Ino:    ino,
			Data:   cp,
			TimeNs: nowNs,
		})
		if err != nil {
			return 0, err
		}
		if jSeq != 0 {
			return jSeq, nil
		}
	}
	return lsn, nil
}

// Lookup queries the live in-memory coalesced state of (dir, name).
func (c *Coalescer) Lookup(dir vfs.InodeID, name string) (EntryView, error) {
	if err := validName(name); err != nil {
		return EntryView{}, err
	}
	c.epochMu.RLock()
	defer c.epochMu.RUnlock()

	dIdx := dirShardIdx(dir)
	c.dirShards[dIdx].mu.Lock()
	dd := c.dirShards[dIdx].dirs[dir]
	if dd == nil {
		c.dirShards[dIdx].mu.Unlock()
		return EntryView{}, fmt.Errorf("%w: inode %d", fs.ErrNotExist, dir)
	}
	st := dd.slots[name]
	if st == nil || !st.present {
		c.dirShards[dIdx].mu.Unlock()
		return EntryView{}, fmt.Errorf("%w: %q in inode %d", fs.ErrNotExist, name, dir)
	}
	child := st.curChild
	mode := st.curMode
	c.dirShards[dIdx].mu.Unlock()

	ish := &c.inoShards[inoShardIdx(child)]
	ish.mu.Lock()
	ind := ish.inodes[child]
	var sz uint64
	var mt time.Time
	if ind != nil {
		sz = uint64(len(ind.inline))
		mt = time.Unix(0, ind.mtimeNs)
		mode = ind.mode
	}
	ish.mu.Unlock()

	return EntryView{Name: name, ID: child, Mode: mode, Mtime: mt, Size: sz}, nil
}

// ReadDir returns all live entries in dir in sorted name order.
func (c *Coalescer) ReadDir(dir vfs.InodeID) ([]EntryView, error) {
	c.epochMu.RLock()
	defer c.epochMu.RUnlock()

	dIdx := dirShardIdx(dir)
	c.dirShards[dIdx].mu.Lock()
	dd := c.dirShards[dIdx].dirs[dir]
	if dd == nil {
		c.dirShards[dIdx].mu.Unlock()
		return nil, fmt.Errorf("%w: inode %d", fs.ErrNotExist, dir)
	}
	var out []EntryView
	for name, st := range dd.slots {
		if st.present {
			out = append(out, EntryView{Name: name, ID: st.curChild, Mode: st.curMode})
		}
	}
	c.dirShards[dIdx].mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Remove deletes a file or an empty directory from dir, cancelling ephemeral
// creates in memory without leaving tombstones in the Merkle-tree update.
func (c *Coalescer) Remove(dir vfs.InodeID, name string) (uint64, error) {
	if err := validName(name); err != nil {
		return 0, err
	}
	c.epochMu.RLock()
	defer c.epochMu.RUnlock()

	nowNs := time.Now().UnixNano()
	dIdx := dirShardIdx(dir)
	c.dirShards[dIdx].mu.Lock()
	dd := c.dirShards[dIdx].dirs[dir]
	if dd == nil {
		c.dirShards[dIdx].mu.Unlock()
		return 0, fmt.Errorf("%w: inode %d", fs.ErrNotExist, dir)
	}
	st := dd.slots[name]
	if st == nil || !st.present {
		c.dirShards[dIdx].mu.Unlock()
		return 0, fmt.Errorf("%w: %q in inode %d", fs.ErrNotExist, name, dir)
	}
	child := st.curChild

	unlockIno := c.lockTwoInoShards(dir, child)
	cIno := c.inoShards[inoShardIdx(child)].inodes[child]
	if cIno != nil && cIno.mode.IsDir() && cIno.liveChildren.Load() > 0 {
		unlockIno()
		c.dirShards[dIdx].mu.Unlock()
		return 0, fmt.Errorf("%w: %q", vfs.ErrNotEmpty, name)
	}

	st.present = false
	st.curChild = 0
	st.curMode = 0
	dd.rawTouches++
	dd.lastTouchNs = nowNs

	if pIno := c.inoShards[inoShardIdx(dir)].inodes[dir]; pIno != nil {
		pIno.liveChildren.Add(-1)
		pIno.mtimeNs = nowNs
	}
	hadInline := false
	if cIno != nil {
		cIno.liveLinks--
		hadInline = len(cIno.inline) > 0 || cIno.dataDirty
		if cIno.liveLinks <= 0 {
			cIno.inline = nil
		}
	}
	lsn := c.assignLSN()
	unlockIno()
	c.dirShards[dIdx].mu.Unlock()

	c.rawOps.Add(1)
	rawRec := uint64(3)
	if hadInline {
		rawRec++
	}
	c.rawRecs.Add(rawRec)

	if c.onAppend != nil {
		jSeq, err := c.onAppend(Op{
			LSN:    lsn,
			Kind:   OpRemove,
			Dir:    dir,
			Name:   name,
			Ino:    child,
			TimeNs: nowNs,
		})
		if err != nil {
			return 0, err
		}
		if jSeq != 0 {
			return jSeq, nil
		}
	}
	return lsn, nil
}

// Rename atomically moves (srcDir, srcName) to (dstDir, dstName), recording a
// global LSN and, for cross-directory moves, a causal dependency edge so both
// directories seal in the same commit epoch.
func (c *Coalescer) Rename(srcDir vfs.InodeID, srcName string, dstDir vfs.InodeID, dstName string) (uint64, error) {
	if err := validName(srcName); err != nil {
		return 0, err
	}
	if err := validName(dstName); err != nil {
		return 0, err
	}
	c.epochMu.RLock()
	defer c.epochMu.RUnlock()

	nowNs := time.Now().UnixNano()
	sIdx, tIdx := dirShardIdx(srcDir), dirShardIdx(dstDir)
	if sIdx == tIdx {
		c.dirShards[sIdx].mu.Lock()
		defer c.dirShards[sIdx].mu.Unlock()
	} else {
		lo, hi := sIdx, tIdx
		if lo > hi {
			lo, hi = hi, lo
		}
		c.dirShards[lo].mu.Lock()
		c.dirShards[hi].mu.Lock()
		defer c.dirShards[hi].mu.Unlock()
		defer c.dirShards[lo].mu.Unlock()
	}

	sdd := c.dirShards[sIdx].dirs[srcDir]
	if sdd == nil {
		return 0, fmt.Errorf("%w: source dir %d", fs.ErrNotExist, srcDir)
	}
	srcSlot := sdd.slots[srcName]
	if srcSlot == nil || !srcSlot.present {
		return 0, fmt.Errorf("%w: %q in inode %d", fs.ErrNotExist, srcName, srcDir)
	}
	if srcDir == dstDir && srcName == dstName {
		return c.assignLSN(), nil
	}

	tdd := c.dirShards[tIdx].dirs[dstDir]
	if tdd == nil {
		tdd = &dirDelta{ino: dstDir, slots: make(map[string]*slotState)}
		c.dirShards[tIdx].dirs[dstDir] = tdd
	}
	dstSlot := tdd.slots[dstName]

	movingIno := srcSlot.curChild
	movingMode := srcSlot.curMode
	var displacedIno vfs.InodeID
	hasDisplaced := dstSlot != nil && dstSlot.present
	if hasDisplaced {
		displacedIno = dstSlot.curChild
	}

	inoShardSet := []int{inoShardIdx(srcDir), inoShardIdx(dstDir), inoShardIdx(movingIno)}
	if hasDisplaced {
		inoShardSet = append(inoShardSet, inoShardIdx(displacedIno))
	}
	sort.Ints(inoShardSet)
	uniq := inoShardSet[:0]
	for _, idx := range inoShardSet {
		if len(uniq) == 0 || uniq[len(uniq)-1] != idx {
			uniq = append(uniq, idx)
		}
	}
	for _, idx := range uniq {
		c.inoShards[idx].mu.Lock()
	}
	unlockAllIno := func() {
		for i := len(uniq) - 1; i >= 0; i-- {
			c.inoShards[uniq[i]].mu.Unlock()
		}
	}

	dstParent := c.inoShards[inoShardIdx(dstDir)].inodes[dstDir]
	if dstParent == nil || dstParent.liveLinks <= 0 {
		unlockAllIno()
		return 0, fmt.Errorf("%w: destination dir %d", fs.ErrNotExist, dstDir)
	}
	if !dstParent.mode.IsDir() {
		unlockAllIno()
		return 0, fmt.Errorf("%w: inode %d", vfs.ErrNotDir, dstDir)
	}

	if hasDisplaced {
		disp := c.inoShards[inoShardIdx(displacedIno)].inodes[displacedIno]
		if disp != nil {
			if movingMode.IsDir() && !disp.mode.IsDir() {
				unlockAllIno()
				return 0, fmt.Errorf("%w: %q", vfs.ErrNotDir, dstName)
			}
			if !movingMode.IsDir() && disp.mode.IsDir() {
				unlockAllIno()
				return 0, fmt.Errorf("%w: %q", vfs.ErrIsDir, dstName)
			}
			if disp.mode.IsDir() && disp.liveChildren.Load() > 0 {
				unlockAllIno()
				return 0, fmt.Errorf("%w: %q", vfs.ErrNotEmpty, dstName)
			}
			disp.liveLinks--
			if disp.liveLinks <= 0 {
				disp.inline = nil
			}
		}
	} else {
		if dstSlot == nil {
			dstSlot = &slotState{}
			tdd.slots[dstName] = dstSlot
		}
		dstParent.liveChildren.Add(1)
	}

	srcSlot.present = false
	srcSlot.curChild = 0
	srcSlot.curMode = 0
	sdd.rawTouches++
	sdd.lastTouchNs = nowNs
	if srcParent := c.inoShards[inoShardIdx(srcDir)].inodes[srcDir]; srcParent != nil {
		srcParent.liveChildren.Add(-1)
		srcParent.mtimeNs = nowNs
	}

	dstSlot.present = true
	dstSlot.curChild = movingIno
	dstSlot.curMode = movingMode
	tdd.rawTouches++
	tdd.lastTouchNs = nowNs
	dstParent.mtimeNs = nowNs

	if mv := c.inoShards[inoShardIdx(movingIno)].inodes[movingIno]; mv != nil {
		mv.mtimeNs = nowNs
	}

	lsn := c.assignLSN()
	unlockAllIno()

	if srcDir != dstDir {
		c.depMu.Lock()
		c.deps = append(c.deps, CrossDirDep{LSN: lsn, SrcDir: srcDir, DstDir: dstDir})
		c.depMu.Unlock()
		c.crossDeps.Add(1)
	}

	c.rawOps.Add(1)
	rawRec := uint64(4)
	if srcDir == dstDir {
		rawRec = 3
	}
	if hasDisplaced {
		rawRec++
	}
	c.rawRecs.Add(rawRec)

	if c.onAppend != nil {
		jSeq, err := c.onAppend(Op{
			LSN:     lsn,
			Kind:    OpRename,
			Dir:     srcDir,
			Name:    srcName,
			DstDir:  dstDir,
			DstName: dstName,
			Ino:     movingIno,
			Mode:    movingMode,
			TimeNs:  nowNs,
		})
		if err != nil {
			return 0, err
		}
		if jSeq != 0 {
			return jSeq, nil
		}
	}
	return lsn, nil
}

// SealEpoch freezes a globally ordered commit cut across all directory and inode
// shards, algebraically reduces all pending mutations into minimal net tree
// changes, and advances the coalescer into a fresh epoch whose new base state
// equals the post-epoch namespace.
func (c *Coalescer) SealEpoch() EpochPlan {
	c.epochMu.Lock()
	defer c.epochMu.Unlock()

	for i := range c.dirShards {
		c.dirShards[i].mu.Lock()
	}
	for i := range c.inoShards {
		c.inoShards[i].mu.Lock()
	}
	defer func() {
		for i := len(c.inoShards) - 1; i >= 0; i-- {
			c.inoShards[i].mu.Unlock()
		}
		for i := len(c.dirShards) - 1; i >= 0; i-- {
			c.dirShards[i].mu.Unlock()
		}
	}()

	epochID := c.epochSeq.Add(1)
	minLSN := c.epochMin.Swap(0)
	maxLSN := c.nextLSN.Load()
	rawOps := c.rawOps.Swap(0)
	rawRecs := c.rawRecs.Swap(0)

	c.depMu.Lock()
	deps := c.deps
	c.deps = nil
	c.depMu.Unlock()

	var entries []EntryMut
	var inodes []InodeMut
	var touchedDirs, parentTouches int
	var collapsedParents uint64

	for i := range c.dirShards {
		for dirID, dd := range c.dirShards[i].dirs {
			if dd.rawTouches == 0 && len(dd.slots) == 0 {
				continue
			}
			pIno := c.inoShards[inoShardIdx(dirID)].inodes[dirID]
			dirAlive := pIno != nil && pIno.liveLinks > 0

			if dd.rawTouches > 0 {
				touchedDirs++
				if dirAlive {
					parentTouches++
					if dd.rawTouches > 1 {
						collapsedParents += dd.rawTouches - 1
					}
				} else {
					collapsedParents += dd.rawTouches
				}
				dd.rawTouches = 0
			}

			for name, st := range dd.slots {
				switch {
				case !st.baseHad && !st.present:
					delete(dd.slots, name)

				case !st.baseHad && st.present:
					if dirAlive {
						entries = append(entries, EntryMut{
							Dir:   dirID,
							Name:  name,
							Child: st.curChild,
							Mode:  st.curMode,
						})
						st.baseHad = true
						st.baseChild = st.curChild
						st.baseMode = st.curMode
					}

				case st.baseHad && !st.present:
					entries = append(entries, EntryMut{
						Dir:        dirID,
						Name:       name,
						Del:        true,
						BaseChild:  st.baseChild,
						BaseWasDir: st.baseMode.IsDir(),
					})
					delete(dd.slots, name)

				case st.baseHad && st.present:
					if st.curChild != st.baseChild || st.curMode.Type() != st.baseMode.Type() {
						entries = append(entries, EntryMut{
							Dir:          dirID,
							Name:         name,
							Child:        st.curChild,
							Mode:         st.curMode,
							BaseReplaced: true,
							BaseChild:    st.baseChild,
							BaseWasDir:   st.baseMode.IsDir(),
						})
						st.baseChild = st.curChild
						st.baseMode = st.curMode
					}
				}
			}

			if !dirAlive {
				delete(c.dirShards[i].dirs, dirID)
			}
		}
	}

	var cancelledInodes uint64
	for i := range c.inoShards {
		for id, ind := range c.inoShards[i].inodes {
			if id == vfs.RootInode {
				continue
			}
			switch {
			case ind.bornInEpoch && ind.liveLinks <= 0:
				cancelledInodes++
				delete(c.inoShards[i].inodes, id)

			case !ind.bornInEpoch && ind.liveLinks <= 0:
				inodes = append(inodes, InodeMut{
					ID:    id,
					Mode:  ind.mode,
					IsDir: ind.mode.IsDir(),
					Del:   true,
				})
				delete(c.inoShards[i].inodes, id)

			case ind.bornInEpoch && ind.liveLinks > 0:
				var cp []byte
				if ind.inline != nil {
					cp = append([]byte(nil), ind.inline...)
				}
				inodes = append(inodes, InodeMut{
					ID:          id,
					Mode:        ind.mode,
					IsDir:       ind.mode.IsDir(),
					BornInEpoch: true,
					DataDirty:   ind.dataDirty,
					Inline:      cp,
					Mtime:       time.Unix(0, ind.mtimeNs),
				})
				ind.bornInEpoch = false
				ind.dataDirty = false

			case !ind.bornInEpoch && ind.liveLinks > 0:
				if ind.dataDirty {
					cp := append([]byte(nil), ind.inline...)
					inodes = append(inodes, InodeMut{
						ID:        id,
						Mode:      ind.mode,
						IsDir:     false,
						DataDirty: true,
						Inline:    cp,
						Mtime:     time.Unix(0, ind.mtimeNs),
					})
					ind.dataDirty = false
				}
			}
		}
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Dir != entries[j].Dir {
			return entries[i].Dir < entries[j].Dir
		}
		return entries[i].Name < entries[j].Name
	})
	sort.Slice(inodes, func(i, j int) bool {
		return inodes[i].ID < inodes[j].ID
	})

	netRecords := uint64(len(entries) + len(inodes) + parentTouches)
	var cancelledEntries uint64
	if rawRecs > netRecords {
		cancelledEntries = rawRecs - netRecords
	}

	c.statsMu.Lock()
	c.totals.Epochs++
	c.totals.RawOps += rawOps
	c.totals.RawRecordMutations += rawRecs
	c.totals.NetEntryMutations += uint64(len(entries))
	c.totals.NetInodeMutations += uint64(len(inodes))
	c.totals.CancelledEntries += cancelledEntries
	c.totals.CancelledInodes += cancelledInodes
	c.totals.CollapsedParentDirs += collapsedParents
	c.totals.CrossDirRenames += uint64(len(deps))
	c.statsMu.Unlock()

	return EpochPlan{
		EpochID:       epochID,
		MinLSN:        minLSN,
		MaxLSN:        maxLSN,
		TouchedDirs:   touchedDirs,
		ParentTouches: parentTouches,
		RawOps:        rawOps,
		RawRecords:    rawRecs,
		Entries:       entries,
		Inodes:        inodes,
		Deps:          deps,
	}
}

// Stats returns cumulative coalescing statistics across sealed epochs.
func (c *Coalescer) Stats() Stats {
	c.statsMu.Lock()
	defer c.statsMu.Unlock()
	return c.totals
}

// ReplayOp applies a decoded journal Op back into Coalescer during crash recovery,
// preserving exact LSN monotonicity and inode assignments.
func (c *Coalescer) ReplayOp(op Op) error {
	c.epochMu.Lock()
	defer c.epochMu.Unlock()

	if op.LSN > c.nextLSN.Load() {
		c.nextLSN.Store(op.LSN)
	}
	if c.epochMin.Load() == 0 || op.LSN < c.epochMin.Load() {
		c.epochMin.Store(op.LSN)
	}
	if uint64(op.Ino) >= c.nextIno.Load() {
		c.nextIno.Store(uint64(op.Ino) + 1)
	}

	switch op.Kind {
	case OpCreate, OpMkdir:
		dIdx := dirShardIdx(op.Dir)
		dd := c.dirShards[dIdx].dirs[op.Dir]
		if dd == nil {
			dd = &dirDelta{ino: op.Dir, slots: make(map[string]*slotState)}
			c.dirShards[dIdx].dirs[op.Dir] = dd
		}
		st := dd.slots[op.Name]
		if st == nil {
			st = &slotState{}
			dd.slots[op.Name] = st
		}
		st.present = true
		st.curChild = op.Ino
		st.curMode = op.Mode
		dd.rawTouches++
		dd.lastTouchNs = op.TimeNs
		if pIno := c.inoShards[inoShardIdx(op.Dir)].inodes[op.Dir]; pIno != nil {
			pIno.liveChildren.Add(1)
			pIno.mtimeNs = op.TimeNs
		}
		var cp []byte
		dirty := false
		if op.Kind == OpCreate && op.Data != nil {
			cp = append([]byte(nil), op.Data...)
			dirty = true
		}
		c.inoShards[inoShardIdx(op.Ino)].inodes[op.Ino] = &inoDelta{
			id:          op.Ino,
			mode:        op.Mode,
			bornInEpoch: true,
			liveLinks:   1,
			dataDirty:   dirty,
			inline:      cp,
			mtimeNs:     op.TimeNs,
		}
		if op.Kind == OpMkdir {
			cIdx := dirShardIdx(op.Ino)
			if c.dirShards[cIdx].dirs[op.Ino] == nil {
				c.dirShards[cIdx].dirs[op.Ino] = &dirDelta{ino: op.Ino, slots: make(map[string]*slotState)}
			}
		}
		c.rawOps.Add(1)
		c.rawRecs.Add(3)

	case OpWriteInline:
		ind := c.inoShards[inoShardIdx(op.Ino)].inodes[op.Ino]
		if ind == nil {
			return fmt.Errorf("djfs: replay write for unknown inode %d", op.Ino)
		}
		ind.inline = append([]byte(nil), op.Data...)
		ind.dataDirty = true
		ind.mtimeNs = op.TimeNs
		c.rawOps.Add(1)
		c.rawRecs.Add(2)

	case OpRemove:
		dIdx := dirShardIdx(op.Dir)
		dd := c.dirShards[dIdx].dirs[op.Dir]
		if dd == nil {
			return fmt.Errorf("djfs: replay remove in unknown dir %d", op.Dir)
		}
		st := dd.slots[op.Name]
		if st == nil || !st.present {
			return fmt.Errorf("djfs: replay remove of missing %q in %d", op.Name, op.Dir)
		}
		child := st.curChild
		st.present = false
		st.curChild = 0
		st.curMode = 0
		dd.rawTouches++
		dd.lastTouchNs = op.TimeNs
		if pIno := c.inoShards[inoShardIdx(op.Dir)].inodes[op.Dir]; pIno != nil {
			pIno.liveChildren.Add(-1)
			pIno.mtimeNs = op.TimeNs
		}
		if cIno := c.inoShards[inoShardIdx(child)].inodes[child]; cIno != nil {
			cIno.liveLinks--
			if cIno.liveLinks <= 0 {
				cIno.inline = nil
			}
		}
		c.rawOps.Add(1)
		c.rawRecs.Add(3)

	case OpRename:
		if op.Dir == op.DstDir && op.Name == op.DstName {
			return nil
		}
		sdd := c.dirShards[dirShardIdx(op.Dir)].dirs[op.Dir]
		if sdd == nil {
			return fmt.Errorf("djfs: replay rename missing src dir %d", op.Dir)
		}
		srcSlot := sdd.slots[op.Name]
		if srcSlot == nil || !srcSlot.present {
			return fmt.Errorf("djfs: replay rename missing src %q", op.Name)
		}
		movingIno, movingMode := srcSlot.curChild, srcSlot.curMode

		tIdx := dirShardIdx(op.DstDir)
		tdd := c.dirShards[tIdx].dirs[op.DstDir]
		if tdd == nil {
			tdd = &dirDelta{ino: op.DstDir, slots: make(map[string]*slotState)}
			c.dirShards[tIdx].dirs[op.DstDir] = tdd
		}
		dstSlot := tdd.slots[op.DstName]
		if dstSlot != nil && dstSlot.present {
			if disp := c.inoShards[inoShardIdx(dstSlot.curChild)].inodes[dstSlot.curChild]; disp != nil {
				disp.liveLinks--
				if disp.liveLinks <= 0 {
					disp.inline = nil
				}
			}
		} else {
			if dstSlot == nil {
				dstSlot = &slotState{}
				tdd.slots[op.DstName] = dstSlot
			}
			if dstP := c.inoShards[inoShardIdx(op.DstDir)].inodes[op.DstDir]; dstP != nil {
				dstP.liveChildren.Add(1)
			}
		}
		srcSlot.present = false
		srcSlot.curChild = 0
		srcSlot.curMode = 0
		sdd.rawTouches++
		if srcP := c.inoShards[inoShardIdx(op.Dir)].inodes[op.Dir]; srcP != nil {
			srcP.liveChildren.Add(-1)
		}
		dstSlot.present = true
		dstSlot.curChild = movingIno
		dstSlot.curMode = movingMode
		tdd.rawTouches++
		if op.Dir != op.DstDir {
			c.deps = append(c.deps, CrossDirDep{LSN: op.LSN, SrcDir: op.Dir, DstDir: op.DstDir})
		}
		c.rawOps.Add(1)
		c.rawRecs.Add(4)

	default:
		return fmt.Errorf("djfs: unknown op kind %d", op.Kind)
	}
	return nil
}

// EncodeOp serializes an Op into a compact length-framed binary journal record
// protected by a Castagnoli CRC-32 trailer.
func EncodeOp(op Op) []byte {
	buf := make([]byte, 0, 48+len(op.Name)+len(op.DstName)+len(op.Data))
	buf = binary.LittleEndian.AppendUint32(buf, journalMagic)
	buf = append(buf, byte(op.Kind))
	buf = binary.AppendUvarint(buf, op.LSN)
	buf = binary.AppendUvarint(buf, uint64(op.Dir))
	buf = binary.AppendUvarint(buf, uint64(len(op.Name)))
	buf = append(buf, op.Name...)
	buf = binary.AppendUvarint(buf, uint64(op.DstDir))
	buf = binary.AppendUvarint(buf, uint64(len(op.DstName)))
	buf = append(buf, op.DstName...)
	buf = binary.AppendUvarint(buf, uint64(op.Ino))
	buf = binary.AppendUvarint(buf, uint64(op.Mode))
	buf = binary.AppendVarint(buf, op.TimeNs)
	buf = binary.AppendUvarint(buf, uint64(len(op.Data)))
	buf = append(buf, op.Data...)
	sum := crc32.Checksum(buf, castagnoliTable)
	buf = binary.LittleEndian.AppendUint32(buf, sum)
	return buf
}

// ErrCorruptOp indicates a malformed or checksum-invalid serialized Op.
var ErrCorruptOp = errors.New("djfs: corrupt journal operation")

// DecodeOp deserializes an Op produced by EncodeOp.
func DecodeOp(b []byte) (Op, error) {
	if len(b) < 9 {
		return Op{}, ErrCorruptOp
	}
	body, tail := b[:len(b)-4], b[len(b)-4:]
	want := binary.LittleEndian.Uint32(tail)
	got := crc32.Checksum(body, castagnoliTable)
	if want != got {
		return Op{}, ErrCorruptOp
	}
	if binary.LittleEndian.Uint32(body[:4]) != journalMagic {
		return Op{}, ErrCorruptOp
	}
	r := bytes.NewReader(body[5:])
	readUvar := func() (uint64, error) { return binary.ReadUvarint(r) }
	readStr := func() (string, error) {
		n, err := readUvar()
		if err != nil || n > uint64(r.Len()) {
			return "", ErrCorruptOp
		}
		s := make([]byte, n)
		if _, err := r.Read(s); err != nil {
			return "", ErrCorruptOp
		}
		return string(s), nil
	}
	var op Op
	op.Kind = OpKind(body[4])
	var err error
	if op.LSN, err = readUvar(); err != nil {
		return Op{}, ErrCorruptOp
	}
	dir, err := readUvar()
	if err != nil {
		return Op{}, ErrCorruptOp
	}
	op.Dir = vfs.InodeID(dir)
	if op.Name, err = readStr(); err != nil {
		return Op{}, ErrCorruptOp
	}
	dstDir, err := readUvar()
	if err != nil {
		return Op{}, ErrCorruptOp
	}
	op.DstDir = vfs.InodeID(dstDir)
	if op.DstName, err = readStr(); err != nil {
		return Op{}, ErrCorruptOp
	}
	ino, err := readUvar()
	if err != nil {
		return Op{}, ErrCorruptOp
	}
	op.Ino = vfs.InodeID(ino)
	mode, err := readUvar()
	if err != nil {
		return Op{}, ErrCorruptOp
	}
	op.Mode = fs.FileMode(mode)
	if op.TimeNs, err = binary.ReadVarint(r); err != nil {
		return Op{}, ErrCorruptOp
	}
	dlen, err := readUvar()
	if err != nil || dlen > uint64(r.Len()) {
		return Op{}, ErrCorruptOp
	}
	if dlen > 0 {
		op.Data = make([]byte, dlen)
		if _, err := r.Read(op.Data); err != nil {
			return Op{}, ErrCorruptOp
		}
	}
	if r.Len() != 0 {
		return Op{}, ErrCorruptOp
	}
	return op, nil
}
