// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package djfs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"sync"

	"github.com/hanzoai/vfs"
	"github.com/hanzoai/vfs/journal"
)

// Session couples a Coalescer with a live vfs.Volume and an optional local
// journal.Journal so metadata storms acknowledge at local NVMe speed and
// publish minimal coalesced prolly-tree commits to S3.
type Session struct {
	vol  *vfs.Volume
	coal *Coalescer
	jnl  *journal.Journal

	mu           sync.Mutex
	inoMap       map[vfs.InodeID]vfs.InodeID // logical DJFS InodeID -> committed Volume InodeID
	lastJSeq     uint64
	publishedSeq uint64
}

// NewSession initializes a DJFS Session seeded from the committed namespace of vol.
// If jnl is non-nil, any unpublished records in jnl are replayed in global LSN
// order before new mutations are accepted.
func NewSession(ctx context.Context, vol *vfs.Volume, jnl *journal.Journal) (*Session, error) {
	st := vol.Stats()
	c := NewCoalescer(vfs.InodeID(st.Inodes + 2))
	s := &Session{
		vol:    vol,
		coal:   c,
		jnl:    jnl,
		inoMap: map[vfs.InodeID]vfs.InodeID{vfs.RootInode: vfs.RootInode},
	}
	if err := s.seedTree(ctx, vfs.RootInode); err != nil {
		return nil, err
	}
	if jnl != nil {
		var ops []Op
		var maxSeq uint64
		if err := jnl.Replay(func(rec journal.Record) error {
			op, err := DecodeOp(rec.Data)
			if err != nil {
				return err
			}
			ops = append(ops, op)
			if rec.Seq > maxSeq {
				maxSeq = rec.Seq
			}
			return nil
		}); err != nil {
			return nil, err
		}
		sort.Slice(ops, func(i, j int) bool { return ops[i].LSN < ops[j].LSN })
		for _, op := range ops {
			if err := c.ReplayOp(op); err != nil {
				return nil, err
			}
		}
		s.lastJSeq = maxSeq
		c.SetAppendHook(func(op Op) (uint64, error) {
			seq, err := jnl.Append(ctx, EncodeOp(op))
			if err != nil {
				return 0, err
			}
			s.mu.Lock()
			if seq > s.lastJSeq {
				s.lastJSeq = seq
			}
			s.mu.Unlock()
			return seq, nil
		})
	}
	return s, nil
}

func (s *Session) seedTree(ctx context.Context, dir vfs.InodeID) error {
	ents, err := s.vol.ReadDir(ctx, dir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		in, err := s.vol.Stat(ctx, e.ID)
		if err != nil {
			return err
		}
		s.coal.RegisterBaseEntry(dir, e.Name, e.ID, in.Mode, in.Size)
		s.inoMap[e.ID] = e.ID
		if in.IsDir() {
			if err := s.seedTree(ctx, e.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// Coalescer returns the underlying Coalescer.
func (s *Session) Coalescer() *Coalescer { return s.coal }

// Fsync makes all mutations appended up to jSeq durable in the local journal
// via group sync, returning without issuing any synchronous S3 requests.
func (s *Session) Fsync(ctx context.Context, jSeq uint64) error {
	if s.jnl == nil || jSeq == 0 {
		return nil
	}
	return s.jnl.Sync(ctx, jSeq)
}

func (s *Session) readVolumeFile(ctx context.Context, vIno vfs.InodeID) ([]byte, error) {
	in, err := s.vol.Stat(ctx, vIno)
	if err != nil {
		return nil, err
	}
	if in.Size == 0 {
		return nil, nil
	}
	f, err := s.vol.Open(ctx, vIno)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, in.Size)
	n, err := f.ReadAt(ctx, buf, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf[:n], nil
}

// PublishEpoch seals the current DJFS epoch, syncs the local journal through
// the epoch cut, applies only the surviving net changes onto the underlying
// vfs.Volume, executes a single conditional-PUT Volume.Flush, and advances the
// journal's Published watermark.
func (s *Session) PublishEpoch(ctx context.Context) (EpochPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	plan := s.coal.SealEpoch()
	targetJSeq := s.lastJSeq
	if s.jnl != nil && targetJSeq > s.publishedSeq {
		if err := s.jnl.Sync(ctx, targetJSeq); err != nil {
			return plan, err
		}
	}

	if len(plan.Entries) == 0 && len(plan.Inodes) == 0 {
		if s.jnl != nil && targetJSeq > s.publishedSeq {
			if err := s.jnl.Publish(targetJSeq); err != nil {
				return plan, err
			}
			_ = s.jnl.Reclaim()
			s.publishedSeq = targetJSeq
		}
		return plan, nil
	}

	inoMutByID := make(map[vfs.InodeID]InodeMut, len(plan.Inodes))
	for _, im := range plan.Inodes {
		inoMutByID[im.ID] = im
	}

	// 1. Preserve bytes of any previously committed base file that was renamed to
	// a new path without rewriting its data, before base removals run.
	preservedBytes := make(map[vfs.InodeID][]byte)
	for _, em := range plan.Entries {
		if em.Del || em.Mode.IsDir() {
			continue
		}
		im, hasMut := inoMutByID[em.Child]
		if hasMut && (im.BornInEpoch || im.DataDirty) {
			continue
		}
		if vOld, ok := s.inoMap[em.Child]; ok {
			b, err := s.readVolumeFile(ctx, vOld)
			if err != nil {
				return plan, err
			}
			preservedBytes[em.Child] = b
		}
	}

	// 2. Remove deleted or replaced base files first, then base directories in
	// post-order (children before parents).
	type dirRemoval struct {
		parent vfs.InodeID
		name   string
		child  vfs.InodeID
	}
	var dirRemovals []dirRemoval
	for _, em := range plan.Entries {
		if !em.Del && !em.BaseReplaced {
			continue
		}
		vParent, ok := s.inoMap[em.Dir]
		if !ok {
			continue
		}
		if em.BaseWasDir {
			dirRemovals = append(dirRemovals, dirRemoval{
				parent: em.Dir,
				name:   em.Name,
				child:  em.BaseChild,
			})
			continue
		}
		if err := s.vol.Remove(ctx, vParent, em.Name); err != nil {
			return plan, fmt.Errorf("djfs: remove base file %q in %d: %w", em.Name, vParent, err)
		}
	}
	if len(dirRemovals) > 0 {
		childSet := make(map[vfs.InodeID]bool, len(dirRemovals))
		parentOf := make(map[vfs.InodeID]vfs.InodeID, len(dirRemovals))
		for _, dr := range dirRemovals {
			childSet[dr.child] = true
			parentOf[dr.child] = dr.parent
		}
		depthOf := func(id vfs.InodeID) int {
			d := 0
			for cur := id; childSet[cur] && d < 4096; cur = parentOf[cur] {
				d++
			}
			return d
		}
		sort.Slice(dirRemovals, func(i, j int) bool {
			return depthOf(dirRemovals[i].child) > depthOf(dirRemovals[j].child)
		})
		for _, dr := range dirRemovals {
			vParent, ok := s.inoMap[dr.parent]
			if !ok {
				continue
			}
			if err := s.vol.Remove(ctx, vParent, dr.name); err != nil {
				return plan, fmt.Errorf("djfs: remove base dir %q in %d: %w", dr.name, vParent, err)
			}
		}
	}

	// 3. Create surviving directories in topological parent-before-child order.
	var pendingDirs []EntryMut
	for _, em := range plan.Entries {
		if !em.Del && em.Mode.IsDir() {
			pendingDirs = append(pendingDirs, em)
		}
	}
	for len(pendingDirs) > 0 {
		progressed := false
		rest := pendingDirs[:0]
		for _, dm := range pendingDirs {
			vParent, ok := s.inoMap[dm.Dir]
			if !ok {
				rest = append(rest, dm)
				continue
			}
			perm := dm.Mode.Perm()
			if perm == 0 {
				perm = 0o755
			}
			created, err := s.vol.Mkdir(ctx, vParent, dm.Name, perm)
			if err != nil {
				return plan, fmt.Errorf("djfs: mkdir %q in %d: %w", dm.Name, vParent, err)
			}
			s.inoMap[dm.Child] = created.ID
			progressed = true
		}
		if !progressed {
			return plan, fmt.Errorf("djfs: unresolved parent directory dependency across %d dirs", len(rest))
		}
		pendingDirs = rest
	}

	// 4. Create surviving new or moved files, and apply dirty inline writes to
	// surviving in-place base files.
	placedInEpoch := make(map[vfs.InodeID]bool)
	for _, em := range plan.Entries {
		if em.Del || em.Mode.IsDir() {
			continue
		}
		vParent, ok := s.inoMap[em.Dir]
		if !ok {
			return plan, fmt.Errorf("djfs: missing mapped parent %d for %q", em.Dir, em.Name)
		}
		perm := em.Mode.Perm()
		if perm == 0 {
			perm = 0o644
		}
		var data []byte
		if im, ok := inoMutByID[em.Child]; ok && (im.BornInEpoch || im.DataDirty) {
			data = im.Inline
		} else if pb, ok := preservedBytes[em.Child]; ok {
			data = pb
		}
		created, err := s.vol.Create(ctx, vParent, em.Name, perm)
		if err != nil {
			return plan, fmt.Errorf("djfs: create %q in %d: %w", em.Name, vParent, err)
		}
		if len(data) > 0 {
			f, err := s.vol.Open(ctx, created.ID)
			if err != nil {
				return plan, err
			}
			_, werr := f.WriteAt(ctx, data, 0)
			cerr := f.Close()
			if werr != nil {
				return plan, werr
			}
			if cerr != nil {
				return plan, cerr
			}
		}
		s.inoMap[em.Child] = created.ID
		placedInEpoch[em.Child] = true
	}

	// Update surviving base files whose path did not change but whose bytes did.
	for _, im := range plan.Inodes {
		if im.Del || im.IsDir || im.BornInEpoch || !im.DataDirty || placedInEpoch[im.ID] {
			continue
		}
		vIno, ok := s.inoMap[im.ID]
		if !ok {
			continue
		}
		if err := s.vol.Truncate(ctx, vIno, 0); err != nil {
			return plan, err
		}
		if len(im.Inline) > 0 {
			f, err := s.vol.Open(ctx, vIno)
			if err != nil {
				return plan, err
			}
			_, werr := f.WriteAt(ctx, im.Inline, 0)
			cerr := f.Close()
			if werr != nil {
				return plan, werr
			}
			if cerr != nil {
				return plan, cerr
			}
		}
	}

	// Clean up unlinked logical IDs from inoMap.
	for _, im := range plan.Inodes {
		if im.Del {
			delete(s.inoMap, im.ID)
		}
	}

	// 5. Commit once to S3/backend and advance journal watermark.
	if err := s.vol.Flush(ctx); err != nil {
		return plan, err
	}
	if s.jnl != nil && targetJSeq > s.publishedSeq {
		if err := s.jnl.Publish(targetJSeq); err != nil {
			return plan, err
		}
		_ = s.jnl.Reclaim()
		s.publishedSeq = targetJSeq
	}
	return plan, nil
}
