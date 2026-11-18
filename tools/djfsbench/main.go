// Copyright 2026 Hanzo AI Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Command djfsbench runs the reproducible DJFS directory-mutation coalescing
// benchmark suite (HIP-1342, Refs #20), reporting Plain NVMe beside uncoalesced
// vfs.Volume and DJFS coalesced vfs.Volume + journal.Journal across npm-ci,
// 100k-op storm, MDTest, Varmail, and a monotonic negative control.
//
//	go run ./tools/djfsbench
//	go run ./tools/djfsbench -full
//	go run ./tools/djfsbench -json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/hanzoai/vfs/pkg/djfs"
)

func main() {
	dir := flag.String("dir", "", "scratch directory (defaults to temporary directory on local disk)")
	full := flag.Bool("full", false, "run full-scale workload sizes")
	asJSON := flag.Bool("json", false, "emit machine-readable JSON")
	flag.Parse()

	rep, err := djfs.RunSuite(context.Background(), *dir, *full)
	if err != nil {
		fmt.Fprintf(os.Stderr, "djfsbench: %v\n", err)
		os.Exit(1)
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintf(os.Stderr, "djfsbench: encode: %v\n", err)
			os.Exit(1)
		}
		return
	}

	fmt.Printf("DJFS Directory Mutation Coalescing Report (HIP-1342, Refs #20)\n")
	fmt.Printf("==============================================================\n")
	fmt.Printf("Scale              : %s\n", rep.Scale)
	fmt.Printf("100k mixed storm   : %.2f ms (%.0f ops/s)\n\n", rep.Storm100kOpsMs, rep.Storm100kOpsSec)

	n := rep.NpmCI
	fmt.Printf("1. npm-ci Tarball Staging Storm (%d packages, %d surviving files, %d raw ops)\n",
		n.Packages, n.SurvivingFiles, n.RawNamespaceOps)
	fmt.Printf("| System | Wall Time (ms) | Total S3 PUTs | PUTs / Surviving File | Cancelled Records |\n")
	fmt.Printf("|---|---:|---:|---:|---:|\n")
	fmt.Printf("| Plain NVMe | %.2f | n/a | n/a | n/a |\n", n.PlainNVMeMs)
	fmt.Printf("| Uncoalesced vfs.Volume | %.2f | %d | %.4f | 0 |\n",
		n.UncoalescedMs, n.UncoalescedPuts, n.UncoalescedPutsFile)
	fmt.Printf("| DJFS Coalesced Volume + Journal | %.2f (pub %.2f) | %d (meta=%d,pack=%d,head=%d) | %.4f | %d |\n\n",
		n.DJFSMs, n.DJFSPublishMs, n.DJFSPuts, n.DJFSShardPuts, n.DJFSPackPuts, n.DJFSHeadPuts,
		n.DJFSPutsPerFile, n.CancelledRecords)

	m := rep.MDTest
	fmt.Printf("2. MDTest Directory Storm (%d files)\n", m.Files)
	fmt.Printf("| System | Create (ops/s) | Stat/Lookup (ops/s) | Delete (ops/s) | S3 PUTs |\n")
	fmt.Printf("|---|---:|---:|---:|---:|\n")
	fmt.Printf("| Plain NVMe | %.0f | %.0f | %.0f | n/a |\n",
		m.PlainNVMeCreateOpsSec, m.PlainNVMeStatOpsSec, m.PlainNVMeDeleteOpsSec)
	fmt.Printf("| Uncoalesced vfs.Volume | %.0f | %.0f | %.0f | %d |\n",
		m.RawVFSCreateOpsSec, m.RawVFSStatOpsSec, m.RawVFSDeleteOpsSec, m.RawVFSPuts)
	fmt.Printf("| DJFS Coalesced Volume | %.0f | %.0f | %.0f | %d |\n\n",
		m.DJFSCreateOpsSec, m.DJFSStatOpsSec, m.DJFSDeleteOpsSec, m.DJFSPuts)

	v := rep.Varmail
	fmt.Printf("3. Varmail Delivery + Fsync Storm (%d deliveries)\n", v.Deliveries)
	fmt.Printf("| System | Throughput (deliveries/s) | Mean Fsync (us) | Fsync vs NVMe | S3 PUTs | Pub <= 5s |\n")
	fmt.Printf("|---|---:|---:|---:|---:|---:|\n")
	fmt.Printf("| Plain NVMe | %.0f | %.2f | 1.00x | n/a | n/a |\n", v.PlainNVMeOpsSec, v.PlainNVMeFsyncUs)
	fmt.Printf("| Uncoalesced vfs.Volume | %.0f | n/a (remote Flush) | n/a | %d | yes |\n", v.UncoalescedOpsSec, v.UncoalescedPuts)
	fmt.Printf("| DJFS + Group Journal | %.0f | %.2f | %.2fx | %d | %v |\n\n",
		v.DJFSOpsSec, v.DJFSFsyncUs, v.DJFSFsyncVsNVMe, v.DJFSPuts, v.DJFSPublishWithin5s)

	nc := rep.NegativeControl
	fmt.Printf("4. Negative Control: Monotonic Permanent Creates (%d files, 0%% churn)\n", nc.Files)
	fmt.Printf("| System | Wall Time (ms) | S3 PUTs | Cancelled Inodes |\n")
	fmt.Printf("|---|---:|---:|---:|\n")
	fmt.Printf("| Plain NVMe | %.2f | n/a | n/a |\n", nc.PlainNVMeMs)
	fmt.Printf("| Raw vfs.Volume (single Flush) | %.2f | %d | 0 |\n", nc.RawVFSMs, nc.RawVFSPuts)
	fmt.Printf("| DJFS Coalesced Volume | %.2f | %d | %d |\n", nc.DJFSMs, nc.DJFSPuts, nc.CancelledInode)
}
