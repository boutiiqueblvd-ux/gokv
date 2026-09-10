package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The manifest records the sequence watermark of the most recent compaction.
// A replica whose position is older than that watermark cannot be caught up
// incrementally, because the tombstones it still needs have been reclaimed;
// the leader sends it a full snapshot instead. Keeping the watermark on disk
// means that guarantee survives a restart of the leader.

func manifestPath(dir string) string { return filepath.Join(dir, "MANIFEST") }

func readManifest(dir string) uint64 {
	b, err := os.ReadFile(manifestPath(dir))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok || k != "merge_seq" {
			continue
		}
		n, err := strconv.ParseUint(v, 10, 64)
		if err == nil {
			return n
		}
	}
	return 0
}

func writeManifest(dir string, mergeSeq uint64) error {
	tmp := manifestPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "merge_seq=%d\n", mergeSeq); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	if err := os.Rename(tmp, manifestPath(dir)); err != nil {
		return err
	}
	return syncDir(dir)
}
