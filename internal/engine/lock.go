package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// The data directory holds an advisory flock so two processes cannot append to
// the same log. flock is released automatically by the kernel if the process
// dies, which means a crashed node can be restarted without manual cleanup.

func lockPath(dir string) string { return filepath.Join(dir, "LOCK") }

func acquireLock(dir string) (*os.File, error) {
	f, err := os.OpenFile(lockPath(dir), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("%w: %s", ErrLocked, dir)
	}
	f.Truncate(0)
	fmt.Fprintf(f, "%d\n", os.Getpid())
	return f, nil
}

func releaseLock(f *os.File, dir string) {
	if f == nil {
		return
	}
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	f.Close()
	os.Remove(lockPath(dir))
}
