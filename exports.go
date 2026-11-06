package main

/*
#cgo LDFLAGS: -llog
#include <stdlib.h>
extern void WexDownloaderEmit(int is_err, const char* line);
*/
import "C"
import (
	"bytes"
	"context"
	"os"
	"sync"
	"unsafe"
)

var (
	dlRunMu    sync.Mutex
	dlCancelMu sync.Mutex
	dlCancel   context.CancelFunc
)

type dlLineWriter struct {
	err bool
	buf []byte
}

func (w *dlLineWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		line := w.buf[:i]
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		emitDL(w.err, line)
		w.buf = w.buf[i+1:]
	}
	return len(p), nil
}

func (w *dlLineWriter) flush() {
	if len(w.buf) == 0 {
		return
	}
	emitDL(w.err, w.buf)
	w.buf = nil
}

func emitDL(isErr bool, line []byte) {
	cs := C.CString(string(line))
	flag := C.int(0)
	if isErr {
		flag = 1
	}
	C.WexDownloaderEmit(flag, cs)
	C.free(unsafe.Pointer(cs))
}

//export WexDownloaderCancel
func WexDownloaderCancel() {
	dlCancelMu.Lock()
	c := dlCancel
	dlCancelMu.Unlock()
	if c != nil {
		c()
	}
}

//export WexDownloaderRun
func WexDownloaderRun(argc C.int, argv **C.char, cwd *C.char) C.int {
	dlRunMu.Lock()
	defer dlRunMu.Unlock()

	n := int(argc)
	args := make([]string, 0, n)
	if argv != nil && n > 0 {
		ptr := unsafe.Slice(argv, n)
		for i := 0; i < n; i++ {
			if ptr[i] != nil {
				args = append(args, C.GoString(ptr[i]))
			}
		}
	}
	if len(args) == 0 {
		args = []string{"wex_downloader"}
	}
	_ = cwd

	ctx, cancel := context.WithCancel(context.Background())
	dlCancelMu.Lock()
	dlCancel = cancel
	dlCancelMu.Unlock()
	defer func() {
		dlCancelMu.Lock()
		dlCancel = nil
		dlCancelMu.Unlock()
		cancel()
	}()

	outW := &dlLineWriter{}
	errW := &dlLineWriter{err: true}
	oldOut, oldErr := os.Stdout, os.Stderr
	rout, wout, _ := os.Pipe()
	rerr, werr, _ := os.Pipe()
	os.Stdout, os.Stderr = wout, werr
	defer func() {
		os.Stdout, os.Stderr = oldOut, oldErr
	}()

	var wg sync.WaitGroup
	copyPipe := func(r *os.File, w *dlLineWriter) {
		defer wg.Done()
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				_, _ = w.Write(buf[:n])
			}
			if err != nil {
				w.flush()
				return
			}
		}
	}
	wg.Add(2)
	go copyPipe(rout, outW)
	go copyPipe(rerr, errW)

	code := run(ctx, args)

	_ = wout.Close()
	_ = werr.Close()
	wg.Wait()
	_ = rout.Close()
	_ = rerr.Close()
	return C.int(code)
}
