package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	version		= "1.3.0"
	appUA		= "WEX-Downloader/" + version
	writeBufferSize	= 1 << 20
	progressTick	= 750 * time.Millisecond
	maxWorkers	= 8
	maxRetries	= 5
	probeRetries	= 3
	minPiece	= 4 << 20
	maxPiece	= 32 << 20
	stateVersion	= 1

	defaultDNS	= "1.1.1.1:53,8.8.8.8:53,1.0.0.1:53,8.8.4.4:53,9.9.9.9:53"
)

func newResolver(servers string) *net.Resolver {
	list := make([]string, 0, 5)
	for _, s := range strings.Split(servers, ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}

		if _, _, err := net.SplitHostPort(s); err != nil {
			s = net.JoinHostPort(s, "53")
		}
		list = append(list, s)
	}
	if len(list) == 0 {
		return nil
	}

	var idx uint32
	return &net.Resolver{
		PreferGo:	true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := &net.Dialer{Timeout: 5 * time.Second}

			start := int(atomic.AddUint32(&idx, 1))
			var lastErr error
			for i := 0; i < len(list); i++ {
				srv := list[(start+i)%len(list)]

				netw := network
				if netw == "" {
					netw = "udp"
				}
				conn, err := d.DialContext(ctx, netw, srv)
				if err == nil {
					return conn, nil
				}
				lastErr = err
			}
			if lastErr == nil {
				lastErr = fmt.Errorf("no DNS servers configured")
			}
			return nil, lastErr
		},
	}
}

type event map[string]any

func emit(v event) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	b = append(b, '\n')
	_, _ = os.Stdout.Write(b)
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ch:
			cancel()
		case <-ctx.Done():
		}
	}()
	go func() {
		buf := make([]byte, 1)
		_, _ = os.Stdin.Read(buf)
		cancel()
	}()
	os.Exit(run(ctx, os.Args))
}

func run(ctx context.Context, argv []string) int {
	fs := flag.NewFlagSet("wex_downloader", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	url := fs.String("url", "", "Download URL")
	out := fs.String("out", "", "Output file path")
	connections := fs.Int("connections", 0, "Workers 1-8; 0 = auto by file size")
	ua := fs.String("user-agent", appUA, "User-Agent")
	headerTimeoutSec := fs.Int("timeout", 45, "Connect/TLS/response-header timeout seconds")
	insecure := fs.Bool("insecure", false, "Skip TLS verify")
	forceHTTP1 := fs.Bool("http1", true, "Force HTTP/1.1 for parallel TCP")
	resume := fs.Bool("resume", true, "Resume from .part + .wexdl if present")
	dnsServers := fs.String("dns", defaultDNS, "Comma-separated DNS servers (host:port) to use instead of /etc/resolv.conf")
	printVersion := fs.Bool("version", false, "Print version and exit")
	rest := argv
	if len(rest) > 0 {
		rest = rest[1:]
	}
	if err := fs.Parse(rest); err != nil {
		emit(event{"type": "error", "message": err.Error()})
		return 1
	}

	if *printVersion {
		fmt.Printf("wex_downloader %s\n", version)
		return 0
	}
	if strings.TrimSpace(*url) == "" || strings.TrimSpace(*out) == "" {
		emit(event{"type": "error", "message": "url and out are required"})
		return 1
	}
	if *connections < 0 {
		*connections = 0
	}
	if *connections > maxWorkers {
		*connections = maxWorkers
	}
	if *headerTimeoutSec < 5 {
		*headerTimeoutSec = 5
	}

	emit(event{"type": "hello", "version": version, "message": "wex_downloader ready"})

	connCap := *connections
	if connCap <= 0 {
		connCap = maxWorkers
	}

	headerTO := time.Duration(*headerTimeoutSec) * time.Second
	dialer := &net.Dialer{
		Timeout:	20 * time.Second,
		KeepAlive:	30 * time.Second,
		Resolver:	newResolver(*dnsServers),
	}
	transport := &http.Transport{
		Proxy:			http.ProxyFromEnvironment,
		DialContext:		dialer.DialContext,
		TLSHandshakeTimeout:	headerTO,
		ResponseHeaderTimeout:	headerTO,
		ExpectContinueTimeout:	5 * time.Second,
		MaxIdleConns:		16,
		MaxIdleConnsPerHost:	connCap,
		MaxConnsPerHost:	connCap,
		IdleConnTimeout:	90 * time.Second,
		DisableCompression:	true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify:	*insecure,
			ClientSessionCache:	tls.NewLRUClientSessionCache(32),
		},
		ForceAttemptHTTP2:	!*forceHTTP1,
	}
	if *forceHTTP1 {
		transport.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}

	client := &http.Client{Timeout: 0, Transport: transport}
	proto := "http2"
	if *forceHTTP1 {
		proto = "http1"
	}

	effectiveConns := *connections
	if proto == "http2" && effectiveConns <= 0 {

		effectiveConns = 1
	}

	if err := download(ctx, client, *url, *out, effectiveConns, *ua, *resume, proto); err != nil {
		if ctx.Err() != nil {
			emit(event{"type": "cancelled", "message": "download cancelled", "resumable": true})
			return 2
		}
		emit(event{"type": "error", "message": err.Error()})
		return 1
	}
	return 0
}

type probeResult struct {
	size		int64
	acceptRanges	bool
	etag		string
	lastModified	string
}

type resumeState struct {
	Version		int	`json:"v"`
	URL		string	`json:"url"`
	Size		int64	`json:"size"`
	ETag		string	`json:"etag"`
	LastMod		string	`json:"last_mod"`
	PieceSize	int64	`json:"piece_size"`
	Done		[]int	`json:"done"`
}

func partPath(out string) string	{ return out + ".part" }
func statePath(out string) string	{ return out + ".wexdl" }

func ifRangeValue(etag, lastMod string) string {
	if etag != "" && !strings.HasPrefix(etag, "W/") {
		return etag
	}
	if lastMod != "" {
		return lastMod
	}

	return ""
}

func download(ctx context.Context, client *http.Client, rawURL, outPath string, requested int, ua string, allowResume bool, proto string) error {

	var pr probeResult
	var err error
	for attempt := 0; attempt < probeRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		pr, err = probe(ctx, client, rawURL, ua)
		if err == nil {
			break
		}
		if attempt == probeRetries-1 {
			break
		}
		delay := time.Duration(400*(1<<attempt)) * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	if err != nil {
		return err
	}
	size := pr.size
	supportsRange := pr.acceptRanges

	workers := chooseWorkers(size, requested, supportsRange)
	auto := requested <= 0

	emit(event{
		"type":			"info",
		"total":		size,
		"accept_ranges":	supportsRange,
		"connections":		workers,
		"connections_auto":	auto,
		"url":			rawURL,
		"out":			outPath,
		"etag":			pr.etag,
		"proto":		proto,
		"resume":		allowResume,
	})

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return err
	}

	if !supportsRange || size <= 0 || workers == 1 || size < minPiece {
		return downloadSingle(ctx, client, rawURL, outPath, size, ua, allowResume, pr)
	}

	return downloadMulti(ctx, client, rawURL, outPath, size, workers, ua, allowResume, pr, proto)
}

func chooseWorkers(size int64, requested int, supportsRange bool) int {
	if !supportsRange || size <= 0 {
		return 1
	}

	var bySize int
	switch {
	case size <= 8<<20:
		bySize = 1
	case size <= 64<<20:
		bySize = 4
	case size <= 512<<20:
		bySize = 6
	default:
		bySize = 8
	}
	if requested <= 0 {
		return bySize
	}

	if size <= 8<<20 {
		return 1
	}
	if size <= 64<<20 && requested > 4 {
		return 4
	}
	return requested
}

func downloadMulti(
	ctx context.Context,
	client *http.Client,
	rawURL, outPath string,
	size int64,
	workers int,
	ua string,
	allowResume bool,
	pr probeResult,
	proto string,
) error {
	pPath := partPath(outPath)
	sPath := statePath(outPath)

	pieceSize := choosePieceSize(size, workers)
	type piece struct {
		index	int
		start	int64
		end	int64
	}
	var pieces []piece
	for start, idx := int64(0), 0; start < size; start, idx = start+pieceSize, idx+1 {
		end := start + pieceSize - 1
		if end >= size {
			end = size - 1
		}
		pieces = append(pieces, piece{index: idx, start: start, end: end})
	}

	doneSet := map[int]struct{}{}
	resuming := false
	if allowResume {
		if st, ok := loadState(sPath); ok && stateMatches(st, rawURL, size, pr, pieceSize, len(pieces)) {
			for _, i := range st.Done {
				if i >= 0 && i < len(pieces) {
					doneSet[i] = struct{}{}
				}
			}
			if fi, err := os.Stat(pPath); err == nil && fi.Size() == size {
				resuming = len(doneSet) > 0
			} else {

				doneSet = map[int]struct{}{}
				_ = os.Remove(sPath)
			}
		} else {

			_ = os.Remove(sPath)
			_ = os.Remove(pPath)
		}
	} else {
		_ = os.Remove(sPath)
		_ = os.Remove(pPath)
	}

	var f *os.File
	var err error
	if resuming {
		f, err = os.OpenFile(pPath, os.O_RDWR, 0o644)
	} else {

		_ = os.Remove(pPath)
		f, err = os.OpenFile(pPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err == nil {
			if err = f.Truncate(size); err == nil {
				preallocate(f, size)
			}
		}

		_ = saveState(sPath, &resumeState{
			Version:	stateVersion,
			URL:		rawURL,
			Size:		size,
			ETag:		pr.etag,
			LastMod:	pr.lastModified,
			PieceSize:	pieceSize,
			Done:		nil,
		})
	}
	if err != nil {
		return err
	}

	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()

	var downloaded atomic.Int64
	var seed int64
	for i := range doneSet {
		p := pieces[i]
		seed += p.end - p.start + 1
	}
	downloaded.Store(seed)

	if resuming {
		emit(event{
			"type":		"info",
			"message":	"resume",
			"resumed":	true,
			"done_parts":	len(doneSet),
			"parts":	len(pieces),
			"downloaded":	seed,
			"total":	size,
		})
	}

	jobs := make(chan piece, len(pieces))
	pending := 0
	for _, p := range pieces {
		if _, ok := doneSet[p.index]; ok {
			continue
		}
		jobs <- p
		pending++
	}
	close(jobs)

	if pending == 0 {

		_ = f.Sync()
		_ = f.Close()
		f = nil
		return finalizePart(outPath, pPath, sPath, size, len(pieces), workers, seed, 0, proto)
	}

	var firstErr error
	var errOnce sync.Once
	var wg sync.WaitGroup
	var stateMu sync.Mutex
	startTime := time.Now()
	chunkCtx, chunkCancel := context.WithCancel(ctx)
	defer chunkCancel()

	persistDone := func(idx int) {
		stateMu.Lock()
		defer stateMu.Unlock()
		doneSet[idx] = struct{}{}
		list := make([]int, 0, len(doneSet))
		for i := range doneSet {
			list = append(list, i)
		}
		_ = saveState(sPath, &resumeState{
			Version:	stateVersion,
			URL:		rawURL,
			Size:		size,
			ETag:		pr.etag,
			LastMod:	pr.lastModified,
			PieceSize:	pieceSize,
			Done:		list,
		})
	}

	doneCh := make(chan struct{})
	go func() {
		t := time.NewTicker(progressTick)
		defer t.Stop()
		var lastBytes int64 = seed
		lastT := time.Now()
		for {
			select {
			case <-doneCh:
				return
			case <-chunkCtx.Done():
				return
			case now := <-t.C:
				cur := downloaded.Load()
				dt := now.Sub(lastT).Seconds()
				speed := float64(0)
				if dt > 0 {
					speed = float64(cur-lastBytes) / dt
				}
				elapsed := now.Sub(startTime).Seconds()

				session := cur - seed
				avg := float64(0)
				if elapsed > 0 && session > 0 {
					avg = float64(session) / elapsed
				}
				emit(event{
					"type":		"progress",
					"downloaded":	cur,
					"total":	size,
					"speed":	speed,
					"avg_speed":	avg,
					"percent":	float64(cur) * 100 / float64(size),
					"mode":		"multi",
					"parts":	len(pieces),
					"workers":	workers,
					"proto":	proto,
					"resumed":	resuming,
				})
				lastBytes = cur
				lastT = now
			}
		}
	}()

	nWorkers := workers
	if nWorkers > pending {
		nWorkers = pending
	}
	for i := 0; i < nWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				if chunkCtx.Err() != nil {
					return
				}
				err := downloadRangeWithRetry(
					chunkCtx, client, rawURL, f,
					p.start, p.end, ua, pr.etag, pr.lastModified, &downloaded,
				)
				if err != nil {
					errOnce.Do(func() {
						firstErr = err
						chunkCancel()
					})
					return
				}
				persistDone(p.index)
			}
		}()
	}
	wg.Wait()
	close(doneCh)

	if firstErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if got := downloaded.Load(); got != size {
		return fmt.Errorf("size mismatch: got %d want %d", got, size)
	}

	_ = f.Sync()
	elapsed := time.Since(startTime).Seconds()

	_ = f.Close()
	f = nil
	return finalizePart(outPath, pPath, sPath, size, len(pieces), workers, size, elapsed, proto)
}

func finalizePart(outPath, pPath, sPath string, size int64, parts, workers int, downloaded int64, elapsed float64, proto string) error {

	_ = os.Remove(outPath)
	if err := os.Rename(pPath, outPath); err != nil {

		if err2 := copyFile(pPath, outPath); err2 != nil {
			return fmt.Errorf("finalize: %v / %v", err, err2)
		}
		_ = os.Remove(pPath)
	}
	_ = os.Remove(sPath)
	if elapsed <= 0 {
		elapsed = 0.001
	}
	emit(event{
		"type":		"done",
		"downloaded":	downloaded,
		"total":	size,
		"path":		outPath,
		"mode":		"multi",
		"parts":	parts,
		"workers":	workers,
		"avg_speed":	float64(size) / elapsed,
		"elapsed":	elapsed,
		"proto":	proto,
	})
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func loadState(path string) (*resumeState, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var st resumeState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, false
	}
	if st.Version != stateVersion {
		return nil, false
	}
	return &st, true
}

func saveState(path string, st *resumeState) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func stateMatches(st *resumeState, url string, size int64, pr probeResult, pieceSize int64, nPieces int) bool {
	if st.URL != url || st.Size != size || st.PieceSize != pieceSize {
		return false
	}

	if pr.etag != "" && st.ETag != "" && pr.etag != st.ETag {
		return false
	}
	if pr.etag == "" && pr.lastModified != "" && st.LastMod != "" && pr.lastModified != st.LastMod {
		return false
	}
	for _, i := range st.Done {
		if i < 0 || i >= nPieces {
			return false
		}
	}
	return true
}

func choosePieceSize(size int64, workers int) int64 {
	if workers < 1 {
		workers = 1
	}
	piece := size / int64(workers*4)
	if piece < minPiece {
		piece = minPiece
	}
	if piece > maxPiece {
		piece = maxPiece
	}
	if piece > size {
		piece = size
	}
	return piece
}

func probe(ctx context.Context, client *http.Client, rawURL, ua string) (probeResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, rawURL, nil)
	if err != nil {
		return probeResult{}, err
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return probeRange(ctx, client, rawURL, ua)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return probeRange(ctx, client, rawURL, ua)
	}
	var size int64
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		size, _ = strconv.ParseInt(cl, 10, 64)
	}
	ar := strings.ToLower(resp.Header.Get("Accept-Ranges"))
	accept := strings.Contains(ar, "bytes") && size > 0
	pr := probeResult{
		size:		size,
		acceptRanges:	accept,
		etag:		resp.Header.Get("ETag"),
		lastModified:	resp.Header.Get("Last-Modified"),
	}
	if !accept && size > 0 {
		return probeRange(ctx, client, rawURL, ua)
	}
	return pr, nil
}

func probeRange(ctx context.Context, client *http.Client, rawURL, ua string) (probeResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return probeResult{}, err
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := client.Do(req)
	if err != nil {
		return probeResult{}, err
	}
	defer resp.Body.Close()

	pr := probeResult{
		etag:		resp.Header.Get("ETag"),
		lastModified:	resp.Header.Get("Last-Modified"),
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		cr := resp.Header.Get("Content-Range")
		slash := strings.LastIndexByte(cr, '/')
		if slash < 0 {
			return probeResult{}, fmt.Errorf("invalid Content-Range: %q", cr)
		}
		total, err := strconv.ParseInt(cr[slash+1:], 10, 64)
		if err != nil || total <= 0 {
			return probeResult{}, fmt.Errorf("invalid Content-Range: %q", cr)
		}
		_, _ = io.CopyN(io.Discard, resp.Body, 1)
		pr.size = total
		pr.acceptRanges = true
		return pr, nil
	case http.StatusOK:
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			pr.size, _ = strconv.ParseInt(cl, 10, 64)
		}
		pr.acceptRanges = false
		return pr, nil
	default:
		return probeResult{}, fmt.Errorf("probe failed: HTTP %d", resp.StatusCode)
	}
}

func downloadSingle(
	ctx context.Context,
	client *http.Client,
	rawURL, outPath string,
	knownSize int64,
	ua string,
	allowResume bool,
	pr probeResult,
) error {
	pPath := partPath(outPath)
	sPath := statePath(outPath)

	var startOff int64
	if allowResume && knownSize > 0 {
		if fi, err := os.Stat(pPath); err == nil && fi.Size() > 0 && fi.Size() < knownSize {

			if pr.acceptRanges {
				startOff = fi.Size()
			}
		}
	} else {
		_ = os.Remove(pPath)
		_ = os.Remove(sPath)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Accept-Encoding", "identity")
	if startOff > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", startOff))
		if v := ifRangeValue(pr.etag, pr.lastModified); v != "" {
			req.Header.Set("If-Range", v)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if startOff > 0 && resp.StatusCode == http.StatusOK {
		resp.Body.Close()
		startOff = 0
		_ = os.Remove(pPath)
		req2, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return err
		}
		req2.Header.Set("User-Agent", ua)
		req2.Header.Set("Accept-Encoding", "identity")
		resp, err = client.Do(req2)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if startOff > 0 && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("resume failed: HTTP %d", resp.StatusCode)
	}

	total := knownSize
	if total <= 0 {
		if cl := resp.Header.Get("Content-Length"); cl != "" {
			total, _ = strconv.ParseInt(cl, 10, 64)
			if startOff > 0 {

				total += startOff
			}
		}
	}

	var f *os.File
	if startOff > 0 {
		f, err = os.OpenFile(pPath, os.O_RDWR, 0o644)
		if err == nil {
			_, err = f.Seek(startOff, io.SeekStart)
		}
	} else {
		f, err = os.Create(pPath)
		if err == nil && total > 0 {
			_ = f.Truncate(total)
			preallocate(f, total)
		}
	}
	if err != nil {
		return err
	}
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()

	if startOff > 0 {
		emit(event{
			"type":	"info", "message": "resume", "resumed": true,
			"downloaded":	startOff, "total": total, "mode": "single",
		})
	}

	start := time.Now()
	downloaded := startOff
	buf := make([]byte, writeBufferSize)
	lastEmit := time.Now()
	lastBytes := downloaded
	for {
		if ctx.Err() != nil {

			return ctx.Err()
		}
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				return werr
			}
			downloaded += int64(n)
			now := time.Now()
			if now.Sub(lastEmit) >= progressTick {
				dt := now.Sub(lastEmit).Seconds()
				speed := float64(downloaded-lastBytes) / dt
				pct := float64(0)
				if total > 0 {
					pct = float64(downloaded) * 100 / float64(total)
				}
				emit(event{
					"type":		"progress",
					"downloaded":	downloaded,
					"total":	total,
					"speed":	speed,
					"avg_speed":	float64(downloaded-startOff) / now.Sub(start).Seconds(),
					"percent":	pct,
					"mode":		"single",
					"parts":	1,
				})
				lastEmit = now
				lastBytes = downloaded
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	_ = f.Sync()
	_ = f.Close()
	f = nil

	_ = os.Remove(outPath)
	if err := os.Rename(pPath, outPath); err != nil {
		if err2 := copyFile(pPath, outPath); err2 != nil {
			return err2
		}
		_ = os.Remove(pPath)
	}
	_ = os.Remove(sPath)

	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}
	emit(event{
		"type":		"done",
		"downloaded":	downloaded,
		"total":	total,
		"path":		outPath,
		"mode":		"single",
		"parts":	1,
		"avg_speed":	float64(downloaded-startOff) / elapsed,
		"elapsed":	elapsed,
	})
	return nil
}

func downloadRangeWithRetry(
	ctx context.Context,
	client *http.Client,
	rawURL string,
	f *os.File,
	start, end int64,
	ua, etag, lastMod string,
	downloaded *atomic.Int64,
) error {
	current := start
	for attempt := 0; current <= end; attempt++ {
		if attempt >= maxRetries {
			return fmt.Errorf("range %d-%d: retry limit exceeded at offset %d", start, end, current)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		written, err := downloadRangeOnce(ctx, client, rawURL, f, current, end, ua, etag, lastMod, downloaded)
		current += written
		if err == nil && current == end+1 {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		delay := time.Duration(250*(1<<attempt)) * time.Millisecond
		if delay > 4*time.Second {
			delay = 4 * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil
}

func downloadRangeOnce(
	ctx context.Context,
	client *http.Client,
	rawURL string,
	f *os.File,
	start, end int64,
	ua, etag, lastMod string,
	downloaded *atomic.Int64,
) (written int64, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	req.Header.Set("Accept-Encoding", "identity")
	if v := ifRangeValue(etag, lastMod); v != "" {
		req.Header.Set("If-Range", v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("server ignored range %d-%d: HTTP %d", start, end, resp.StatusCode)
	}
	expectedPrefix := fmt.Sprintf("bytes %d-%d/", start, end)
	cr := resp.Header.Get("Content-Range")
	if !strings.HasPrefix(cr, expectedPrefix) {
		if !strings.HasPrefix(cr, fmt.Sprintf("bytes %d-", start)) {
			return 0, fmt.Errorf("unexpected Content-Range %q, expected prefix %q", cr, expectedPrefix)
		}
	}

	expected := end - start + 1
	buf := make([]byte, writeBufferSize)
	offset := start
	var pending int64
	flushCounter := func() {
		if pending > 0 {
			downloaded.Add(pending)
			written += pending
			pending = 0
		}
	}

	for offset <= end {
		if ctx.Err() != nil {
			flushCounter()
			return written, ctx.Err()
		}
		toRead := int64(len(buf))
		remain := end - offset + 1
		if toRead > remain {
			toRead = remain
		}
		n, readErr := io.ReadFull(resp.Body, buf[:toRead])
		if n > 0 {
			if _, werr := f.WriteAt(buf[:n], offset); werr != nil {
				flushCounter()
				return written, werr
			}
			offset += int64(n)
			pending += int64(n)
			if pending >= 256*1024 {
				flushCounter()
			}
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			flushCounter()
			return written, readErr
		}
	}
	flushCounter()

	got := offset - start
	if got != expected {
		return written, fmt.Errorf("range incomplete: received %d of %d bytes (%d-%d)", got, expected, start, end)
	}
	return written, nil
}

func preallocate(f *os.File, size int64) {
	if size <= 0 || f == nil {
		return
	}
	fd := int(f.Fd())
	var trap uintptr
	switch runtime.GOARCH {
	case "arm64":
		trap = 47
	case "arm":
		trap = 352
	case "amd64":
		trap = 285
	default:
		return
	}
	_, _, _ = syscall.Syscall6(trap, uintptr(fd), 0, 0, uintptr(size), 0, 0)
}
