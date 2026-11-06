# wex-fastest-downloader

[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Platform](https://img.shields.io/badge/Platform-Android%20%7C%20Linux%20%7C%20macOS%20%7C%20Windows-blue)](https://github.com/inclusivnp-max/wex-fastest-downloader)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Architecture](https://img.shields.io/badge/Arch-Segmented%20Multi--Worker-orange)]()

High-performance concurrent segmented downloader engine written in Go with JNI integration for Android. Designed to maximize throughput on unreliable or throttled connections, survive carrier tethering disconnects, and seamlessly operate both as a standalone CLI executable and an in-process native Android library.

---

## Key Features

- **Multi-Worker Segmented Downloads**: Dynamically chunks files into 4 MiB – 32 MiB segments with up to 8 concurrent HTTP `Range` workers.
- **Fail-Safe Resume Engine**: Automatically tracks completed chunks in a lightweight sidecar file (`<filename>.wexdl`) while writing to `<filename>.part`. Network drops or restarts cleanly resume without redownloading finished pieces.
- **Direct DNS Fallback**: Queries public DNS resolvers (`1.1.1.1`, `8.8.8.8`, `9.9.9.9`) directly over UDP/TCP. Bypasses broken carrier DNS, tethering `/etc/resolv.conf` deadlocks, and stale routing.
- **Android In-Process JNI Mode**: Bypasses Android 10+ SELinux restrictions (`execute_no_trans` / `noexec` on `filesDir`) by building as a native shared library (`-buildmode=c-shared`) driven via JNI without spawning child processes.
- **Streamable JSON Progress Protocol**: Emits line-delimited JSON events to `stdout` / JNI callbacks for reactive UI rendering (speed, percentage, ETA, chunks).

---

## Architecture Overview

```
                      +---------------------------------------+
                      |       Caller (Kotlin UI / CLI)        |
                      +-------------------+-------------------+
                                          |
                        [JNI in-process / CLI Pipe]
                                          |
                      +-------------------v-------------------+
                      |         WEX Download Engine           |
                      +-------------------+-------------------+
                                          |
              +---------------------------+---------------------------+
              |                           |                           |
    +---------v---------+       +---------v---------+       +---------v---------+
    | Dynamic DNS Stack |       | Chunk Allocator   |       | State Persistence |
    | 1.1.1.1 / 8.8.8.8 |       | 4..32 MiB Ranges  |       | .part + .wexdl    |
    +-------------------+       +-------------------+       +-------------------+
```

---

## CLI Usage

### Basic Command

```bash
./wex-fastest-downloader --url "https://example.com/archive.zip" --out "archive.zip"
```

### Options

| Flag | Default | Description |
|---|---|---|
| `--url` | *required* | Source URL to download |
| `--out` | *required* | Destination file path |
| `--conns` | `0` (auto) | Number of parallel chunk workers (1 to 8) |
| `--dns` | `1.1.1.1:53,...` | Custom comma-separated list of DNS servers (empty for system) |
| `--timeout` | `15s` | Dial and request header timeout |
| `--insecure` | `false` | Skip TLS certificate verification |
| `--resume` | `true` | Enable `.wexdl` state resume |

---

## JSON Event Protocol

All status events are emitted as single-line JSON objects to standard output:

#### 1. Ready Handshake
```json
{"type":"hello","version":"1.3.0","message":"wex_downloader ready"}
```

#### 2. Probe Response
```json
{"type":"info","url":"https://...","out":"/data/.../file.zip","total":104857600,"accept_ranges":true,"connections":4,"connections_auto":true,"proto":"http1","resume":true,"etag":"\"65a1-7c9\""}
```

#### 3. Realtime Progress Tick
```json
{"type":"progress","downloaded":26214400,"total":104857600,"percent":25.0,"speed":12582912.0,"active_workers":4,"eta_seconds":6,"mode":"multi"}
```

#### 4. Completion
```json
{"type":"done","path":"/data/.../file.zip","downloaded":104857600,"total":104857600,"avg_speed":11400200.5,"elapsed":9.19,"parts":8,"mode":"multi"}
```

---

## Android JNI Integration

When embedded into Android applications, compile as `-buildmode=c-shared`:

```kotlin
package client.wex.modules

object WexDownloaderJni {
    init {
        System.loadLibrary("wexdl")
    }

    external fun nativeRun(
        args: Array<String>,
        cwd: String?,
        stdoutCallback: DownloaderLineSink,
        stderrCallback: DownloaderLineSink
    ): Int

    external fun nativeCancel()
}

fun interface DownloaderLineSink {
    fun onLine(line: String)
}
```

The C bridge intercepts Go's output pipes and safely redirects every emitted event into the JVM main loop via JNI callbacks.

---

## Building from Source

### Standalone Executable (Host)

```bash
make build
```

Or using standard Go toolchain:
```bash
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o wex-fastest-downloader .
```

### Android NDK (ARM64 Shared Library)

```bash
export ANDROID_NDK=/path/to/android-ndk
export TOOLCHAIN=$ANDROID_NDK/toolchains/llvm/prebuilt/linux-x86_64/bin
export CC=$TOOLCHAIN/aarch64-linux-android30-clang

CGO_ENABLED=1 \
GOOS=android \
GOARCH=arm64 \
go build -trimpath -ldflags="-s -w" -buildmode=c-shared -o libwexdl.so .
```

---

## License

This project is licensed under the Apache License 2.0.
