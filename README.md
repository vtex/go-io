# go-io

## Overview

Collection of general-purpose packages for writing scalable go services.

Many sub-packages are merely compositions or thin layers on top of other
open source packages like for caching, monitoring, etc., offering easy
consumption by providing simplified configuration with sane defaults
and opinionated/higher-level usage interfaces.

Initially developed and open-source by VTEX IO Infra team.

## Packages

As we don't have much documentation in the code itself yet, here are some
high level descriptions for each of the root exported packages:
 - `cache`: Simplified interfaces for http, local and remote caching,
 as well as some ready-to-use implementations for "stale on error" and
 multi-layer "hybrid" cache.
 - `redis`: Provides an implementation for the `Cache` interface from the above
 package using a Redis instance as storage. Also provides implementation of an
 optimized and channel-oriented Redis PubSub client.
- `ioext`: Utilities related to I/O that could be in go's `io` package.
  * `ChunkedData` implements `gin.Render` interface for serving large responses
  with `Transfer-Encoding: chunked` and optimized buffer/memory and async flushing.
  * `TarGzWriter` and `Zip...` provide simplified interfaces for extracting and
  compressing data on those popular formats.
  * `Tee` creates an easy way to clone an `io.Reader`, effectively duplicating the
  contents streamed through it. e.g. Useful for caching in background while
  concurrently streaming response to client.
- `prometheus`: Opinionated higher-level interfaces for sending metrics of the system
to Prometheus.
- `sharedflight`: Like (and built on top of) [`singleflight`](golang.org/x/sync/singleflight),
but supporting shared cancellation based on all consumers waiting for response.
- `reflext` Utilities related to reflection that could be on `reflect`. Only a single
utility is currently provided, for panic-safely setting the value of a pointer with
explicit error handling, to avoid panicking to users of APIs.
