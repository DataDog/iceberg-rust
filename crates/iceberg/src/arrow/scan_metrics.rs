// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Scan metrics and I/O counting for Parquet data file reads.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;

use crate::error::Result;
use crate::io::FileRead;
use crate::scan::ArrowRecordBatchStream;

/// Wraps a [`FileRead`] to count bytes read via a shared atomic counter.
pub(crate) struct CountingFileRead<F: FileRead> {
    inner: F,
    metrics: ScanMetrics,
}

impl<F: FileRead> CountingFileRead<F> {
    pub(crate) fn new(inner: F, metrics: ScanMetrics) -> Self {
        Self { inner, metrics }
    }
}

#[async_trait::async_trait]
impl<F: FileRead> FileRead for CountingFileRead<F> {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        debug_assert!(range.end >= range.start);
        self.metrics
            .bytes_read
            .fetch_add(range.end - range.start, Ordering::Relaxed);
        self.metrics.read_requests.fetch_add(1, Ordering::Relaxed);

        let started = Instant::now();
        let result = self.inner.read(range).await;
        self.metrics
            .read_elapsed_nanos
            .fetch_add(duration_as_u64_nanos(started), Ordering::Relaxed);
        if result.is_err() {
            self.metrics.read_errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

/// Metrics collected during an Iceberg scan.
#[derive(Clone, Debug)]
pub struct ScanMetrics {
    bytes_read: Arc<AtomicU64>,
    read_requests: Arc<AtomicU64>,
    read_errors: Arc<AtomicU64>,
    read_elapsed_nanos: Arc<AtomicU64>,
    parquet_files_opened: Arc<AtomicU64>,
    file_open_elapsed_nanos: Arc<AtomicU64>,
    parquet_metadata_load_elapsed_nanos: Arc<AtomicU64>,
    file_scan_tasks_started: Arc<AtomicU64>,
}

impl ScanMetrics {
    pub(crate) fn new() -> Self {
        Self {
            bytes_read: Arc::new(AtomicU64::new(0)),
            read_requests: Arc::new(AtomicU64::new(0)),
            read_errors: Arc::new(AtomicU64::new(0)),
            read_elapsed_nanos: Arc::new(AtomicU64::new(0)),
            parquet_files_opened: Arc::new(AtomicU64::new(0)),
            file_open_elapsed_nanos: Arc::new(AtomicU64::new(0)),
            parquet_metadata_load_elapsed_nanos: Arc::new(AtomicU64::new(0)),
            file_scan_tasks_started: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn file_scan_task_started(&self) {
        self.file_scan_tasks_started.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn parquet_file_opened(&self) {
        self.parquet_files_opened.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_file_open_elapsed(&self, started: Instant) {
        self.file_open_elapsed_nanos
            .fetch_add(duration_as_u64_nanos(started), Ordering::Relaxed);
    }

    pub(crate) fn add_parquet_metadata_load_elapsed(&self, started: Instant) {
        self.parquet_metadata_load_elapsed_nanos
            .fetch_add(duration_as_u64_nanos(started), Ordering::Relaxed);
    }

    /// Total bytes requested from storage during this scan, including data and delete files.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Number of range-read requests issued to storage.
    pub fn read_requests(&self) -> u64 {
        self.read_requests.load(Ordering::Relaxed)
    }

    /// Number of storage range-read requests that returned an error.
    pub fn read_errors(&self) -> u64 {
        self.read_errors.load(Ordering::Relaxed)
    }

    /// Sum of storage range-read latency in nanoseconds.
    ///
    /// Concurrent requests overlap, so this can exceed wall-clock scan time.
    pub fn read_elapsed_nanos(&self) -> u64 {
        self.read_elapsed_nanos.load(Ordering::Relaxed)
    }

    /// Number of Parquet data and delete files successfully opened.
    pub fn parquet_files_opened(&self) -> u64 {
        self.parquet_files_opened.load(Ordering::Relaxed)
    }

    /// Sum of time spent opening Parquet data and delete files in nanoseconds.
    pub fn file_open_elapsed_nanos(&self) -> u64 {
        self.file_open_elapsed_nanos.load(Ordering::Relaxed)
    }

    /// Sum of time spent loading Parquet metadata in nanoseconds.
    pub fn parquet_metadata_load_elapsed_nanos(&self) -> u64 {
        self.parquet_metadata_load_elapsed_nanos
            .load(Ordering::Relaxed)
    }

    /// Number of data-file scan tasks whose processing has started.
    pub fn file_scan_tasks_started(&self) -> u64 {
        self.file_scan_tasks_started.load(Ordering::Relaxed)
    }
}

fn duration_as_u64_nanos(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// Result of [`ArrowReader::read`](super::ArrowReader::read), containing the
/// record batch stream and metrics collected during the scan.
pub struct ScanResult {
    stream: ArrowRecordBatchStream,
    metrics: ScanMetrics,
}

impl ScanResult {
    pub(crate) fn new(stream: ArrowRecordBatchStream, metrics: ScanMetrics) -> Self {
        Self { stream, metrics }
    }

    /// Consumes the result, returning only the record batch stream.
    pub fn stream(self) -> ArrowRecordBatchStream {
        self.stream
    }

    /// Returns a reference to the scan metrics.
    pub fn metrics(&self) -> &ScanMetrics {
        &self.metrics
    }
}
