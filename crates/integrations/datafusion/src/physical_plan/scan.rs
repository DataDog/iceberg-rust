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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use std::vec;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::{Stream, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, FileScanTaskStream, ScanMetrics};
use iceberg::table::Table;

use super::scan_planning::{IcebergScanConfig, build_table_scan};
use crate::to_datafusion_error;

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
#[derive(Debug)]
pub struct IcebergTableScan {
    /// A table in the catalog.
    table: Table,
    /// Snapshot, projection, output schema, and pushed predicates for this scan.
    scan_config: IcebergScanConfig,
    /// Stores certain, often expensive to compute,
    /// plan properties used in query optimization.
    plan_properties: Arc<PlanProperties>,
    /// Optional limit on the number of rows to return
    limit: Option<usize>,
    /// Pre-planned file scan tasks, grouped by partition. `None` keeps planning lazy.
    file_task_groups: Option<Vec<Arc<[FileScanTask]>>>,
    /// Execution metrics for this scan.
    metrics: ExecutionPlanMetricsSet,
}

impl IcebergTableScan {
    pub fn new(
        table: Table,
        scan_config: IcebergScanConfig,
        limit: Option<usize>,
        file_task_groups: Option<Vec<Vec<FileScanTask>>>,
    ) -> Self {
        let partition_count = file_task_groups.as_ref().map_or(1, |groups| groups.len());
        let plan_properties =
            IcebergTableScan::compute_properties(scan_config.output_schema(), partition_count);
        let file_task_groups = file_task_groups.map(|groups| {
            groups
                .into_iter()
                .map(Arc::<[FileScanTask]>::from)
                .collect()
        });

        Self {
            table,
            scan_config,
            plan_properties,
            limit,
            file_task_groups,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.scan_config.snapshot_id()
    }

    pub fn projection(&self) -> Option<&[String]> {
        self.scan_config.column_names()
    }

    pub fn predicates(&self) -> Option<&Predicate> {
        self.scan_config.predicates()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn scan_config(&self) -> &IcebergScanConfig {
        &self.scan_config
    }

    pub fn file_task_groups(&self) -> Option<&[Arc<[FileScanTask]>]> {
        self.file_task_groups.as_deref()
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef, partition_count: usize) -> Arc<PlanProperties> {
        Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl ExecutionPlan for IcebergTableScan {
    fn name(&self) -> &str {
        "IcebergTableScan"
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::common::DataFusionError::Internal(
                "IcebergTableScan is a leaf node and cannot have children".to_string(),
            ));
        }

        Ok(self)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let storage_metrics = DataFusionStorageMetrics::new(&self.metrics, partition);
        let stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> = match &self
            .file_task_groups
        {
            Some(file_task_groups) => {
                let Some(file_task_group) = file_task_groups.get(partition).cloned() else {
                    return Err(datafusion::common::DataFusionError::Internal(format!(
                        "IcebergTableScan partition {partition} does not exist; scan has {} partitions",
                        file_task_groups.len()
                    )));
                };

                let tasks: FileScanTaskStream = Box::pin(futures::stream::iter(
                    (0..file_task_group.len()).map(move |idx| Ok(file_task_group[idx].clone())),
                ));
                let scan_result = build_table_scan(&self.table, &self.scan_config)?
                    .arrow_reader_builder()
                    // Eager planning lets DataFusion drive scan concurrency via output
                    // partitions. Match DataFusion's FileStream model, where each
                    // output partition owns one ScanState; keep one data file in
                    // flight per output partition here.
                    // https://github.com/apache/datafusion/blob/ad8e7b7f2babe3fcddc3a4f9b5cd1ac0d1b16ad9/datafusion/datasource/src/file_stream/scan_state.rs#L42-L43
                    .with_data_file_concurrency_limit(1)
                    .build()
                    // TODO: Avoid cloning FileScanTasks here once ArrowReader can accept shared tasks.
                    .read(tasks)
                    .map_err(to_datafusion_error)?;
                let scan_metrics = scan_result.metrics().clone();
                let stream = scan_result.stream().map_err(to_datafusion_error);

                Box::pin(StorageMetricStream::new(
                    Box::pin(stream),
                    scan_metrics,
                    storage_metrics,
                ))
            }
            None => {
                let table = self.table.clone();
                let scan_config = self.scan_config.clone();
                let fut = async move {
                    let table_scan = build_table_scan(&table, &scan_config)?;
                    let tasks = table_scan.plan_files().await.map_err(to_datafusion_error)?;
                    let scan_result = table_scan
                        .arrow_reader_builder()
                        .build()
                        .read(tasks)
                        .map_err(to_datafusion_error)?;
                    let scan_metrics = scan_result.metrics().clone();
                    let stream = scan_result.stream().map_err(to_datafusion_error);
                    Ok::<_, datafusion::common::DataFusionError>(StorageMetricStream::new(
                        Box::pin(stream),
                        scan_metrics,
                        storage_metrics,
                    ))
                };

                Box::pin(futures::stream::once(fut).try_flatten())
            }
        };

        // Apply a scan-partition bound if specified. In eager planning this is only
        // a per-partition bound; DataFusion's GlobalLimitExec remains responsible
        // for enforcing the final global limit.
        let limited_stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            if let Some(limit) = self.limit {
                let mut remaining = limit;
                Box::pin(stream.try_filter_map(move |batch| {
                    futures::future::ready(if remaining == 0 {
                        Ok(None)
                    } else if batch.num_rows() <= remaining {
                        remaining -= batch.num_rows();
                        Ok(Some(batch))
                    } else {
                        let limited_batch = batch.slice(0, remaining);
                        remaining = 0;
                        Ok(Some(limited_batch))
                    })
                }))
            } else {
                Box::pin(stream)
            };

        let measured_stream = ScanMetricStream::new(
            limited_stream,
            BaselineMetrics::new(&self.metrics, partition),
            MetricBuilder::new(&self.metrics).subset_time("time_to_first_batch", partition),
            MetricBuilder::new(&self.metrics).subset_time("scan_elapsed", partition),
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            measured_stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn reset_state(self: Arc<Self>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            table: self.table.clone(),
            scan_config: self.scan_config.clone(),
            plan_properties: Arc::clone(&self.plan_properties),
            limit: self.limit,
            file_task_groups: self.file_task_groups.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }
}

#[derive(Clone)]
struct DataFusionStorageMetrics {
    storage_bytes_read: Count,
    storage_read_requests: Count,
    storage_read_errors: Count,
    storage_read_elapsed: Time,
    parquet_files_opened: Count,
    file_open_elapsed: Time,
    parquet_metadata_load_elapsed: Time,
    file_scan_tasks_started: Count,
}

impl DataFusionStorageMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            storage_bytes_read: MetricBuilder::new(metrics)
                .counter("storage_bytes_read", partition),
            storage_read_requests: MetricBuilder::new(metrics)
                .counter("storage_read_requests", partition),
            storage_read_errors: MetricBuilder::new(metrics)
                .counter("storage_read_errors", partition),
            storage_read_elapsed: MetricBuilder::new(metrics)
                .subset_time("storage_read_elapsed", partition),
            parquet_files_opened: MetricBuilder::new(metrics)
                .counter("parquet_files_opened", partition),
            file_open_elapsed: MetricBuilder::new(metrics)
                .subset_time("file_open_elapsed", partition),
            parquet_metadata_load_elapsed: MetricBuilder::new(metrics)
                .subset_time("parquet_metadata_load_elapsed", partition),
            file_scan_tasks_started: MetricBuilder::new(metrics)
                .counter("file_scan_tasks_started", partition),
        }
    }

    fn record(&self, scan_metrics: &ScanMetrics, previous: &mut ScanMetricSnapshot) {
        self.storage_bytes_read.add(delta_as_usize(
            scan_metrics.bytes_read(),
            &mut previous.storage_bytes_read,
        ));
        self.storage_read_requests.add(delta_as_usize(
            scan_metrics.read_requests(),
            &mut previous.storage_read_requests,
        ));
        self.storage_read_errors.add(delta_as_usize(
            scan_metrics.read_errors(),
            &mut previous.storage_read_errors,
        ));
        add_elapsed_delta(
            &self.storage_read_elapsed,
            scan_metrics.read_elapsed_nanos(),
            &mut previous.storage_read_elapsed,
        );
        self.parquet_files_opened.add(delta_as_usize(
            scan_metrics.parquet_files_opened(),
            &mut previous.parquet_files_opened,
        ));
        add_elapsed_delta(
            &self.file_open_elapsed,
            scan_metrics.file_open_elapsed_nanos(),
            &mut previous.file_open_elapsed,
        );
        add_elapsed_delta(
            &self.parquet_metadata_load_elapsed,
            scan_metrics.parquet_metadata_load_elapsed_nanos(),
            &mut previous.parquet_metadata_load_elapsed,
        );
        self.file_scan_tasks_started.add(delta_as_usize(
            scan_metrics.file_scan_tasks_started(),
            &mut previous.file_scan_tasks_started,
        ));
    }
}

#[derive(Default)]
struct ScanMetricSnapshot {
    storage_bytes_read: u64,
    storage_read_requests: u64,
    storage_read_errors: u64,
    storage_read_elapsed: u64,
    parquet_files_opened: u64,
    file_open_elapsed: u64,
    parquet_metadata_load_elapsed: u64,
    file_scan_tasks_started: u64,
}

struct StorageMetricStream {
    stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    scan_metrics: ScanMetrics,
    datafusion_metrics: DataFusionStorageMetrics,
    previous: ScanMetricSnapshot,
}

impl StorageMetricStream {
    fn new(
        stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
        scan_metrics: ScanMetrics,
        datafusion_metrics: DataFusionStorageMetrics,
    ) -> Self {
        Self {
            stream,
            scan_metrics,
            datafusion_metrics,
            previous: ScanMetricSnapshot::default(),
        }
    }

    fn record_metrics(&mut self) {
        self.datafusion_metrics
            .record(&self.scan_metrics, &mut self.previous);
    }
}

impl Stream for StorageMetricStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.stream.as_mut().poll_next(cx);
        self.record_metrics();
        poll
    }
}

impl Drop for StorageMetricStream {
    fn drop(&mut self) {
        self.record_metrics();
    }
}

struct ScanMetricStream {
    stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
    baseline_metrics: BaselineMetrics,
    time_to_first_batch: Time,
    scan_elapsed: Time,
    started: Instant,
    first_batch_recorded: bool,
    finished: bool,
}

impl ScanMetricStream {
    fn new(
        stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>,
        baseline_metrics: BaselineMetrics,
        time_to_first_batch: Time,
        scan_elapsed: Time,
    ) -> Self {
        Self {
            stream,
            baseline_metrics,
            time_to_first_batch,
            scan_elapsed,
            started: Instant::now(),
            first_batch_recorded: false,
            finished: false,
        }
    }

    fn finish(&mut self) {
        if !self.finished {
            self.scan_elapsed.add_elapsed(self.started);
            self.finished = true;
        }
    }
}

impl Stream for ScanMetricStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let poll = {
            let _timer = elapsed_compute.timer();
            self.stream.as_mut().poll_next(cx)
        };

        match &poll {
            Poll::Ready(Some(Ok(_batch))) if !self.first_batch_recorded => {
                self.time_to_first_batch.add_elapsed(self.started);
                self.first_batch_recorded = true;
            }
            Poll::Ready(Some(Err(_))) | Poll::Ready(None) => self.finish(),
            Poll::Pending | Poll::Ready(Some(Ok(_))) => {}
        }

        self.baseline_metrics.record_poll(poll)
    }
}

impl Drop for ScanMetricStream {
    fn drop(&mut self) {
        self.finish();
    }
}

fn delta_as_usize(current: u64, previous: &mut u64) -> usize {
    let delta = current.saturating_sub(*previous);
    *previous = current;
    usize::try_from(delta).unwrap_or(usize::MAX)
}

fn add_elapsed_delta(metric: &Time, current: u64, previous: &mut u64) {
    let delta = current.saturating_sub(*previous);
    *previous = current;
    if delta > 0 {
        metric.add_duration(Duration::from_nanos(delta));
    }
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "IcebergTableScan projection:[{}] predicate:[{}]",
            self.projection().map_or(String::new(), |v| v.join(",")),
            self.predicates()
                .map_or(String::from(""), |p| format!("{p}")),
        )?;
        if let Some(file_task_groups) = &self.file_task_groups {
            let task_count: usize = file_task_groups.iter().map(|group| group.len()).sum();
            write!(
                f,
                " task_groups:[{}] tasks:[{}]",
                file_task_groups.len(),
                task_count,
            )?;
        }
        if let Some(limit) = self.limit {
            write!(f, " limit:[{limit}]")?;
        }
        Ok(())
    }
}
