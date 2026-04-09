use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::to_datafusion_error;

const CHANNEL_BUFFER_SIZE: usize = 32;

/// A DataFusion [`ExecutionPlan`] that maps Iceberg bucket partitions to DataFusion partitions.
///
/// Each DataFusion partition corresponds to one Iceberg bucket value (0..N). A bucket may
/// contain zero, one, or multiple data files; `execute(i)` streams all RecordBatches for
/// bucket `i` sequentially via a single [`ArrowReaderBuilder`] pass.
///
/// Reports `Partitioning::Hash([source_col_expr], N)`, so DataFusion can eliminate `Exchange`
/// nodes for aggregations and joins on the bucketed column.
///
/// When an IO runtime [`Handle`] is provided via [`IcebergBucketScan::with_io_handle`],
/// Parquet reads are spawned on that runtime and bridged back via an mpsc channel, same
/// pattern as [`IcebergPartitionedScan`](crate::physical_plan::partitioned_scan::IcebergPartitionedScan).
#[derive(Debug, Clone)]
pub struct IcebergBucketScan {
    /// `tasks_by_bucket[i]` = all FileScanTasks whose Iceberg bucket value equals `i`.
    /// `len()` equals the bucket count N. Inner Vec may be empty (no files for that bucket).
    tasks_by_bucket: Vec<Vec<FileScanTask>>,
    file_io: FileIO,
    plan_properties: PlanProperties,
    io_handle: Option<Handle>,
}

impl IcebergBucketScan {
    pub fn new(
        tasks_by_bucket: Vec<Vec<FileScanTask>>,
        file_io: FileIO,
        schema: ArrowSchemaRef,
        source_col_expr: Arc<dyn PhysicalExpr>,
    ) -> Self {
        let bucket_count = tasks_by_bucket.len();
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::Hash(vec![source_col_expr], bucket_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            tasks_by_bucket,
            file_io,
            plan_properties,
            io_handle: None,
        }
    }

    /// Attaches an IO runtime handle.
    ///
    /// When set, `execute()` spawns Parquet reads on the given runtime and bridges results
    /// back via an mpsc channel, preventing opendal / network I/O from running on the CPU
    /// runtime.
    pub fn with_io_handle(mut self, handle: Handle) -> Self {
        self.io_handle = Some(handle);
        self
    }

    pub fn tasks_by_bucket(&self) -> &[Vec<FileScanTask>] {
        &self.tasks_by_bucket
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }
}

impl ExecutionPlan for IcebergBucketScan {
    fn name(&self) -> &str {
        "IcebergBucketScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let tasks = self
            .tasks_by_bucket
            .get(partition)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "IcebergBucketScan: partition index {partition} out of bounds \
                     (bucket count: {})",
                    self.tasks_by_bucket.len()
                ))
            })?
            .clone();

        let file_io = self.file_io.clone();
        let schema = self.schema();

        if tasks.is_empty() {
            let empty = futures::stream::empty::<DFResult<RecordBatch>>();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, empty)));
        }

        match &self.io_handle {
            None => {
                let fut = async move {
                    let task_stream =
                        futures::stream::iter(tasks.into_iter().map(Ok::<_, iceberg::Error>));
                    let record_batch_stream = ArrowReaderBuilder::new(file_io)
                        .build()
                        .read(Box::pin(task_stream))
                        .map_err(to_datafusion_error)?
                        .map_err(to_datafusion_error);
                    Ok::<_, datafusion::error::DataFusionError>(record_batch_stream)
                };
                let stream = futures::stream::once(fut).try_flatten();
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
            Some(io_handle) => {
                let (tx, rx) = mpsc::channel::<DFResult<RecordBatch>>(CHANNEL_BUFFER_SIZE);

                io_handle.spawn(async move {
                    let task_stream =
                        futures::stream::iter(tasks.into_iter().map(Ok::<_, iceberg::Error>));
                    match ArrowReaderBuilder::new(file_io)
                        .build()
                        .read(Box::pin(task_stream))
                        .map_err(to_datafusion_error)
                    {
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                        }
                        Ok(stream) => {
                            let mut stream = stream.map_err(to_datafusion_error);
                            while let Some(batch) = stream.next().await {
                                if tx.send(batch).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });

                let stream = BucketChannelStream { receiver: rx };
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
        }
    }
}

impl DisplayAs for IcebergBucketScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let projection = self
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>()
            .join(",");
        let predicate = self
            .tasks_by_bucket
            .iter()
            .flatten()
            .next()
            .and_then(|t| t.predicate())
            .map_or(String::new(), |p| format!("{p}"));
        let total_files: usize = self.tasks_by_bucket.iter().map(|g| g.len()).sum();
        let bucket_count = self.tasks_by_bucket.len();
        let io_tag = if self.io_handle.is_some() {
            " [io-runtime]"
        } else {
            ""
        };
        write!(
            f,
            "{}{io_tag} projection:[{projection}] predicate:[{predicate}] \
             buckets:[{bucket_count}] total_files:[{total_files}]",
            self.name()
        )
    }
}

struct BucketChannelStream {
    receiver: mpsc::Receiver<DFResult<RecordBatch>>,
}

impl Stream for BucketChannelStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}
