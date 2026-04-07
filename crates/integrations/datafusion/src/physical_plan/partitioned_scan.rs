use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
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

/// Channel buffer size for streaming record batches between runtimes.
///
/// Mirrors the constant used in `dd-datafusion`'s `IOExec`.
const CHANNEL_BUFFER_SIZE: usize = 32;

/// A DataFusion [`ExecutionPlan`] that reads one [`FileScanTask`] per partition.
///
/// Display information (projection, predicate) is derived at runtime from the output schema and
/// the tasks rather than stored as dedicated struct fields. This keeps the node self-contained:
/// all state is already serializable via `FileScanTask`, which simplifies the DataFusion
/// distributed codec, adding dedicated fields would require encoding them separately in the
/// protobuf round-trip.
///
/// When an IO runtime [`Handle`] is provided via [`IcebergPartitionedScan::with_io_handle`],
/// `execute()` spawns Parquet reads on that runtime and bridges results back via a channel.
/// This ensures that opendal / network I/O does not compete with CPU-bound compute threads
/// when runtime segregation is enabled.
#[derive(Debug, Clone)]
pub struct IcebergPartitionedScan {
    tasks: Vec<FileScanTask>,
    file_io: FileIO,
    plan_properties: PlanProperties,
    io_handle: Option<Handle>,
}

impl IcebergPartitionedScan {
    pub fn new(tasks: Vec<FileScanTask>, file_io: FileIO, schema: ArrowSchemaRef) -> Self {
        let n_partitions = tasks.len();
        let plan_properties = Self::compute_properties(schema, n_partitions);
        Self {
            tasks,
            file_io,
            plan_properties,
            io_handle: None,
        }
    }

    /// Attaches an IO runtime handle to this scan.
    ///
    /// When set, `execute()` spawns Parquet reads on the given runtime and bridges results
    /// back to the caller via an mpsc channel, ensuring that opendal / network I/O runs on
    /// the IO runtime rather than the CPU runtime.
    pub fn with_io_handle(mut self, handle: Handle) -> Self {
        self.io_handle = Some(handle);
        self
    }

    pub fn tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    fn compute_properties(schema: ArrowSchemaRef, n_partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(n_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for IcebergPartitionedScan {
    fn name(&self) -> &str {
        "IcebergPartitionedScan"
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
        let task = self.tasks.get(partition).cloned().ok_or_else(|| {
            datafusion::error::DataFusionError::Internal(format!(
                "{}: partition index {partition} is out of bounds \
                 (total tasks: {})",
                self.name(),
                self.tasks.len()
            ))
        })?;

        let file_io = self.file_io.clone();
        let schema = self.schema();

        match &self.io_handle {
            None => {
                let fut = async move {
                    let task_stream = futures::stream::once(futures::future::ready(Ok(task)));
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

                // The JoinHandle is intentionally dropped (task detached).
                // Errors from the arrow reader are forwarded via the channel.
                // If the task panics, `tx` is dropped, the channel closes, and
                // the consumer sees end-of-stream. This matches the behaviour of
                // `dd-datafusion`'s `IOExec`.
                io_handle.spawn(async move {
                    let task_stream = futures::stream::once(futures::future::ready(Ok(task)));
                    match ArrowReaderBuilder::new(file_io)
                        .build()
                        .read(Box::pin(task_stream))
                        .map_err(to_datafusion_error)
                    {
                        Err(e) => {
                            // If the receiver is dropped (query cancelled), there is nothing to
                            // propagate the error to.
                            // Mirrors `dd-datafusion`'s `IOExec`.
                            let _ = tx.send(Err(e)).await;
                        }
                        Ok(stream) => {
                            let mut stream = stream.map_err(to_datafusion_error);
                            while let Some(batch) = stream.next().await {
                                // If the receiver is dropped, stop processing.
                                if tx.send(batch).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });

                let stream = ChannelRecordBatchStream { receiver: rx };
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
        }
    }
}

impl DisplayAs for IcebergPartitionedScan {
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
        // All tasks share the same predicate (they come from a single scan plan build),
        // so reading it from the first task is sufficient.
        let predicate = self
            .tasks
            .first()
            .and_then(|t| t.predicate())
            .map_or(String::new(), |p| format!("{p}"));
        let file_count = self.tasks.len();
        let io_tag = if self.io_handle.is_some() {
            " [io-runtime]"
        } else {
            ""
        };
        write!(
            f,
            "{}{io_tag} projection:[{projection}] predicate:[{predicate}] file_count:[{file_count}]",
            self.name()
        )?;
        if self.tasks.len() <= 5 {
            let files = self
                .tasks
                .iter()
                .map(|t| t.data_file_path())
                .collect::<Vec<_>>()
                .join(", ");
            write!(f, " files:[{files}]")?;
        }
        Ok(())
    }
}

/// Bridges an mpsc channel into a [`Stream`] of [`RecordBatch`] results.
///
/// Used by [`IcebergPartitionedScan::execute`] when an IO runtime handle is configured:
/// the Parquet read runs on the IO runtime and pushes batches through this channel to the
/// CPU runtime that is polling the stream.
struct ChannelRecordBatchStream {
    receiver: mpsc::Receiver<DFResult<RecordBatch>>,
}

impl Stream for ChannelRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_recv(cx)
    }
}
