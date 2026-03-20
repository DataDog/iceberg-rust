use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use futures::{Stream, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;

use crate::to_datafusion_error;
#[derive(Debug)]
pub struct IcebergPartitionedScan {
    tasks: Vec<FileScanTask>,
    file_io: FileIO,
    plan_properties: PlanProperties,
    limit: Option<usize>,
}

impl IcebergPartitionedScan {
    pub fn new(
        tasks: Vec<FileScanTask>,
        file_io: FileIO,
        schema: ArrowSchemaRef,
        limit: Option<usize>,
    ) -> Self {
        let n_partitions = tasks.len();
        let plan_properties = Self::compute_properties(schema, n_partitions);
        Self {
            tasks,
            file_io,
            plan_properties,
            limit,
        }
    }

    pub fn scan_tasks(&self) -> &[FileScanTask] {
        &self.tasks
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    pub fn file_io(&self) -> &FileIO {
        &self.file_io
    }

    fn compute_properties(schema: ArrowSchemaRef, n_partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(n_partitions.max(1)),
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
                "IcebergPartitionedScan: partition index {partition} is out of bounds \
                 (total tasks: {})",
                self.tasks.len()
            ))
        })?;

        let file_io = self.file_io.clone();

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

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            limited_stream,
        )))
    }
}

impl DisplayAs for IcebergPartitionedScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "IcebergPartitionedScan partitions:[{}] limit:[{}]",
            self.tasks.len(),
            self.limit.map_or(String::new(), |l| format!("{l}")),
        )
    }
}
