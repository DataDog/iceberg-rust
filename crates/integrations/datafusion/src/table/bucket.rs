use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::expressions::Column;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Literal, PartitionSpec, PrimitiveLiteral, Schema, Transform};
use iceberg::{Catalog, NamespaceIdent, Result, TableIdent};
use tokio::runtime::Handle;

use crate::error::to_datafusion_error;
use crate::physical_plan::bucket_scan::IcebergBucketScan;
use crate::physical_plan::expr_to_predicate::convert_filters_to_predicate;

/// A DataFusion [`TableProvider`] that exposes Iceberg bucket partitions as native DataFusion
/// partitions, enabling DataFusion to eliminate shuffle (`Exchange`) nodes for aggregations and
/// joins on the bucketed column.
///
/// Unlike [`IcebergPartitionedTableProvider`](crate::table::partitioned::IcebergPartitionedTableProvider),
/// which maps one DataFusion partition per data file, this provider maps one DataFusion partition
/// per Iceberg bucket. All files sharing the same bucket value are read within a single partition,
/// and the plan reports `Partitioning::Hash([source_col], N)`.
///
/// Returns an error from `scan()` if the table has no bucket partition field or if the bucketed
/// source column was projected out.
#[derive(Debug, Clone)]
pub struct IcebergBucketTableProvider {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema: ArrowSchemaRef,
    io_handle: Option<Handle>,
}

impl IcebergBucketTableProvider {
    pub async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
    ) -> Result<Self> {
        let table_ident = TableIdent::new(namespace, name.into());
        let table = catalog.load_table(&table_ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        Ok(Self {
            catalog,
            table_ident,
            schema,
            io_handle: None,
        })
    }

    /// Attaches an IO runtime handle.
    ///
    /// When set, the network I/O performed during `scan()` (`load_table`, `plan_files`) is
    /// spawned on this handle, and each resulting [`IcebergBucketScan`] will have the handle
    /// injected for Parquet reads.
    pub fn with_io_handle(mut self, handle: Handle) -> Self {
        self.io_handle = Some(handle);
        self
    }

    async fn fetch_tasks(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        col_names: Option<Vec<String>>,
        predicate: Option<iceberg::expr::Predicate>,
    ) -> Result<(FileIO, Vec<FileScanTask>, Arc<PartitionSpec>, Arc<Schema>)> {
        let table = catalog.load_table(&table_ident).await?;
        let partition_spec = table.metadata().default_partition_spec().clone();
        let iceberg_schema = table.metadata().current_schema().clone();

        let mut builder = table.scan();
        builder = match col_names {
            Some(names) => builder.select(names),
            None => builder.select_all(),
        };
        if let Some(pred) = predicate {
            builder = builder.with_filter(pred);
        }

        let tasks = builder
            .build()?
            .plan_files()
            .await?
            .try_collect::<Vec<_>>()
            .await?;

        Ok((table.file_io().clone(), tasks, partition_spec, iceberg_schema))
    }
}

#[async_trait]
impl TableProvider for IcebergBucketTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let col_names = projection.map(|indices| {
            indices
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });

        let predicate = convert_filters_to_predicate(filters);

        let catalog = Arc::clone(&self.catalog);
        let table_ident = self.table_ident.clone();
        let (file_io, tasks, partition_spec, iceberg_schema) = match &self.io_handle {
            Some(h) => h
                .spawn(Self::fetch_tasks(
                    catalog,
                    table_ident,
                    col_names,
                    predicate,
                ))
                .await
                .map_err(|e| {
                    DataFusionError::Internal(format!(
                        "IcebergBucketScan: IO task panicked: {e}"
                    ))
                })?
                .map_err(to_datafusion_error)?,
            None => Self::fetch_tasks(catalog, table_ident, col_names, predicate)
                .await
                .map_err(to_datafusion_error)?,
        };

        let output_schema: ArrowSchemaRef = match projection {
            None => self.schema.clone(),
            Some(indices) => Arc::new(self.schema.project(indices).map_err(|e| {
                DataFusionError::Internal(format!("schema projection failed: {e}"))
            })?),
        };

        let info = detect_bucket_field(&partition_spec, &iceberg_schema, &output_schema)
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "IcebergBucketTableProvider: table has no bucket partition field, \
                     or the bucket source column was projected out"
                        .to_string(),
                )
            })?;

        let source_col_expr =
            Arc::new(Column::new(&info.source_col_name, info.source_col_idx))
                as Arc<dyn PhysicalExpr>;

        let tasks_by_bucket = group_tasks_by_bucket(tasks, &info);

        let mut scan = IcebergBucketScan::new(
            tasks_by_bucket,
            file_io,
            output_schema,
            source_col_expr,
            info.bucket_count as usize,
        );
        if let Some(h) = &self.io_handle {
            scan = scan.with_io_handle(h.clone());
        }

        Ok(Arc::new(scan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct BucketFieldInfo {
    /// Index of the bucket field within `PartitionSpec.fields()`.
    field_idx: usize,
    /// Name of the source column in the Arrow output schema.
    source_col_name: String,
    /// Index of the source column in the Arrow output schema.
    source_col_idx: usize,
    /// Number of buckets (N in `bucket(N, col)`).
    bucket_count: u32,
}

/// Finds the first `Transform::Bucket(N)` field in the partition spec and resolves its source
/// column in the output Arrow schema.
///
/// Returns `None` if no bucket field exists or if the source column was projected out.
fn detect_bucket_field(
    partition_spec: &PartitionSpec,
    iceberg_schema: &Schema,
    output_schema: &datafusion::arrow::datatypes::Schema,
) -> Option<BucketFieldInfo> {
    partition_spec
        .fields()
        .iter()
        .enumerate()
        .find_map(|(field_idx, pf)| {
            let Transform::Bucket(n) = pf.transform else {
                return None;
            };
            let source_name = iceberg_schema.field_by_id(pf.source_id)?.name.clone();
            let col_idx = output_schema.index_of(&source_name).ok()?;
            Some(BucketFieldInfo {
                field_idx,
                source_col_name: source_name,
                source_col_idx: col_idx,
                bucket_count: n,
            })
        })
}

/// Groups `FileScanTask`s by their Iceberg bucket value, retaining only non-empty groups.
///
/// Tasks are distributed into `bucket_count` slots by bucket value. Empty slots are then
/// discarded, so the returned `Vec` contains only groups with at least one task. Its length
/// equals the number of distinct populated buckets (≤ `info.bucket_count`).
///
/// This ensures `IcebergBucketScan` creates DataFusion partitions only for buckets that
/// actually have data — including after Iceberg predicate pruning, where `plan_files()` may
/// already have reduced the task list to a single bucket.
///
/// Tasks whose partition value is absent or out of range are silently ignored.
fn group_tasks_by_bucket(
    tasks: Vec<FileScanTask>,
    info: &BucketFieldInfo,
) -> Vec<Vec<FileScanTask>> {
    let mut groups: Vec<Vec<FileScanTask>> = vec![vec![]; info.bucket_count as usize];
    for task in tasks {
        if let Some(bucket) = extract_bucket_value(&task, info.field_idx) {
            let b = bucket as usize;
            if b < groups.len() {
                groups[b].push(task);
            }
        }
    }
    // Drop empty groups: only populated buckets become DataFusion partitions.
    // Partitioning::Hash([source_col], K) remains correct: same source_col value →
    // same Iceberg bucket (deterministic) → same surviving partition.
    groups.into_iter().filter(|g| !g.is_empty()).collect()
}

/// Extracts the integer bucket value stored in `FileScanTask.partition` at `field_idx`.
///
/// Bucket values are stored as `Literal::Primitive(PrimitiveLiteral::Int(i32))` in the
/// partition `Struct`, pre-computed when the manifest was written.
fn extract_bucket_value(task: &FileScanTask, field_idx: usize) -> Option<i32> {
    match task.partition.as_ref()?.fields().get(field_idx)?.as_ref()? {
        Literal::Primitive(PrimitiveLiteral::Int(v)) => Some(*v),
        _ => None,
    }
}
