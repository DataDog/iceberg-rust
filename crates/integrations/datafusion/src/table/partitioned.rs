use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use futures::TryStreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};
use tokio::runtime::Handle;

use crate::error::to_datafusion_error;
use crate::physical_plan::expr_to_predicate::convert_filters_to_predicate;
use crate::physical_plan::partitioned_scan::IcebergPartitionedScan;

#[derive(Debug, Clone)]
pub struct IcebergPartitionedTableProvider {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema: ArrowSchemaRef,
    io_handle: Option<Handle>,
}

impl IcebergPartitionedTableProvider {
    pub async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        name: impl Into<String>,
    ) -> Result<Self> {
        let table_ident = TableIdent::new(namespace, name.into());
        // First load: used only to snapshot the Arrow schema for DataFusion planning.
        // A second load_table is issued at scan time to guarantee the freshest snapshot.
        let table = catalog.load_table(&table_ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        Ok(Self {
            catalog,
            table_ident,
            schema,
            io_handle: None,
        })
    }

    /// Attaches an IO runtime handle to this provider.
    ///
    /// When set, every [`IcebergPartitionedScan`] produced by [`scan()`](Self::scan) will have
    /// the handle injected via [`IcebergPartitionedScan::with_io_handle`], ensuring that Parquet
    /// reads via opendal run on the IO runtime rather than the CPU runtime.
    ///
    /// Additionally, the network I/O performed during `scan()` itself (`load_table` and
    /// `plan_files`) is spawned on this handle, preventing DNS / HTTP calls from running
    /// on the CPU runtime during DataFusion's physical planning phase.
    pub fn with_io_handle(mut self, handle: Handle) -> Self {
        self.io_handle = Some(handle);
        self
    }

    /// Fetches the file scan tasks for this table.
    ///
    /// Performs all network I/O: `load_table` (REST catalog) and `plan_files` (manifest reads).
    /// Extracted as a static method so it can be spawned on the IO runtime via
    /// `io_handle.spawn()` when runtime segregation is enabled.
    async fn fetch_tasks(
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        col_names: Option<Vec<String>>,
        predicate: Option<iceberg::expr::Predicate>,
    ) -> Result<(FileIO, Vec<FileScanTask>)> {
        let table = catalog.load_table(&table_ident).await?;

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

        Ok((table.file_io().clone(), tasks))
    }
}

#[async_trait]
impl TableProvider for IcebergPartitionedTableProvider {
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
        // Projection indices are resolved against self.schema (captured at try_new time),
        // same as IcebergTableProvider / IcebergTableScan.
        let col_names = projection.map(|indices| {
            indices
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });

        let predicate = convert_filters_to_predicate(filters);

        // `load_table` (REST catalog) and `plan_files` (manifest reads) both trigger network I/O.
        // When runtime segregation is enabled, DataFusion calls `scan()` on the CPU runtime during
        // physical planning. Spawning on the IO handle prevents DNS / HTTP calls from running on
        // the CPU runtime and causing a panic.
        let catalog = Arc::clone(&self.catalog);
        let table_ident = self.table_ident.clone();
        let (file_io, tasks) = match &self.io_handle {
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
                        "IcebergPartitionedScan: IO task panicked: {e}"
                    ))
                })?
                .map_err(to_datafusion_error)?,
            None => Self::fetch_tasks(catalog, table_ident, col_names, predicate)
                .await
                .map_err(to_datafusion_error)?,
        };

        let output_schema = match projection {
            None => self.schema.clone(),
            Some(indices) => Arc::new(self.schema.project(indices).map_err(|e| {
                DataFusionError::Internal(format!("schema projection failed: {e}"))
            })?),
        };

        let mut scan = IcebergPartitionedScan::new(tasks, file_io, output_schema);
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

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(to_datafusion_error(Error::new(
            ErrorKind::FeatureUnsupported,
            "IcebergPartitionedTableProvider does not support writes; \
             use IcebergTableProvider instead",
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::prelude::SessionContext;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema, Type,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

    async fn make_catalog_and_table() -> (Arc<dyn Catalog>, NamespaceIdent, String, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse = temp_dir.path().to_str().unwrap().to_string();

        let catalog = Arc::new(
            MemoryCatalogBuilder::default()
                .load(
                    "memory",
                    HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
                )
                .await
                .unwrap(),
        );

        let namespace = NamespaceIdent::new("ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .unwrap();

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("t".to_string())
                    .location(format!("{warehouse}/t"))
                    .schema(schema)
                    .properties(HashMap::new())
                    .build(),
            )
            .await
            .unwrap();

        (catalog, namespace, "t".to_string(), temp_dir)
    }

    /// Registers `n` synthetic data files in the table metadata via the iceberg
    /// transaction API. No actual parquet files are written, only the metadata
    /// entries that `plan_files()` reads are created.
    async fn append_fake_data_files(
        catalog: &Arc<dyn Catalog>,
        namespace: &NamespaceIdent,
        table_name: &str,
        n: usize,
    ) {
        let table = catalog
            .load_table(&TableIdent::new(namespace.clone(), table_name.to_string()))
            .await
            .unwrap();

        let data_files = (0..n)
            .map(|i| {
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(format!(
                        "{}/data/fake_{i}.parquet",
                        table.metadata().location()
                    ))
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(128)
                    .record_count(1)
                    .partition_spec_id(table.metadata().default_partition_spec_id())
                    .build()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        action
            .apply(tx)
            .unwrap()
            .commit(catalog.as_ref())
            .await
            .unwrap();
    }

    /// An empty table must produce a zero-partition scan so DataFusion never calls
    /// execute(0), which would otherwise return an out-of-bounds error.
    #[tokio::test]
    async fn test_empty_table_zero_partitions() {
        let (catalog, namespace, table_name, _temp_dir) = make_catalog_and_table().await;
        // no files appended
        let provider = IcebergPartitionedTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&SessionContext::new().state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan
            .as_any()
            .downcast_ref::<IcebergPartitionedScan>()
            .unwrap();

        assert_eq!(scan.tasks().len(), 0);
        assert_eq!(scan.properties().partitioning.partition_count(), 0);
    }

    /// Each data file in the table must become exactly one DataFusion partition
    /// in IcebergPartitionedScan, enabling parallel file reads.
    #[tokio::test]
    async fn test_one_partition_per_file() {
        let (catalog, namespace, table_name, _temp_dir) = make_catalog_and_table().await;
        append_fake_data_files(&catalog, &namespace, &table_name, 3).await;

        let provider = IcebergPartitionedTableProvider::try_new(catalog, namespace, table_name)
            .await
            .unwrap();
        let plan = provider
            .scan(&SessionContext::new().state(), None, &[], None)
            .await
            .unwrap();
        let scan = plan
            .as_any()
            .downcast_ref::<IcebergPartitionedScan>()
            .unwrap();

        assert_eq!(scan.tasks().len(), 3);
        assert_eq!(scan.properties().partitioning.partition_count(), 3);
    }
}
