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
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableIdent};

use crate::error::to_datafusion_error;
use crate::physical_plan::expr_to_predicate::convert_filters_to_predicate;
use crate::physical_plan::partitioned_scan::IcebergPartitionedScan;

#[derive(Debug, Clone)]
pub struct IcebergPartitionedTableProvider {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema: ArrowSchemaRef,
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
        })
    }

    async fn scan_without_session(
        &self,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> DFResult<IcebergPartitionedScan> {
        // Second load: fetch the latest snapshot so scans always reflect current table state.
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

        // TODO: schema staleness risk, projection indices are resolved against self.schema,
        // which was captured at try_new time. If the table schema evolved between try_new and
        // this scan, the column names may be incorrect. This logic is inherited from IcebergTableProvider.
        let col_names = projection.as_ref().map(|indices| {
            indices
                .iter()
                .map(|&i| self.schema.field(i).name().clone())
                .collect::<Vec<_>>()
        });

        let predicate = convert_filters_to_predicate(&filters);

        let mut builder = table.scan();
        builder = match col_names {
            Some(names) => builder.select(names),
            None => builder.select_all(),
        };
        if let Some(pred) = predicate {
            builder = builder.with_filter(pred);
        }

        let tasks = builder
            .build()
            .map_err(to_datafusion_error)?
            .plan_files()
            .await
            .map_err(to_datafusion_error)?
            .try_collect::<Vec<_>>()
            .await
            .map_err(to_datafusion_error)?;

        let output_schema = match &projection {
            None => self.schema.clone(),
            Some(indices) => Arc::new(self.schema.project(indices).map_err(|e| {
                DataFusionError::Internal(format!("schema projection failed: {e}"))
            })?),
        };

        Ok(IcebergPartitionedScan::new(
            tasks,
            table.file_io().clone(),
            output_schema,
        ))
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
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // limit is a hint only; DataFusion inserts a GlobalLimitExec above us anyway
        let _ = limit;
        let scan = self
            .scan_without_session(projection.cloned(), filters.to_vec())
            .await?;
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
        let scan = provider.scan_without_session(None, vec![]).await.unwrap();

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
        let scan = provider.scan_without_session(None, vec![]).await.unwrap();

        assert_eq!(scan.tasks().len(), 3);
        assert_eq!(scan.properties().partitioning.partition_count(), 3);
    }
}
