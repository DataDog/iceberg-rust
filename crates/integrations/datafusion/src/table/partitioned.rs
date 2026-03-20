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
use iceberg::scan::FileScanTask;
use iceberg::{Catalog, NamespaceIdent, Result, TableIdent};

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
        let table = catalog.load_table(&table_ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(table.metadata().current_schema())?);
        Ok(Self {
            catalog,
            table_ident,
            schema,
        })
    }

    pub async fn scan_without_session(
        &self,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> DFResult<IcebergPartitionedScan> {
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(to_datafusion_error)?;

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

        let tasks: Vec<FileScanTask> = builder
            .build()
            .map_err(to_datafusion_error)?
            .plan_files()
            .await
            .map_err(to_datafusion_error)?
            .try_collect()
            .await
            .map_err(to_datafusion_error)?;

        let output_schema = match &projection {
            None => self.schema.clone(),
            Some(indices) => Arc::new(self.schema.project(indices).map_err(|e| {
                DataFusionError::Internal(format!("schema projection failed: {e}"))
            })?),
        };

        let file_io = table.file_io().clone();

        Ok(IcebergPartitionedScan::new(
            tasks,
            file_io,
            output_schema,
            limit,
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
        let scan = self
            .scan_without_session(projection.cloned(), filters.to_vec(), limit)
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
        Err(DataFusionError::NotImplemented(
            "IcebergPartitionedTableProvider does not support writes; \
             use IcebergTableProvider instead"
                .to_string(),
        ))
    }
}
