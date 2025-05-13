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

//! Execution plan for reading Kafka

use std::{any::Any, fmt, fmt::Formatter, ops::Range, pin::Pin, sync::Arc};

use arrow::datatypes::{DataType, Schema, SchemaRef};
use blaze_jni_bridge::{
    conf, conf::BooleanConf, jni_call_static, jni_new_global_ref, jni_new_string,
};
use bytes::Bytes;
use datafusion::{
    datasource::physical_plan::{
        parquet::{page_filter::PagePruningAccessPlanFilter, ParquetOpener},
        FileMeta, FileScanConfig, FileStream, OnError, ParquetFileMetrics,
        ParquetFileReaderFactory,
    },
    error::Result,
    execution::context::TaskContext,
    parquet::{
        arrow::async_reader::{fetch_parquet_metadata, AsyncFileReader},
        errors::ParquetError,
        file::metadata::ParquetMetaData,
    },
    physical_expr::EquivalenceProperties,
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::{
        metrics::{
            BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet, Time,
        },
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Metric, Partitioning,
        PhysicalExpr, PlanProperties, RecordBatchStream, SendableRecordBatchStream, Statistics,
    },
};
use datafusion_ext_commons::{batch_size, hadoop_fs::FsProvider};
use fmt::Debug;
use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use futures::{future::BoxFuture, stream::once, FutureExt, StreamExt, TryStreamExt};
use object_store::ObjectMeta;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use datafusion_ext_commons::kafka_consumer::Ka;
use crate::{
    common::{internal_file_reader::InternalFileReader, output::TaskOutputter},
    scan::BlazeSchemaAdapterFactory,
};
use serde_json::{Number, Value};

/// Execution plan for scanning Kafka
#[derive(Debug, Clone)]
pub struct KafkaExec {
    kafka_resource_id: String,
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    pruning_predicate: Option<Arc<PruningPredicate>>,
    page_pruning_predicate: Option<Arc<PagePruningAccessPlanFilter>>,
    props: OnceCell<PlanProperties>,
}

impl KafkaExec {
    /// Create a new Kafka reader execution plan provided file list and
    /// schema.
    pub fn new(
        base_config: FileScanConfig,
        kafka_resource_id: String,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let predicate_creation_errors =
            MetricBuilder::new(&metrics).global_counter("num_predicate_creation_errors");

        let file_schema = &base_config.file_schema;
        let pruning_predicate = predicate
            .clone()
            .and_then(|predicate_expr| {
                match PruningPredicate::try_new(predicate_expr, file_schema.clone()) {
                    Ok(pruning_predicate) => Some(Arc::new(pruning_predicate)),
                    Err(e) => {
                        log::warn!("Could not create pruning predicate: {e}");
                        predicate_creation_errors.add(1);
                        None
                    }
                }
            })
            .filter(|p| !p.always_true());

        let page_pruning_predicate = predicate
            .as_ref()
            .map(|p| Arc::new(PagePruningAccessPlanFilter::new(p, file_schema.clone())));

        let (projected_schema, projected_statistics, _projected_output_ordering) =
            base_config.project();

        Self {
            kafka_resource_id,
            base_config,
            projected_schema,
            projected_statistics,
            metrics,
            predicate,
            pruning_predicate,
            page_pruning_predicate,
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for KafkaExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        let limit = self.base_config.limit;

        write!(
            f,
            "KafkaExec: limit={:?}, predicate={}",
            limit,
            self.pruning_predicate
                .as_ref()
                .map(|pre| format!("{}", pre.predicate_expr()))
                .unwrap_or(format!("<empty>")),
        )
    }
}

impl ExecutionPlan for KafkaExec {
    fn name(&self) -> &str {
        "KafkaExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(1),  // 现在设置为1
                ExecutionMode::Bounded,
            )
        })
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition_index: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition_index);
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        let io_time = Time::default();
        let io_time_metric = Arc::new(Metric::new(
            MetricValue::Time {
                name: "io_time".into(),
                time: io_time.clone(),
            },
            Some(partition_index),
        ));
        self.metrics.register(io_time_metric);


        // get fs object from jni bridge resource
        let resource_id = jni_new_string!(&self.kafka_resource_id)?;
        let ka = jni_call_static!(JniBridge.getKafkaConsumerResource(resource_id.as_obj()) -> JObject)?;
        let ka = Ka::new(jni_new_global_ref!(ka.as_obj())?, &io_time);
        // let ka_provider = Arc::new(KaProvier::new(jni_new_global_ref!(ka.as_obj())?, &io_time));
        // let ka = ka_provider.provide()?;

        let timed_stream = Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            once(execute_kafka_scan(context, self.base_config.file_schema.clone(), ka, baseline_metrics)).try_flatten(),
        ));
        Ok(timed_stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }
}

async fn execute_kafka_scan(
    context: Arc<TaskContext>,
    schema: Arc<Schema>,
    ka: Ka,
    baseline_metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    let value = ka.open()?;
    context.output_with_sender("KafkaScan", schema.clone(), move |sender| async move {
        sender.exclude_time(baseline_metrics.elapsed_compute());
        let _timer = baseline_metrics.elapsed_compute().timer();

        let batch: RecordBatch = conv_record_batch(value, schema);
        sender.send(Ok(batch)).await;
        Ok(())
    })
}

fn conv_record_batch(val: Value, schema: Arc<Schema>) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::new();
    for field in schema.fields() {
        let field_name = field.name();
        let data_type = &field.data_type();

        match data_type {
            DataType::Int32 => {
                let int_values = val.as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|v| v.get(field_name)
                        .unwrap_or(&Value::Number(Number::from(0)))
                        .as_i64()
                        .unwrap_or(0) as i32)
                    .collect::<Vec<i32>>();
                columns.push(Arc::new(Int32Array::from(int_values)) as ArrayRef);
            }
            DataType::Utf8 => {
                let str_values = val.as_array()
                    .unwrap_or(&vec![])
                    .iter()
                    .map(|v| v.get(field_name)
                        .unwrap_or(&Value::String("".to_string()))
                        .as_str()
                        .unwrap_or("")
                        .to_string())
                    .collect::<Vec<String>>();
                columns.push(Arc::new(StringArray::from(str_values)) as ArrayRef);
            }
            _ => unimplemented!(),
        }
    }
    let record_batch = RecordBatch::try_new(schema, columns).unwrap();
    record_batch
}
