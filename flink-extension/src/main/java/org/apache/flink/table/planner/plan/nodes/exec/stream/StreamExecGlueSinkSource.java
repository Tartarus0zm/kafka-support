package org.apache.flink.table.planner.plan.nodes.exec.stream;


import org.apache.calcite.rel.RelNode;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connectors.hive.HiveSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.flaze.plan.protobuf.PhysicalPlanNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecSink;
import org.apache.flink.table.planner.plan.nodes.exec.glue.stream.StreamGlueKafkaSourceFunction;
import org.apache.flink.table.planner.plan.nodes.exec.glue.stream.StreamGlueSourceFunction;
import org.apache.flink.table.planner.plan.nodes.exec.glue.stream.StreamGlueSourceOperator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.utils.CommonFlazeTools;
import org.apache.flink.table.planner.plan.utils.NativeHelper$;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Hive;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;


@ExecNodeMetadata(
        name = "stream-exec-glue-sink-source",
        version = 1,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecGlueSinkSource extends CommonExecSink implements StreamExecNode<Object> {
    private final DynamicTableSourceSpec tableSourceSpec;
    private final RelNode rootNode;

    @JsonCreator
    public StreamExecGlueSinkSource(
            ReadableConfig tableConfig,
            DynamicTableSinkSpec tableSinkSpec,
            InputProperty inputProperty,
            LogicalType outputType,
            String description,
            RelNode rootNode,
            DynamicTableSourceSpec tableSourceSpec
    ) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecGlueSinkSource.class),
                ExecNodeContext.newPersistedConfig(StreamExecGlueSinkSource.class, tableConfig),
                tableSinkSpec,
                ChangelogMode.insertOnly(),
                true, // isBounded
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.rootNode = rootNode;
        this.tableSourceSpec = tableSourceSpec;
    }

    private ProviderContext createProviderContext(ExecNodeConfig config) {
        return name -> {
            if (this instanceof StreamExecNode && config.shouldSetUid()) {
                return Optional.of(createTransformationUid(name, config));
            }
            return Optional.empty();
        };
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<Object> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        String name = "Collect table sink";
        InternalTypeInfo<RowData> outputType = InternalTypeInfo.of((RowType) getOutputType());


        final ScanTableSource tableSource =
                tableSourceSpec.getScanTableSource(
                        planner.getFlinkContext(), ShortcutUtils.unwrapTypeFactory(planner));

        ScanTableSource.ScanRuntimeProvider scanRuntimeProvider = tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        boolean isFileSource = true;
        if (scanRuntimeProvider instanceof DataStreamScanProvider)
            isFileSource = false;

        SourceFunction glueSourceFunction;
        String resourceId = "StreamExecGlueSinkSource:" + UUID.randomUUID();
        try {
            if (isFileSource) {
            } else {
                DataStreamScanProvider dataStreamScanProvider = (DataStreamScanProvider) tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
                final StreamExecutionEnvironment env = planner.getExecEnv();
                SourceTransformation sourceTransformation = ((SourceTransformation) dataStreamScanProvider.produceDataStream(createProviderContext(config), env).getTransformation());
                Source sourceFa = sourceTransformation.getSource();
                if (sourceFa instanceof HiveSource) {
                } else if (sourceFa instanceof KafkaSource) {
                    KafkaSource source = (KafkaSource) sourceFa;
                    String topicName = CommonFlazeTools.getTopicNameFromKafkaSource((KafkaDynamicSource)tableSource);
                    Properties properties = CommonFlazeTools.getPropertiesFromKafkaSource(source);

                    System.out.println("计划创建Stream kafka模式下的SinkSource算子");
                    PhysicalPlanNode gluePhysicalPlan = CommonFlazeTools.generatePhysicalPlanUntilKafka(rootNode, resourceId);
                    glueSourceFunction = new StreamGlueKafkaSourceFunction(gluePhysicalPlan, resourceId, properties, topicName);
                } else {
                    throw new RuntimeException("Failed to access sourceFa: " + sourceFa.getClass().getName());
                }
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to access enumeratorFactory via reflection", e);
        }


        StreamGlueSourceOperator operator = new StreamGlueSourceOperator(glueSourceFunction);
        LegacySourceTransformation<RowData> glueTransformation = new LegacySourceTransformation<>(
                name,
                operator,
                outputType,
                planner.getExecEnv().getParallelism(),
                Boundedness.BOUNDED,
                true
        );


        final DynamicTableSink tableSink = tableSinkSpec.getTableSink(planner.getFlinkContext());
        return createSinkTransformation(
                planner.getExecEnv(),
                config,
                planner.getFlinkContext().getClassLoader(),
                glueTransformation,
                tableSink,
                -1,
                false,
                null);
    }
}

