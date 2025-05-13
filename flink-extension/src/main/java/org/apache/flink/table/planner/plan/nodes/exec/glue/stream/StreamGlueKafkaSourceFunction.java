package org.apache.flink.table.planner.plan.nodes.exec.glue.stream;


import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.flaze.plan.protobuf.PhysicalPlanNode;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.glue.jni.JniBridge;
import org.apache.flink.table.planner.plan.nodes.exec.glue.jni.KafkaConsumerManager;
import org.apache.flink.table.planner.plan.utils.NativeHelper$;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamGlueKafkaSourceFunction extends RichParallelSourceFunction<RowData> {
    private static final long serialVersionUID = -453977389132295338L;
    private volatile boolean isRunning = true;
    private PhysicalPlanNode gluePhysicalPlan;
    private transient AtomicBoolean atomicBoolean;
    private transient JniBridge jniBridge;
    // kafka
    private final String resourceId;
    private final Properties properties;
    private final String topicName;

    public StreamGlueKafkaSourceFunction(PhysicalPlanNode gluePhysicalPlan, String resourceId, Properties properties, String topicName) {
        this.gluePhysicalPlan = gluePhysicalPlan;
        this.resourceId = resourceId;
        this.properties = properties;
        this.topicName = topicName;
    }

    @Override
    public void open(Configuration parameters) {
        KafkaConsumerManager kafkaConsumerManager = new KafkaConsumerManager();
        kafkaConsumerManager.init(properties, topicName);
        NativeHelper$.MODULE$.putKafkaConsumerResource(resourceId, kafkaConsumerManager);
        jniBridge = JniBridge.getInstance();
    }

    @Override
    public void run(SourceContext<RowData> ctx) {
        if (!isRunning) {
            return;
        }
        System.out.println("---------------------执行native glue kafkaSourceFunc operator---------------------");
        while (true) {
            long[] ffiRes = jniBridge.callNative(gluePhysicalPlan.toByteArray());
            long ffiSchemaPtr = ffiRes[0];
            long ffiArrayPtr = ffiRes[1];
            try {
                CDataDictionaryProvider dictionaryProvider = new CDataDictionaryProvider();
                Schema arrowSchema = StreamGlueOperatorTool.importSchema(ffiSchemaPtr, dictionaryProvider);
                GenericRowData[] genericRowDataArrayBuffer = StreamGlueOperatorTool.importBatch(ffiArrayPtr, dictionaryProvider, arrowSchema);
                synchronized (ctx.getCheckpointLock()) {
                    for (GenericRowData genericRowData : genericRowDataArrayBuffer) {
                        ctx.collect(genericRowData);
                    }
                }
            } finally {
                jniBridge.freeArrowSchema(ffiSchemaPtr);
                jniBridge.freeArrowArray(ffiArrayPtr);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}




