package org.apache.flink.table.planner.plan.nodes.exec.glue.jni;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.operators.TaskContext;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unused")
public class JniBridge {
    // kafka connector
    public static Object getKafkaConsumerResource(String key) {
        return kafkaConsumerResourcesMap.get(key);
    }
}
