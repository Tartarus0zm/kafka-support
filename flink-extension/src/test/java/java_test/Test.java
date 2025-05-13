package java_test;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.planner.plan.utils.CommonFlazeTools;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import com.miner.main.Events;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.nio.charset.StandardCharsets;

import static java_test.JMH_GlueOperator.deleteDirectory;
import static java_test.JMH_SortOperator.generateRandomString;

public class Test {
    public static void main(String[] args) throws Exception {
        testKafka();
        // testKafka2();
    }

    public static void testKafka() {
        CommonFlazeTools.glueModel = 0;
        Configuration configuration = new Configuration();
        // configuration.setBoolean("table.optimizer.streaming-glue.enabled", false); // Flaze add streaming-glue.enabled
        // configuration.setBoolean("table.optimizer.batch-glue.enabled", false); // Flaze add streaming-glue.enabled


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(configuration)
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        tableEnv.executeSql(
                " CREATE TABLE test_table1 (" +
                "  name STRING," +
                "  age  INT," +
                "  num  INT" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'test2'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'scan.startup.mode' = 'latest-offset'," +  // latest-offset or earliest-offset
                "  'format' = 'json'," +
                "  'json.ignore-parse-errors' = 'true'," +
                "  'json.timestamp-format.standard' = 'ISO-8601'" +
                ")"
        );

        String query = "SELECT * FROM test_table1";
        TableResult resultSQLTable = tableEnv.executeSql(query);
        resultSQLTable.print();
    }

    public static void testKafka2() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group2");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        String deser = "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArrayDeserializer";
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deser);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deser);

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList("test2"));
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    System.out.println("woc: " + Arrays.toString(record.value()));
                    String key   = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);
                    String value = record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8);

                    System.out.printf("partition=%d offset=%d key=%s value=%s%n",
                            record.partition(), record.offset(), key, value);

                    consumer.commitSync();
                }
            }
        }
    }
}