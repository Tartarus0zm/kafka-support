package org.apache.flink.table.planner.plan.nodes.exec.glue.jni;

import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerManager {
    private KafkaConsumer<byte[], byte[]> consumer;

    public void init(Properties properties, String topicName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.get("client.id.prefix"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.get("enable.auto.commit"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.get("auto.offset.reset"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));
    }

    public ByteBuffer getMessage() {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
        StringBuilder jsonArray = new StringBuilder("[");
        int geshu = 0;
        for (ConsumerRecord<byte[], byte[]> record : records) {
            byte[] value = record.value();
            if (value != null) {
                String message = new String(value, StandardCharsets.UTF_8);
                jsonArray.append(message).append(",");
                geshu++;
            }
        }
        if (jsonArray.length() > 1) {
            jsonArray.setLength(jsonArray.length() - 1);
        }
        jsonArray.append("]");
        byte[] byteArray = jsonArray.toString().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocateDirect(byteArray.length);
        buffer.put(byteArray);
        buffer.flip();
        return buffer;
    }

    public void commit() {
        consumer.commitSync();
    }
}
