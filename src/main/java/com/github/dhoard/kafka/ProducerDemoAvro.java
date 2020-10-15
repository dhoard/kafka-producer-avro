package com.github.dhoard.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.Random;;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoAvro {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoAvro.class);

    private static final String BOOTSTRAP_SERVERS = "cp-6-0-x.address.cx:9092";

    private static final String SCHEMA_REGISTRY_URL = "http://cp-6-0-x.address.cx:8081";

    private static final String TOPIC = "test";

    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws Exception {
        new ProducerDemoAvro().run(args);
    }

    public void run(String[] args) throws Exception {
        KafkaProducer<String, GenericRecord> kafkaProducer = null;

        try {
            Properties properties = new Properties();

            properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class.getName());

            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

            properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                MonitoringProducerInterceptor.class.getName());

            /*
            // Build the sasl.jaas.config String
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"");
            stringBuilder.append("test"); // If username has double quotes, they will need to be escaped
            stringBuilder.append("\" password=\"");
            stringBuilder.append("test123"); // If password has double quotes, they will need to be escaped
            stringBuilder.append("\";");

            // Set the security properties
            properties.setProperty(
                "sasl.jaas.config", stringBuilder.toString());

            properties.setProperty(
                "security.protocol", "SASL_PLAINTEXT"); // Or correct protocol

            properties.setProperty("sasl.mechanism", "PLAIN");
            */

            properties.put("schema.registry.url", SCHEMA_REGISTRY_URL);
            properties.put("auto.register.schemas", false);

            kafkaProducer = new KafkaProducer<String, GenericRecord>(properties);

            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (Throwable t) {
                    // DO NOTHING
                }

                String schemaString = "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"monitoring\",\"fields\":[{\"name\":\"currentValue\",\"type\":\"string\"}]}";

                Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(schemaString);

                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("currentValue", String.valueOf(randomLong(1, 100)));

                String key = String.valueOf(1);

                ProducerRecord<String, GenericRecord> producerRecord =
                    new ProducerRecord<>(TOPIC, String.valueOf(1), avroRecord);

                ExtendedCallback extendedCallback = new ExtendedCallback(producerRecord);
                Future<RecordMetadata> future = kafkaProducer
                    .send(producerRecord, extendedCallback);

                logger.info("Publishing message, key = [" + key + "] value = [" + avroRecord.get("currentValue") + "]");

                future.get();

                if (extendedCallback.getException() != null) {
                    logger.error("Exception", extendedCallback.getException());
                }

                if (extendedCallback.isError()) {
                    logger.error("isError = [" + extendedCallback.isError() + "]");
                }
            }
        } finally {
            if (null != kafkaProducer) {
                kafkaProducer.flush();
                kafkaProducer.close();
            }
        }
    }

    public class ExtendedCallback implements Callback {

        private ProducerRecord producerRecord;

        private RecordMetadata recordMetadata;

        private Exception exception;

        public ExtendedCallback(ProducerRecord<String, GenericRecord> producerRecord) {
            this.producerRecord = producerRecord;
        }

        public boolean isError() {
            return (null != this.exception);
        }

        private RecordMetadata getRecordMetadata() {
            return this.recordMetadata;
        }

        public Exception getException() {
            return this.exception;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
            this.recordMetadata = recordMetadata;

            if (null == exception) {
                //logger.info("Received, key = [" + producerRecord.key() + "] value = [" + producerRecord.value() + "] topic = [" + recordMetadata.topic() + "] partition = [" + recordMetadata.partition() + "] offset = [" + recordMetadata.offset() + "] timestamp = [" + toISOTimestamp(recordMetadata.timestamp(), "America/New_York") + "]");
            }

            this.exception = exception;
        }
    }

    private static String getISOTimestamp() {
        return toISOTimestamp(System.currentTimeMillis(), "America/New_York");
    }

    private static String toISOTimestamp(long milliseconds, String timeZoneId) {
        return Instant.ofEpochMilli(milliseconds).atZone(ZoneId.of(timeZoneId)).toString().replace("[" + timeZoneId + "]", "");
    }

    private static long randomLong(int min, int max) {
        if (max == min) {
            return min;
        }

        if (min > max) {
            throw new IllegalArgumentException("min must be <= max, min = [" + min + "] max = [" + max + "]");
        }

        return + (long) (Math.random() * (max - min));
    }

    private String randomString(int length) {
        return RANDOM.ints(48, 122 + 1)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }
}
