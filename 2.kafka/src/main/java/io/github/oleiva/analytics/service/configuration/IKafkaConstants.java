package io.github.oleiva.analytics.service.configuration;

public abstract class IKafkaConstants {
    public static final String KAFKA_BROKERS = "localhost:9092";
    public static final String TOPIC_NAME = "bitcoin-transactions";
    public static final String GROUP_ID_CONFIG = "consumerGroup10";
    public static final Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    public static final String OFFSET_RESET_EARLIER = "earliest";
    public static final Integer MAX_POLL_RECORDS = 1000;
    public static final Integer BATCH_DELAY =2000;
}
