
package io.github.oleiva.analytics;


import io.github.oleiva.analytics.service.kafka.KafkaService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@EnableCaching
@SpringBootApplication
@Slf4j
@RequiredArgsConstructor
public class MainApp implements CommandLineRunner {
    private final KafkaService kafkaService;

    public static void main(String[] args) {
        SpringApplication.run(MainApp.class, args);
    }

    @Override
    public void run(String... args) {
        log.info("Running Kafka consumer");
        kafkaService.runConsumer();
    }
}
