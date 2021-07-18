package com.example.kafka.producer.controller;

import com.example.kafka.producer.domain.Book;
import com.example.kafka.producer.domain.LibraryEvent;
import com.example.kafka.producer.domain.LibraryEventType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventControllerTest {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    public void setUp(){
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    public void tearDown(){
        consumer.close();
    }

    @Test
    @Timeout(5)
    public void postLibraryEvent() throws InterruptedException {
        //Given
        Book book = Book.builder()
                .bookId(1001)
                .bookAuthor("James Rolling")
                .bookName("Life after Death")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .libraryEventType(LibraryEventType.NEW)
                .book(book)
                .build();

        //When
        ResponseEntity<?> responseEntity = testRestTemplate.postForEntity("/v1/libraryevent/", libraryEvent, LibraryEvent.class);

        //Then
        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, "library-events");

        //sleep is record because previous call KafkaUtils.getSingleRecord() is asynchronous
        // Our test case is running in a different thread and getSingleRecord() is called in different thread.
        // Below line is commented as we have put timeout( 5 seconds ) at method level
        // else we can keep the below call to sleep in place.
        Thread.sleep(3000);
        String expectedValue = "{\"libraryEventId\":null,\"book\":{\"bookId\":1001,\"bookName\":\"Life after Death\",\"bookAuthor\":\"James Rolling\"},\"libraryEventType\":\"NEW\"}";

        String actualValue = consumerRecord.value();

        assertEquals(expectedValue, actualValue);
    }
}
