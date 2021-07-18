package com.example.kafka.producer.controller;

import com.example.kafka.producer.domain.Book;
import com.example.kafka.producer.domain.LibraryEvent;
import com.example.kafka.producer.domain.LibraryEventType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
                        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventControllerTest {

    @Autowired
    TestRestTemplate testRestTemplate;

    @Test
    public void postLibraryEvent(){
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

    }
}
