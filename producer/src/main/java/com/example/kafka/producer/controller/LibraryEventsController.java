package com.example.kafka.producer.controller;


import com.example.kafka.producer.domain.LibraryEvent;
import com.example.kafka.producer.domain.LibraryEventType;
import com.example.kafka.producer.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequiredArgsConstructor
@Slf4j
public class LibraryEventsController {

    private final LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        //invoke Kafka Producer sending messages asynchronously
    //    libraryEventProducer.sendLibraryEvents(libraryEvent);


        //invoke Kafka Producer sending messages *Synchronously*
    //      SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventsSynchronous(libraryEvent);
      //  log.info("Produced messages is: {}", sendResult.toString());

        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendResult_method2(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
