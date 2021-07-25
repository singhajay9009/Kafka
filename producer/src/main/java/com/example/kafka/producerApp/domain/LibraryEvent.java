package com.example.kafka.producerApp.domain;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class LibraryEvent {

    private Integer libraryEventId;
    private Book book;
    private LibraryEventType libraryEventType;
}
