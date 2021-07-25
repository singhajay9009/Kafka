package com.example.kafka.producerApp.producer;


import com.example.kafka.producerApp.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private String topic = "library-events";

    /*
    This is asynchronously sending messages on Kafka
     */
    public void sendLibraryEvents(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();

        //using object mapper to write object as Json String
        String value = objectMapper.writeValueAsString(libraryEvent);

        //producing kafka message
        ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                handleFailure(key, value, throwable);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
               handleSuccess(key, value, result);
            }

        });
    }

    private void handleFailure(Integer key, String value, Throwable throwable) {
        log.error("Error sending the message. Exception is: {}", throwable.getMessage());
        try{
            throw throwable;
        }catch(Throwable throwable1){
            throwable.printStackTrace();
        }


    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message published successfully for key: {} and value: {}. Partition is: {}", key, value, result.getRecordMetadata().partition());
    }


    /*
    This method lets us sending message on Kafka synchronously
    calling get() on send will block this and wait until message is produced.
     */
    public SendResult<Integer, String> sendLibraryEventsSynchronous(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();

        //using object mapper to write object as Json String
        String value = objectMapper.writeValueAsString(libraryEvent);

        SendResult<Integer, String> sendResult = null;
        try {
            //producing kafka message synchronously
            sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
        } catch (ExecutionException | InterruptedException e ) {
            log.error("Execution/Interrupted exception : {}", e.getMessage());
        } catch(Exception e){
            log.error("Exception in send Synchronous: {}", e.getMessage());
        }

        return sendResult;
    }

    public void sendResult_method2(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();

        //using object mapper to write object as Json String
        String value = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.send(getProducerRecord(key, value, topic));
    }

    public ProducerRecord<Integer, String> getProducerRecord (Integer key, String value, String topic) throws JsonProcessingException {

        List<Header> headers = List.of(new RecordHeader("event-source", "scanner".getBytes(StandardCharsets.UTF_8)));
        return new ProducerRecord<Integer, String>(topic, null, key, value, headers);
    }


}
