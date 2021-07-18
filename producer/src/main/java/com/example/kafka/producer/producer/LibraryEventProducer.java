package com.example.kafka.producer.producer;

import com.example.kafka.producer.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;

@Component
@RequiredArgsConstructor
@Slf4j
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

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


    public SendResult<Integer, String> sendLibraryEventsAsynchronous(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.getLibraryEventId();

        //using object mapper to write object as Json String
        String value = objectMapper.writeValueAsString(libraryEvent);

        SendResult<Integer, String> sendResult = null;
        try {
            //producing kafka message asynchronously
            sendResult = kafkaTemplate.sendDefault(key, value).get();
        } catch (ExecutionException | InterruptedException e ) {
            log.error("Execution/Interrupted exception : {}", e.getMessage());
        } catch(Exception e){
            log.error("Exception in send Asynchronous: {}", e.getMessage());
        }

        return sendResult;
    }


}
