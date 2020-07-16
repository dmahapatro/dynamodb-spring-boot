package com.example.dynamodb.dynamodbspringboot.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class FeedConsumer {
  private final Logger logger = LoggerFactory.getLogger(FeedConsumer.class);
  private final FeedService feedService;

  public FeedConsumer(FeedService feedService) {
    this.feedService = feedService;
  }

  @KafkaListener(topics = "feeds", groupId = "group_id")
  public void consume(String message) throws IOException {
    logger.info(String.format("#### -> Consumed message -> %s", message));
    feedService.createFeedItem(message);
  }
}
