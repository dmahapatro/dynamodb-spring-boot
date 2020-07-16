package com.example.dynamodb.dynamodbspringboot;

import com.example.dynamodb.dynamodbspringboot.model.Feed;
import com.example.dynamodb.dynamodbspringboot.services.FeedService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Optional;

import static org.junit.Assert.*;

@SpringBootTest
public class StringTokenizerTest {

  @Autowired
  private FeedService feedService;

  @Test
  @DisplayName("Check Kafka log event tokenized properly")
  void testLogEventStringTokens() {
    String message = "2020-07-12T01:00:00Z INFO FooBar uuid: 1q2n3nr-12edi-asd22, component: TestComponent, ftm: test-ftm, status: InProgress, msg: Test Log Message";

    Optional<Feed> result = feedService.tokenizeMessageToFeed(message);
    assertTrue(result.isPresent());

    Feed feed = result.orElse(null);
    assertEquals("1q2n3nr-12edi-asd22", feed.getPK());
    assertEquals("TestComponent", feed.getComponent());
    assertEquals("Test Log Message", feed.getMessage());
    assertEquals("InProgress", feed.getComponentStatus());
  }
}
