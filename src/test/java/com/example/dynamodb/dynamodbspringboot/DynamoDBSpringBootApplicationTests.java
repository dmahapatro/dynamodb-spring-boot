package com.example.dynamodb.dynamodbspringboot;

import com.example.dynamodb.dynamodbspringboot.controllers.FeedController;
import com.example.dynamodb.dynamodbspringboot.model.Feed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

@SpringBootTest
class DynamoDBSpringBootApplicationTests extends BaseIntegrationTest {
  private static final String TABLE_NAME = "FeedMgmt";

  @Autowired
  private DynamoDbEnhancedClient dynamoDbEnhancedClient;

  @Autowired
  private DynamoDbClient dynamoDbClient;

  @Autowired
  private FeedController feedController;

  @BeforeEach
  void setup() {
    final TableSchema<Feed> FEED_TABLE_SCHEMA = TableSchema.fromBean(Feed.class);
    final DynamoDbTable<Feed> feedTable = dynamoDbEnhancedClient.table(TABLE_NAME, FEED_TABLE_SCHEMA);
    feedTable.createTable();
  }

  @AfterEach
  void teardown() {
    dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
  }

  @Test
  @DisplayName("Check Application Context Loads")
  void contextLoads() {
  }

  @Test
  @DisplayName("Check Local Stack is running")
  void checkLocalStackIsRunning() {
    assertTrue(localStack.isRunning());
  }

  @Test
  @DisplayName("Check FeedMgmt Table exists")
  void testFeedMgmtTableExist() {
    assertEquals(
      "FeedMgmt Table not present",
      dynamoDbClient.listTables().tableNames(),
      Collections.singletonList(TABLE_NAME)
    );
  }

  @Test
  @DisplayName("Test appropriate partition and sort key is setup")
  void testPKAndSKPresent() {
    Set<String> attributeNames = dynamoDbClient
      .describeTable(DescribeTableRequest.builder().tableName(TABLE_NAME).build())
      .table()
      .attributeDefinitions().stream()
      .map(a -> String.join("-", a.attributeName(), a.attributeTypeAsString()))
      .collect(Collectors.toSet());

    assertIterableEquals(new HashSet<>(Arrays.asList("PK-S", "SK-S")), attributeNames);
  }

  @Test
  @DisplayName("Test Get Feeds API returns result")
  void testGetFeedAPIReturnsResults() {
    // GIVEN
    final DynamoDbTable<Feed> feedTable = dynamoDbEnhancedClient.table(TABLE_NAME, TableSchema.fromBean(Feed.class));
    Feed feed = new Feed();
    feed.setPK("1");
    feed.setSK("1");
    feed.setComponent("TestComponent");
    feed.setComponentStatus("COMPLETED");
    feed.setMessage("This is a test message");

    feedTable.putItem(feed);

    // WHEN
    List<Feed> results = feedController.getFeeds("1", "1");

    // THEN
    assertNotNull(results);
    assertFalse(results.isEmpty());

    Feed result = results.get(0);

    assertEquals("This is a test message", result.getMessage());
    assertEquals("TestComponent", result.getComponent());
    assertEquals("COMPLETED", result.getComponentStatus());
  }

  @Test
  @DisplayName("Test Get Feeds API returns multiple feeds when sort key not provided")
  void testGetFeedsAPIReturnsMultipleFeeds() {
    // GIVEN
    final DynamoDbTable<Feed> feedTable = dynamoDbEnhancedClient.table(TABLE_NAME, TableSchema.fromBean(Feed.class));
    Feed feed = new Feed();
    feed.setPK("1");
    feed.setSK("1");
    feed.setComponent("TestComponent");
    feed.setComponentStatus("COMPLETED");
    feed.setMessage("This is a test message for Test Component");

    Feed feed1 = new Feed();
    feed1.setPK("1");
    feed1.setSK("2");
    feed1.setComponent("AnotherComponent");
    feed1.setComponentStatus("STARTED");
    feed1.setMessage("This is a test message for another component");

    Feed feed2 = new Feed();
    feed2.setPK("2");
    feed2.setSK("3");
    feed2.setComponent("YetAnotherComponent");
    feed2.setComponentStatus("IN_PROGRESS");
    feed2.setMessage("This is a test message for yet another component");

    dynamoDbEnhancedClient.batchWriteItem(r -> r.addWriteBatch(
      WriteBatch.builder(Feed.class)
        .mappedTableResource(feedTable)
        .addPutItem(feed)
        .addPutItem(feed1)
        .addPutItem(feed2)
        .build()
    ));

    // WHEN
    List<Feed> results = feedController.getFeeds("1", null);

    // THEN
    assertNotNull(results);
    assertFalse(results.isEmpty());
    assertEquals(2, results.size());

    Set<String> components = results.stream()
      .map(Feed::getComponent)
      .collect(Collectors.toSet());

    assertEquals(
      new HashSet<>(
        Arrays.asList(
          "TestComponent",
          "AnotherComponent"
        )
      ),
      components
    );
  }
}
