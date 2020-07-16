package com.example.dynamodb.dynamodbspringboot.services;

import com.example.dynamodb.dynamodbspringboot.model.Feed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional.keyEqualTo;
import static software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional.sortBetween;

@Service
public class FeedService {
  private static final Logger log = LoggerFactory.getLogger(FeedService.class);
  private static final String FEED_TABLE_NAME = "FeedMgmt";
  private static final String UUID_TOKEN = "uuid:";
  private static final String COMPONENT_SORT_KEY_FORMAT = "C|%s|%s|%s";
  private static final String FEED_SORT_KEY_FORMAT = "F|%s";

  private final DynamoDbEnhancedClient dynamoDbEnhancedClient;
  private final DynamoDbTable<Feed> feedTable;

  public FeedService(@NonNull final DynamoDbEnhancedClient dynamoDbEnhancedClient) {
    this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;
    feedTable = dynamoDbEnhancedClient.table(FEED_TABLE_NAME, TableSchema.fromBean(Feed.class));
  }

  public List<Feed> getFeedsById(final String uuid) {
    List<Feed> results = Collections.emptyList();

    try {
      PageIterable<Feed> pagedResults = feedTable.query(
        keyEqualTo(Key.builder().partitionValue(uuid).build())
      );

      results = pagedResults.items().stream().collect(Collectors.toList());
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
    }

    return results;
  }

  public List<Feed> getFeedsByIds(@NonNull final List<String> ids) {
    ReadBatch.Builder<Feed> readBatchBuilder = ReadBatch.builder(Feed.class).mappedTableResource(feedTable);
    ids.forEach(id -> readBatchBuilder.addGetItem(Key.builder().partitionValue(id).build()));

    BatchGetResultPageIterable batchResults = dynamoDbEnhancedClient.batchGetItem(
      r -> r.addReadBatch(readBatchBuilder.build())
    );

    return batchResults.resultsForTable(feedTable).stream().collect(Collectors.toList());
  }

  public List<Feed> getFeedByDateAndTimeRange(
    final String date,
    final String startTime,
    final String endTime
  ) {
    DynamoDbIndex<Feed> feedsByDateIndex = feedTable.index("DateIdx");

    SdkIterable<Page<Feed>> feedsInDateRange = feedsByDateIndex.query(r ->
      r.queryConditional(
        sortBetween(
          k -> k.partitionValue(date).sortValue(startTime),
          k -> k.partitionValue(date).sortValue(endTime)
        )
      )
    );

    return feedsInDateRange.stream()
      .flatMap(feedPage -> feedPage.items().stream())
      .collect(Collectors.toList());
  }

  public Optional<Feed> getFeed(final String id, final String sortKey) {
    Optional<Feed> feed = Optional.empty();

    try {
      feed = Optional.ofNullable(
        feedTable.getItem(Key.builder().partitionValue(id).sortValue(sortKey).build())
      );
    } catch (DynamoDbException e) {
      System.err.println(e.getMessage());
    }

    return feed;
  }

  public void createFeedItem(String message) {
    Optional<Feed> feed = tokenizeMessageToFeed(message);

    if(feed.isPresent()) {
      Feed componentFeed = feed.get();
      Feed feedItemToUpdate = getFeedItemToUpdate(
        componentFeed.getPK(),
        componentFeed.getComponent(),
        componentFeed.getComponentStatus()
      );

      BatchWriteItemEnhancedRequest batchWriteItemEnhancedRequest = BatchWriteItemEnhancedRequest.builder()
        .writeBatches(
          WriteBatch.builder(Feed.class)
            .mappedTableResource(feedTable)
            .addPutItem(componentFeed) // Create component item
            .addPutItem(feedItemToUpdate) // Update feed item with latest component name and status
            .build()
        ).build();

      dynamoDbEnhancedClient.batchWriteItem(batchWriteItemEnhancedRequest);
    }
  }

  private Feed getFeedItemToUpdate(
    final String pk,
    final String component,
    final String componentStatus
  ) {
    LocalDateTime now = LocalDateTime.now();

    Feed feedItem = new Feed();
    feedItem.setPK(pk);
    feedItem.setSK(String.format(FEED_SORT_KEY_FORMAT, pk));
    feedItem.setComponent(component);
    feedItem.setComponentStatus(componentStatus);
    feedItem.setFeedDay(now.format(DateTimeFormatter.ofPattern("yyyyMMdd")));
    feedItem.setFeedTime(now.format(DateTimeFormatter.ofPattern("HHmm")));

    return feedItem;
  }

  public Optional<Feed> tokenizeMessageToFeed(String message) {
    if(StringUtils.isEmpty(message)) {
      return Optional.empty();
    }

    Map<String, String> result = Stream.of(message.split(","))
      .map(s -> s.contains(UUID_TOKEN) ? s.substring(s.indexOf(UUID_TOKEN)).trim(): s.trim())
      .map(s -> s.split(":"))
      .filter(s -> !StringUtils.isEmpty(s[1]) && !"null".equalsIgnoreCase(s[1].trim()))
      .collect(toMap(k -> k[0], v -> v[1].trim()));

    if(!result.keySet().containsAll(Arrays.asList("uuid", "component", "status", "msg"))) {
      log.error("Message is missing key attributes that are needed in dynamodb table");
      return Optional.empty();
    }

    Feed feed = new Feed();
    feed.setPK(result.get("uuid"));
    feed.setComponent(result.get("component"));
    feed.setComponentStatus(result.get("status"));
    feed.setMessage(result.get("msg"));
    feed.setTimestamp(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
    feed.setSK(
      String.format(COMPONENT_SORT_KEY_FORMAT, feed.getComponent(), feed.getComponentStatus(), feed.getTimestamp())
    );

    return Optional.of(feed);
  }
}
