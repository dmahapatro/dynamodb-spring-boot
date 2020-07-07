package com.example.dynamodb.dynamodbspringboot.services;

import com.example.dynamodb.dynamodbspringboot.model.Feed;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchGetResultPageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.ReadBatch;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional.keyEqualTo;
import static software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional.sortBetween;

@Service
public class FeedService {
  private final static String FEED_TABLE_NAME = "FeedMgmt";
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
}
