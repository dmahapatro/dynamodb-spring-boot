package com.example.dynamodb.dynamodbspringboot;

import com.example.dynamodb.dynamodbspringboot.model.Feed;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.NonNull;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.CreateTableEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.EnhancedGlobalSecondaryIndex;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;
import static software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional.keyEqualTo;
import static software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional.sortBetween;

@SpringBootTest
@ContextConfiguration(initializers = KafkaConsumeToDynamoTableTest.Initializer.class)
public class KafkaConsumeToDynamoTableTest {
  private static final Logger log = LoggerFactory.getLogger(KafkaConsumeToDynamoTableTest.class);
  private static final String TOPIC_NAME = "feeds";
  private static final String TABLE_NAME = "FeedMgmt";

  @Rule
  public static KafkaContainer kafka = new KafkaContainer("5.5.1")
    .withLogConsumer(new Slf4jLogConsumer(log));

  @Container
  public static LocalStackContainer localStack = new LocalStackContainer()
    .withServices(DYNAMODB)
    .withNetwork(kafka.getNetwork());

  static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(@NonNull ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues values = TestPropertyValues.of(
        "spring.kafka.producer.bootstrap-servers=" + kafka.getBootstrapServers(),
        "spring.kafka.consumer.bootstrap-servers=" + kafka.getBootstrapServers(),
        "spring.kafka.consumer.group-id=group_id",
        "spring.kafka.consumer.auto-offset-reset=earliest",

        "dynamo.endpoint=" + localStack.getEndpointConfiguration(DYNAMODB).getServiceEndpoint(),
        "dynamo.accessKey=" + localStack.getAccessKey(),
        "dynamo.secretKey=" + localStack.getSecretKey()
      );
      values.applyTo(configurableApplicationContext);
    }
  }

  @Autowired
  private DynamoDbEnhancedClient dynamoDbEnhancedClient;

  @Autowired
  private DynamoDbClient dynamoDbClient;

  @Autowired
  @SuppressWarnings("all")
  private KafkaTemplate<String, String> kafkaTemplate;

  @BeforeAll
  static void setup() {
    kafka.start();
    localStack.start();
  }

  @AfterAll
  static void teardown() {
    kafka.stop();
    localStack.stop();
  }

  @BeforeEach
  void buildUp() {
    final TableSchema<Feed> FEED_TABLE_SCHEMA = TableSchema.fromBean(Feed.class);
    final DynamoDbTable<Feed> feedTable = dynamoDbEnhancedClient.table(TABLE_NAME, FEED_TABLE_SCHEMA);

    // Create Feed Table and the GSI explicitly
    feedTable.createTable(
      CreateTableEnhancedRequest.builder()
        .globalSecondaryIndices(
          EnhancedGlobalSecondaryIndex.builder()
            .indexName("DateIdx")
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .build()
        )
        .build()
    );
  }

  @AfterEach
  void purge() {
    dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
  }

  private void publish(final String message) {
    kafkaTemplate.send(TOPIC_NAME, message);
  }

  @Test
  @DisplayName("Check upon receiving message, it is parsed and added to DynamoDB")
  public void testConsumerReceivingMessage() {
    // WHEN: A message is published in the topic
    publish(
      "2020-07-14 14:41:06,950 INFO  DPLogger - uuid: e55e438e-1703-4331-84e9-0eb7feb1d2da, component: Eb2bEgressSingleOpChannel, ftm: claims_lte_s3_to_nas_lte01t, file: beta/nwindem/interface-test/lte01t/LTECLAIMGL_CONTROL_FEED.ctl, status: Failed, msg: Leaving Eb2bEgressSingleOpChannel sync() - failed results for file beta/nwindem/interface-test/lte01t/LTECLAIMGL_CONTROL_FEED.ctl, timestamp: Tue Jul 14 14:41:06 EDT 2020"
    );

    // THEN: Message is consumed and transformed to DynamoDB items in FeedMgmt Table
    Unreliables.retryUntilTrue(30, TimeUnit.SECONDS, () -> {
      final DynamoDbTable<Feed> feedTable = dynamoDbEnhancedClient.table(TABLE_NAME, TableSchema.fromBean(Feed.class));

      PageIterable<Feed> pagedResults = feedTable.query(
        keyEqualTo(Key.builder().partitionValue("e55e438e-1703-4331-84e9-0eb7feb1d2da").build())
      );

      List<Feed> feeds = pagedResults.items().stream().collect(Collectors.toList());

      if(feeds.isEmpty()) {
        return false;
      }

      assertThat(feeds)
        .hasSize(2)
        .extracting(Feed::getPK, f -> Stream.of(f.getSK().split("\\|")).limit(3).collect(joining("|")))
        .contains(
          tuple("e55e438e-1703-4331-84e9-0eb7feb1d2da", "C|Eb2bEgressSingleOpChannel|Failed"),
          tuple("e55e438e-1703-4331-84e9-0eb7feb1d2da", "F|e55e438e-1703-4331-84e9-0eb7feb1d2da")
        );

      return true;
    });
  }

  @Test
  @DisplayName("Check date range query")
  public void testFeedsDateRangeQueryOnIndex() {
    // WHEN: A message is published in the topic
    publish(
      "2020-07-14 14:41:06,950 INFO  DPLogger - uuid: c45e438e-1703-4331-84e9-0eb7feb1f2da, component: Eb2bEgressSingleOpChannel, ftm: claims_lte_s3_to_nas_lte01t, file: beta/nwindem/interface-test/lte01t/LTECLAIMGL_CONTROL_FEED.ctl, status: Failed, msg: Leaving Eb2bEgressSingleOpChannel sync() - failed results for file beta/nwindem/interface-test/lte01t/LTECLAIMGL_CONTROL_FEED.ctl, timestamp: Tue Jul 14 14:41:06 EDT 2020"
    );

    // Another message is published to the same topic after a delay
    Executors.newSingleThreadScheduledExecutor()
      .scheduleWithFixedDelay(
        () -> publish("2020-07-14 14:41:06,950 INFO  DPLogger - uuid: ab60v38e-1703-4331-84e9-0eb7feb1f2da, component: Eb2bEgressSingleOpChannel, ftm: claims_lte_s3_to_nas_lte01t, file: beta/nwindem/interface-test/lte01t/LTECLAIMGL_CONTROL_FEED.ctl, status: Failed, msg: Leaving Eb2bEgressSingleOpChannel sync() - failed results for file beta/nwindem/interface-test/lte01t/LTECLAIMGL_CONTROL_FEED.ctl, timestamp: Tue Jul 14 14:41:06 EDT 2020"),
        3,1, TimeUnit.SECONDS
      );

    // THEN: Message is consumed and transformed to DynamoDB items in FeedMgmt Table
    Unreliables.retryUntilTrue(30, TimeUnit.SECONDS, () -> {
      final DynamoDbTable<Feed> feedTable = dynamoDbEnhancedClient.table(TABLE_NAME, TableSchema.fromBean(Feed.class));
      String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

      DynamoDbIndex<Feed> feedsByDateIndex = feedTable.index("DateIdx");

      SdkIterable<Page<Feed>> feedsInDateRange = feedsByDateIndex.query(r ->
        r.queryConditional(
          sortBetween(
            k -> k.partitionValue(today).sortValue("0000"),
            k -> k.partitionValue(today).sortValue("2359")
          )
        )
      );

      List<Feed> feedsByRange = feedsInDateRange.stream()
        .flatMap(feedPage -> feedPage.items().stream())
        .collect(Collectors.toList());

      // Since second message was added with a delay, in order to assert for 2 items to be returned
      // the retry logic has to be executed until we have 2 items returned from the query
      if(feedsByRange.isEmpty() || feedsByRange.size() < 2) {
        return false;
      }

      assertThat(feedsByRange)
        .hasSize(2)
        .extracting(Feed::getPK, Feed::getSK)
        .contains(
          tuple("c45e438e-1703-4331-84e9-0eb7feb1f2da", "F|c45e438e-1703-4331-84e9-0eb7feb1f2da"),
          tuple("ab60v38e-1703-4331-84e9-0eb7feb1f2da", "F|ab60v38e-1703-4331-84e9-0eb7feb1f2da")
        );

      return true;
    });
  }
}
