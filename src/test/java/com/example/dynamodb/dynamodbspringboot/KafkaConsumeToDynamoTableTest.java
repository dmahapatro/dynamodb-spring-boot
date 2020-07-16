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
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;
import static software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional.keyEqualTo;

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
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
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
    feedTable.createTable();
  }

  @AfterEach
  void purge() {
    dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(TABLE_NAME).build());
  }

  @Test
  @DisplayName("Check upon receiving message, it is parsed and added to DynamoDB")
  public void testConsumerReceivingMessage() {
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
      ImmutableMap.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
        ConsumerConfig.GROUP_ID_CONFIG, "group_id",
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
      ),
      new StringDeserializer(),
      new StringDeserializer()
    );

    consumer.subscribe(Collections.singleton(TOPIC_NAME));

    kafkaTemplate.send(
      TOPIC_NAME,
      "2020-07-14 14:41:06,950 INFO  DPLogger - uuid: e55e438e-1703-4331-84e9-0eb7feb1d2da, component: Eb2bEgressSingleOpChannel, ftm: claims_lte_s3_to_nas_lte01t, file: beta/nwindem/interface-test/lte01t/LTECLAIMGL_CONTROL_FEED.ctl, status: Failed, msg: Leaving Eb2bEgressSingleOpChannel sync() - failed results for file beta/nwindem/interface-test/lte01t/LTECLAIMGL_CONTROL_FEED.ctl, timestamp: Tue Jul 14 14:41:06 EDT 2020"
    );

    Unreliables.retryUntilTrue(30, TimeUnit.SECONDS, () -> {
      consumer.poll(Duration.ofMillis(100));

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

    consumer.unsubscribe();
  }
}
