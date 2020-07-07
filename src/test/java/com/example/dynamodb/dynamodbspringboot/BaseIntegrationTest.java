package com.example.dynamodb.dynamodbspringboot;

import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;

@Testcontainers
@ContextConfiguration(initializers = BaseIntegrationTest.Initializer.class)
public abstract class BaseIntegrationTest {
  @Container
  public static LocalStackContainer localStack = new LocalStackContainer()
    .withServices(DYNAMODB);

  static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
      TestPropertyValues values = TestPropertyValues.of(
        "dynamo.endpoint=" + localStack.getEndpointConfiguration(DYNAMODB).getServiceEndpoint(),
        "dynamo.accessKey=" + localStack.getAccessKey(),
        "dynamo.secretKey=" + localStack.getSecretKey()
      );
      values.applyTo(configurableApplicationContext);
    }
  }
}
