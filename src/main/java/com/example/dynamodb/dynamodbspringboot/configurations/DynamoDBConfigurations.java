package com.example.dynamodb.dynamodbspringboot.configurations;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

@Configuration
@EnableConfigurationProperties(DynamoProperties.class)
public class DynamoDBConfigurations {

  @Bean
  public DynamoDbEnhancedClient dynamoDbEnhancedClient(DynamoProperties dynamoProperties) {
    return DynamoDbEnhancedClient.builder()
      .dynamoDbClient(dynamoDbClient(dynamoProperties))
      .build();
  }

  @Bean
  public DynamoDbClient dynamoDbClient(DynamoProperties dynamoProperties) {
    DynamoDbClientBuilder builder = DynamoDbClient.builder()
      .region(dynamoProperties.getRegion() != null ? dynamoProperties.getRegion() : Region.US_EAST_2);

    if(dynamoProperties.getEndpoint() != null) {
      builder.endpointOverride(dynamoProperties.getEndpoint());
    }

    if(
      !StringUtils.isEmpty(dynamoProperties.getAccessKey()) &&
        !StringUtils.isEmpty(dynamoProperties.getSecretKey())
    ) {
      builder.credentialsProvider(
        StaticCredentialsProvider.create(
          AwsBasicCredentials.create(dynamoProperties.getAccessKey(), dynamoProperties.getSecretKey())
        )
      );
    }

    return builder.build();
  }
}
