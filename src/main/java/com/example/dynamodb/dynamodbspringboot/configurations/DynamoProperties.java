package com.example.dynamodb.dynamodbspringboot.configurations;

import org.springframework.boot.context.properties.ConfigurationProperties;
import software.amazon.awssdk.regions.Region;

import java.net.URI;

@ConfigurationProperties("dynamo")
public class DynamoProperties {
  private URI endpoint;
  private Region region;
  private String accessKey;
  private String secretKey;

  public URI getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(URI endpoint) {
    this.endpoint = endpoint;
  }

  public Region getRegion() {
    return region;
  }

  public void setRegion(Region region) {
    this.region = region;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }
}
