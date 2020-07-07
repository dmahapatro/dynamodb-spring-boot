package com.example.dynamodb.dynamodbspringboot.model;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

@DynamoDbBean
public class Feed {
  private String PK;
  private String SK;
  private String component;
  private String componentStatus;
  private String timestamp;
  private String message;
  private String feedDay;
  private String feedTime;

  @DynamoDbPartitionKey
  public String getPK() {
    return PK;
  }

  public void setPK(String PK) {
    this.PK = PK;
  }

  @DynamoDbSortKey
  public String getSK() {
    return SK;
  }

  public void setSK(String SK) {
    this.SK = SK;
  }

  @DynamoDbAttribute(value = "COMP")
  public String getComponent() {
    return component;
  }

  public void setComponent(String component) {
    this.component = component;
  }

  @DynamoDbAttribute(value = "CSTAT")
  public String getComponentStatus() {
    return componentStatus;
  }

  public void setComponentStatus(String componentStatus) {
    this.componentStatus = componentStatus;
  }

  @DynamoDbSecondaryPartitionKey(indexNames = "DateIdx")
  @DynamoDbAttribute("FDAY")
  public String getFeedDay() {
    return feedDay;
  }

  public void setFeedDay(String feedDay) {
    this.feedDay = feedDay;
  }

  @DynamoDbSecondarySortKey(indexNames = "DateIdx")
  @DynamoDbAttribute("FTIME")
  public String getFeedTime() {
    return feedTime;
  }

  public void setFeedTime(String feedTime) {
    this.feedTime = feedTime;
  }

  @DynamoDbAttribute(value = "TIMESTAMP")
  public String getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  @DynamoDbAttribute(value = "MSG")
  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  @Override
  public String toString() {
    return "Feed{" +
      "PK='" + PK + '\'' +
      ", SK='" + SK + '\'' +
      ", component='" + component + '\'' +
      ", componentStatus='" + componentStatus + '\'' +
      ", timestamp='" + timestamp + '\'' +
      ", message='" + message + '\'' +
      ", feedDay='" + feedDay + '\'' +
      ", feedTime='" + feedTime + '\'' +
      '}';
  }
}
