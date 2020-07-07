package com.example.dynamodb.dynamodbspringboot.controllers;

import com.example.dynamodb.dynamodbspringboot.model.Feed;
import com.example.dynamodb.dynamodbspringboot.services.FeedService;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
public class FeedController {
  private final FeedService feedService;

  public FeedController(FeedService feedService) {
    this.feedService = feedService;
  }

  @GetMapping("/feeds")
  public List<Feed> getFeeds(
    @RequestParam("id") final String id,
    @RequestParam(value = "sort", required = false) final String sortKey
  ) {
    if(!StringUtils.isEmpty(id) && !StringUtils.isEmpty(sortKey)) {
      return Collections.singletonList(feedService.getFeed(id, sortKey).orElse(null));
    } else if (!StringUtils.isEmpty(id)) {
      return feedService.getFeedsById(id);
    }

    return Collections.emptyList();
  }

  @GetMapping("/feeds/range")
  public List<Feed> getFeedsByDateRange(
    @RequestParam final String day,
    @RequestParam final String startTime,
    @RequestParam final String endTime
  ) {
    return feedService.getFeedByDateAndTimeRange(day, startTime, endTime);
  }
}
