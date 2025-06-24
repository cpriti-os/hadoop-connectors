package com.google.cloud.hadoop.util;

import com.google.api.client.http.HttpRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.GeneratedMessageV3;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public class GcsGrpcApiEvent implements IGcsApiEvent {
  public static final String BACKOFF_TIME = "BACKOFF_TIME";
  public static final String RETRY_COUNT = "RETRY_COUNT";
  public static final String STATUS_CODE = "STATUS_CODE";
  public static final String DURATION = "DURATION";
  public static final String REQUEST_TYPE = "REQUEST_TYPE";
  private final EventType eventType;

  // Having this as Object type so that we do not have to create the URL string.
  private final Object context;
  private final String method;
  private Map<String, Object> properties;

  @VisibleForTesting
  protected GcsGrpcApiEvent(
      @Nonnull GeneratedMessageV3 request, EventType eventType, Object context) {
    this.eventType = eventType;
    this.context = context;
    this.method = request.getDescriptorForType().getFullName();
  }

  private GcsGrpcApiEvent(
      GeneratedMessageV3 request, EventType eventType, int capacity, Object context) {
    this(request, eventType, context);
    this.properties = new HashMap<>(capacity, 1);
  }

  @VisibleForTesting
  public static GcsGrpcApiEvent getResponseEvent(
      GeneratedMessageV3 response, @Nonnegative long duration, Object context) {
    return null;
  }

  static GcsGrpcApiEvent getRequestStartedEvent(HttpRequest request, Object context) {
    return null;
  }

  @Override
  public EventType getEventType() {
    return eventType;
  }

  @Override
  public Object getContext() {
    return context;
  }

  @Override
  public String getMethod() {
    return method;
  }

  @Override
  public Object getProperty(String key) {
    return properties != null ? properties.get(key) : null;
  }

  @Override
  public ApiType getApiType() {
    return ApiType.GRPC;
  }
}
