package com.google.cloud.hadoop.util;

import java.util.UUID;

public class InvocationIdContext {
  private static final InheritableThreadLocal<String> INVOCATION_ID =
      new InheritableThreadLocal<>() {
        @Override
        protected String initialValue() {
          return "";
        }
      };

  public static String getInvocationId() {
    return INVOCATION_ID.get();
  }

  public static void setInvocationId() {
    INVOCATION_ID.set(UUID.randomUUID() + ":");
  }

  public static void clear() {
    INVOCATION_ID.remove();
  }
}
