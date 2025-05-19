package com.google.cloud.hadoop.util;

import static com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX;

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
    INVOCATION_ID.set(GCCL_INVOCATION_ID_PREFIX + UUID.randomUUID());
  }

  public static void clear() {
    INVOCATION_ID.remove();
  }
}
