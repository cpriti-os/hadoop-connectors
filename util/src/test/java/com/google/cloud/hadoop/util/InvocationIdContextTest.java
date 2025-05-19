package com.google.cloud.hadoop.util;

import org.junit.Before;
import org.junit.Test;

import static com.google.cloud.hadoop.util.interceptors.InvocationIdInterceptor.GCCL_INVOCATION_ID_PREFIX;
import static org.junit.Assert.*;

public class InvocationIdContextTest {

    @Before
    public void clearInvocationId() {
        // Ensure the INVOCATION_ID is cleared before each test
        InvocationIdContext.clear();
    }

    @Test
    public void testInitialValue() {
        // Test that the initial value of INVOCATION_ID is an empty string
        assertEquals("", InvocationIdContext.getInvocationId());
    }

    @Test
    public void testSetInvocationId() {
        // Set a new invocation ID and verify it is not empty
        InvocationIdContext.setInvocationId();
        String invocationId = InvocationIdContext.getInvocationId();
        assertNotEquals("", invocationId);
        // Verify the format of the invocation ID
        assertTrue(invocationId.startsWith(GCCL_INVOCATION_ID_PREFIX));
        String uuidPart = invocationId.substring(GCCL_INVOCATION_ID_PREFIX.length());
        assertEquals(36, uuidPart.length());
    }

    @Test
    public void testClearInvocationId() {
        // Set an invocation ID, clear it, and verify it is reset to an empty string
        InvocationIdContext.setInvocationId();
        InvocationIdContext.clear();
        assertEquals("", InvocationIdContext.getInvocationId());
    }

    @Test
    public void testThreadLocalIsolation() throws InterruptedException {
        // Test that the INVOCATION_ID is isolated across threads
        InvocationIdContext.setInvocationId();
        String mainThreadId = InvocationIdContext.getInvocationId();

        Thread thread = new Thread(() -> {
            InvocationIdContext.setInvocationId();
            assertNotEquals(mainThreadId, InvocationIdContext.getInvocationId());
        });
        thread.start();
        thread.join();

        // Ensure the main thread's invocation ID remains unchanged
        assertEquals(mainThreadId, InvocationIdContext.getInvocationId());
    }
}