/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests covering the retry / transient-error classification behavior added to
 * {@link AbstractKubernetesConfigProvider}. Tests use a fake {@link AbstractKubernetesConfigProvider.Sleeper}
 * so the suite runs in milliseconds, and call the retry loop directly via a Supplier to avoid
 * mocking the full fabric8 client chain.
 */
class AbstractKubernetesConfigProviderRetryTest {

    private TestProvider provider;
    private AtomicInteger sleepCount;
    private long totalSleptMs;

    @BeforeEach
    void setUp() {
        provider = new TestProvider();
        provider.configureRetryForTest(Map.of(
                AbstractKubernetesConfigProvider.MAX_RETRIES_CONFIG_NAME, "3",
                AbstractKubernetesConfigProvider.INITIAL_BACKOFF_MS_CONFIG_NAME, "10",
                AbstractKubernetesConfigProvider.MAX_BACKOFF_MS_CONFIG_NAME, "100"
        ));
        sleepCount = new AtomicInteger();
        totalSleptMs = 0;
        provider.setSleeper(ms -> {
            sleepCount.incrementAndGet();
            totalSleptMs += ms;
        });
    }

    // ─── isTransient classification ──────────────────────────────────────────────────

    @Test
    void retryableHttpCodesAreTransient() {
        for (int code : new int[]{408, 429, 500, 502, 503, 504}) {
            assertTrue(provider.isTransient(new KubernetesClientException("boom", code, null)),
                    "HTTP " + code + " should be transient");
        }
    }

    @Test
    void permanentHttpCodesAreNotTransient() {
        for (int code : new int[]{400, 401, 403, 404, 409, 422}) {
            assertFalse(provider.isTransient(new KubernetesClientException("boom", code, null)),
                    "HTTP " + code + " should be permanent");
        }
    }

    @Test
    void goawayInCauseChainIsTransient() {
        KubernetesClientException e = new KubernetesClientException("wrap",
                new IOException("/10.0.0.1:8443: GOAWAY received"));
        assertTrue(provider.isTransient(e));
    }

    @Test
    void socketTimeoutInCauseChainIsTransient() {
        KubernetesClientException e = new KubernetesClientException("wrap",
                new SocketTimeoutException("read timed out"));
        assertTrue(provider.isTransient(e));
    }

    @Test
    void eofExceptionInCauseChainIsTransient() {
        KubernetesClientException e = new KubernetesClientException("wrap",
                new EOFException("unexpected end of stream"));
        assertTrue(provider.isTransient(e));
    }

    @Test
    void plainIOExceptionWithoutGoawayIsNotTransient() {
        // Deliberately narrow: we don't retry on every IOException because auth failures and
        // other real errors also surface as IOExceptions in some fabric8 code paths.
        KubernetesClientException e = new KubernetesClientException("wrap",
                new IOException("authentication failed"));
        assertFalse(provider.isTransient(e));
    }

    @Test
    void unrelatedExceptionIsNotTransient() {
        assertFalse(provider.isTransient(new KubernetesClientException("bad config")));
    }

    // ─── retry loop behavior ─────────────────────────────────────────────────────────

    @Test
    void returnsImmediatelyOnSuccessWithoutSleeping() {
        Queued queued = new Queued().enqueue(configMap());

        ConfigMap got = provider.getResourceWithRetry(id(), queued);

        assertNotNull(got);
        assertEquals(0, sleepCount.get(), "Should not sleep on first-try success");
    }

    @Test
    void retriesTransientErrorThenSucceeds() {
        ConfigMap cm = configMap();
        Queued queued = new Queued()
                .enqueue(transientGoaway())
                .enqueue(transient503())
                .enqueue(cm);

        ConfigMap got = provider.getResourceWithRetry(id(), queued);

        assertSame(cm, got);
        assertEquals(2, sleepCount.get(), "Two retries should mean two sleeps");
        assertTrue(totalSleptMs >= 10, "Backoff should have elapsed at least one initial interval");
    }

    @Test
    void exhaustsRetriesThenThrowsConfigExceptionPreservingCause() {
        KubernetesClientException last = transient503();
        Queued queued = new Queued()
                .enqueue(transientGoaway())
                .enqueue(transient503())
                .enqueue(last);

        ConfigException ce = assertThrows(ConfigException.class,
                () -> provider.getResourceWithRetry(id(), queued));

        // maxRetries=3 → attempts 1,2 sleep; attempt 3 fails and is not retried.
        assertEquals(2, sleepCount.get(), "Should sleep maxRetries-1 times");
        assertNotNull(ce.getCause(), "Cause must be preserved for debugging");
        assertSame(last, ce.getCause());
    }

    @Test
    void doesNotRetryOnPermanentError() {
        KubernetesClientException notFound = new KubernetesClientException("not found", 404, null);
        Queued queued = new Queued().enqueue(notFound);

        ConfigException ce = assertThrows(ConfigException.class,
                () -> provider.getResourceWithRetry(id(), queued));

        assertEquals(0, sleepCount.get(), "Permanent errors must not be retried");
        assertSame(notFound, ce.getCause());
    }

    @Test
    void nullResourceTreatedAsNotFoundWithoutRetry() {
        Queued queued = new Queued().enqueue((ConfigMap) null);

        ConfigException ce = assertThrows(ConfigException.class,
                () -> provider.getResourceWithRetry(id(), queued));

        assertEquals(0, sleepCount.get(), "Null (not-found) is not retried");
        assertTrue(ce.getMessage().contains("not found"));
    }

    @Test
    void backoffIsBoundedByMax() {
        TestProvider tp = new TestProvider();
        tp.configureRetryForTest(Map.of(
                AbstractKubernetesConfigProvider.MAX_RETRIES_CONFIG_NAME, "20",
                AbstractKubernetesConfigProvider.INITIAL_BACKOFF_MS_CONFIG_NAME, "1",
                AbstractKubernetesConfigProvider.MAX_BACKOFF_MS_CONFIG_NAME, "50"
        ));
        long[] observed = new long[19];
        AtomicInteger idx = new AtomicInteger();
        tp.setSleeper(ms -> observed[idx.getAndIncrement()] = ms);

        Queued queued = new Queued();
        for (int i = 0; i < 20; i++) {
            queued.enqueue(transientGoaway());
        }

        assertThrows(ConfigException.class, () -> tp.getResourceWithRetry(id(), queued));

        for (long ms : observed) {
            assertTrue(ms >= 1 && ms <= 50, "Sleep " + ms + "ms must be within [1, 50]");
        }
    }

    @Test
    void interruptDuringBackoffPreservesCauseAndReinterruptsThread() {
        provider.setSleeper(ms -> {
            throw new InterruptedException("test interrupt");
        });
        KubernetesClientException transient1 = transientGoaway();
        Queued queued = new Queued().enqueue(transient1).enqueue(configMap());

        ConfigException ce = assertThrows(ConfigException.class,
                () -> provider.getResourceWithRetry(id(), queued));

        assertTrue(Thread.interrupted(), "Thread interrupt status must be restored");
        assertSame(transient1, ce.getCause(),
                "Underlying transient error should be preserved when interrupted");
    }

    @Test
    void invalidRetryConfigFallsBackToDefaults() {
        TestProvider tp = new TestProvider();
        tp.configureRetryForTest(Map.of(
                AbstractKubernetesConfigProvider.MAX_RETRIES_CONFIG_NAME, "not-a-number",
                AbstractKubernetesConfigProvider.INITIAL_BACKOFF_MS_CONFIG_NAME, "-5"
        ));

        assertEquals(AbstractKubernetesConfigProvider.DEFAULT_MAX_RETRIES, tp.getMaxRetries());
        assertEquals(AbstractKubernetesConfigProvider.DEFAULT_INITIAL_BACKOFF_MS, tp.getInitialBackoffMs());
    }

    // ─── helpers ────────────────────────────────────────────────────────────────────

    private static ConfigMap configMap() {
        return new ConfigMapBuilder()
                .withNewMetadata().withName("foo").withNamespace("kafka").endMetadata()
                .addToData("key", "value")
                .build();
    }

    private static KubernetesResourceIdentifier id() {
        return new KubernetesResourceIdentifier("foo", "kafka");
    }

    private static KubernetesClientException transientGoaway() {
        return new KubernetesClientException("transport failure",
                new IOException("/10.0.0.1:8443: GOAWAY received"));
    }

    private static KubernetesClientException transient503() {
        return new KubernetesClientException("service unavailable", 503, null);
    }

    /**
     * Queueable supplier: each call to get() returns or throws the next enqueued item.
     */
    private static final class Queued implements Supplier<ConfigMap> {
        private final Deque<Object> items = new ArrayDeque<>();
        private final Object nullSentinel = new Object();

        Queued enqueue(ConfigMap cm) {
            items.add(cm == null ? nullSentinel : cm);
            return this;
        }

        Queued enqueue(KubernetesClientException e) {
            items.add(e);
            return this;
        }

        @Override
        public ConfigMap get() {
            Object next = items.poll();
            if (next == null) {
                throw new IllegalStateException("Queued supplier exhausted");
            }
            if (next == nullSentinel) {
                return null;
            }
            if (next instanceof KubernetesClientException) {
                throw (KubernetesClientException) next;
            }
            return (ConfigMap) next;
        }
    }

    /**
     * Minimal concrete subclass — we never actually call operator() in these tests because
     * the suite goes through getResourceWithRetry() with its own Supplier, bypassing the
     * fabric8 client chain entirely.
     */
    static final class TestProvider extends AbstractKubernetesConfigProvider<ConfigMap, ConfigMapList, Resource<ConfigMap>> {
        TestProvider() {
            super("ConfigMap");
        }

        @Override
        protected MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>> operator() {
            throw new UnsupportedOperationException("operator() should not be invoked in unit tests");
        }

        @Override
        protected Map<String, String> valuesFromResource(ConfigMap resource) {
            Map<String, String> out = new HashMap<>();
            if (resource.getData() != null) {
                out.putAll(resource.getData());
            }
            return out;
        }
    }
}
