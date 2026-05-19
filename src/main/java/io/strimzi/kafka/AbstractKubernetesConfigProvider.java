/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Abstract class for Kafka configuration providers using Kubernetes resources
 *
 * @param <T>   Resource
 * @param <L>   Resource list
 * @param <R>   Kubernetes resource
 */
abstract class AbstractKubernetesConfigProvider<T extends HasMetadata, L extends KubernetesResourceList<T>, R extends Resource<T>> implements ConfigProvider {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractKubernetesConfigProvider.class);
    private static final String SEPARATOR_CONFIG_NAME = "separator";

    // Configuration property names for retry behavior. Overridable via Kafka Connect's
    // `config.providers.<name>.param.<...>` mechanism so operators can tune without recompiling.
    static final String MAX_RETRIES_CONFIG_NAME = "max.retries";
    static final String INITIAL_BACKOFF_MS_CONFIG_NAME = "retry.backoff.ms";
    static final String MAX_BACKOFF_MS_CONFIG_NAME = "retry.backoff.max.ms";

    // Defaults: 5 attempts with 200ms..5s backoff bounds the startup latency at ~6s worst case,
    // which fits comfortably under Connect's default config-provider timeout.
    static final int DEFAULT_MAX_RETRIES = 5;
    static final long DEFAULT_INITIAL_BACKOFF_MS = 200L;
    static final long DEFAULT_MAX_BACKOFF_MS = 5_000L;

    protected final String kind;

    protected KubernetesClient client;
    private String separator = System.lineSeparator();

    private int maxRetries = DEFAULT_MAX_RETRIES;
    private long initialBackoffMs = DEFAULT_INITIAL_BACKOFF_MS;
    private long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;

    // Injectable seam for tests; production uses Thread.sleep.
    private Sleeper sleeper = Thread::sleep;

    /**
     * Creates the configuration provider
     *
     * @param kind  Kind of the Kubernetes resource handled by the provider implementation
     */
    AbstractKubernetesConfigProvider(String kind) {
        this.kind = kind;
    }

    // Abstract methods
    protected abstract MixedOperation<T, L, R> operator();

    protected abstract Map<String, String> valuesFromResource(T resource);

    // Methods from Kafka ConfigProvider
    @Override
    public void close() {
        LOG.info("Closing Kubernetes {} config provider", kind);
        client.close();
    }

    @Override
    public void configure(Map<String, ?> config) {
        LOG.info("Configuring Kubernetes {} config provider with configuration {}", kind, config);

        if (config.get(SEPARATOR_CONFIG_NAME) != null) {
            separator = (String) config.get(SEPARATOR_CONFIG_NAME);
        }

        maxRetries = parseIntConfig(config, MAX_RETRIES_CONFIG_NAME, DEFAULT_MAX_RETRIES, 1, 50);
        initialBackoffMs = parseLongConfig(config, INITIAL_BACKOFF_MS_CONFIG_NAME, DEFAULT_INITIAL_BACKOFF_MS, 1L, 60_000L);
        maxBackoffMs = parseLongConfig(config, MAX_BACKOFF_MS_CONFIG_NAME, DEFAULT_MAX_BACKOFF_MS, initialBackoffMs, 600_000L);

        client = new KubernetesClientBuilder().build();
    }

    @Override
    public ConfigData get(String path) {
        return getValues(path, null);
    }

    @Override
    public ConfigData get(String path, Set<String> keys) {
        return getValues(path, keys);
    }

    /**
     * Gets the values from the Kubernetes resource.
     *
     * @param path  Path to the Kubernetes resource
     * @param keys  Keys, which should be extracted from the resource
     *
     * @return      Kafka ConfigData with the configuration
     */
    private ConfigData getValues(String path, Set<String> keys)    {
        Map<String, String> values = valuesFromResource(getResource(path));
        Map<String, String> configs = new HashMap<>(0);

        if (keys == null)   {
            configs.putAll(values);
        } else {
            for (String key : keys) {
                PathMatcher pathMatcher = FileSystems.getDefault().getPathMatcher("glob:" + key);
                configs.put(key, values.entrySet().stream().filter(entry -> pathMatcher.matches(Paths.get(entry.getKey()))).sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.joining(separator)));
            }
        }

        return new ConfigData(configs);
    }

    // Kubernetes helper methods

    /**
     * Gets the resource from Kubernetes.
     *
     * Retries on transient API server errors (HTTP/2 GOAWAY, connection resets, 5xx server errors,
     * 429 throttling, socket timeouts) using exponential backoff with full jitter. These errors
     * commonly occur when the Kubernetes API server load-balances or gracefully terminates HTTP/2
     * connections, and they should not cause the Connect pod to crash.
     *
     * @param path  Path to the Kubernetes resource
     *
     * @return      Resource retrieved from the Kubernetes cluster
     */
    protected T getResource(String path)   {
        final KubernetesResourceIdentifier resourceIdentifier = KubernetesResourceIdentifier.fromConfigString(client, path);
        return getResourceWithRetry(resourceIdentifier,
                () -> operator().inNamespace(resourceIdentifier.getNamespace()).withName(resourceIdentifier.getName()).get());
    }

    /**
     * Core retry loop, extracted to be testable without mocking the full fabric8 client chain.
     * Tests can call this directly with a Supplier that yields a queued series of results/errors.
     *
     * @param id       Identifier of the resource being fetched (used for logging only)
     * @param fetch    Supplier that performs the actual Kubernetes API call
     * @return         The fetched resource (never null)
     */
    T getResourceWithRetry(KubernetesResourceIdentifier id, java.util.function.Supplier<T> fetch) {
        LOG.info("Retrieving configuration from {} {} in namespace {}", kind, id.getName(), id.getNamespace());

        for (int attempt = 1; ; attempt++) {
            try {
                T resource = fetch.get();

                if (resource == null)   {
                    throw new ConfigException(kind +  " " + id.getName() + " in namespace " + id.getNamespace() + " not found");
                }

                if (attempt > 1) {
                    LOG.info("Successfully retrieved {} {} from namespace {} after {} attempts",
                            kind, id.getName(), id.getNamespace(), attempt);
                }

                return resource;
            } catch (KubernetesClientException e) {
                if (shouldRetry(e, attempt)) {
                    sleepBeforeRetry(attempt, id, e);
                    continue;
                }
                throw configException(id, e);
            }
        }
    }

    /**
     * Decides whether the failed call should be retried. Returns true only if the exception is
     * classified as transient AND we have remaining retry budget.
     *
     * @param e         The exception thrown by the Kubernetes client
     * @param attempt   The just-completed attempt number (1-indexed)
     * @return          true if another attempt should be made
     */
    boolean shouldRetry(KubernetesClientException e, int attempt) {
        return attempt < maxRetries && isTransient(e);
    }

    /**
     * Classifies an exception as transient based on the structured fields of the
     * {@link KubernetesClientException} (HTTP status code) and well-known transport-layer
     * exception types in the cause chain. Message-string matching is used only as a last
     * resort for HTTP/2 GOAWAY, which has no dedicated exception type in the JDK HTTP client.
     *
     * @param e the exception thrown by the Kubernetes client
     * @return true if the error appears to be a recoverable network or server-side condition
     */
    boolean isTransient(KubernetesClientException e) {
        // 1. Structured: retryable HTTP status codes.
        int code = e.getCode();
        if (code == 408   // Request Timeout
                || code == 429   // Too Many Requests
                || code == 500   // Internal Server Error
                || code == 502   // Bad Gateway
                || code == 503   // Service Unavailable
                || code == 504) { // Gateway Timeout
            return true;
        }

        // 2. Structured: walk the cause chain for transport-level exception types.
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof SocketException
                    || cause instanceof SocketTimeoutException
                    || cause instanceof EOFException) {
                return true;
            }
            // 3. Last resort: GOAWAY is reported as a generic IOException from the JDK HTTP client.
            // We deliberately keep this narrow to avoid swallowing real errors (e.g. auth failures
            // that may also produce IOExceptions in some fabric8 code paths).
            if (cause instanceof IOException && cause.getMessage() != null
                    && cause.getMessage().contains("GOAWAY")) {
                return true;
            }
        }
        return false;
    }

    private void sleepBeforeRetry(int attempt, KubernetesResourceIdentifier id, KubernetesClientException cause) {
        // Exponential backoff with full jitter: random in [base/2, base) to avoid thundering herd.
        long base = Math.min(initialBackoffMs * (1L << Math.min(attempt - 1, 30)), maxBackoffMs);
        long backoff = ThreadLocalRandom.current().nextLong(Math.max(1L, base / 2), Math.max(2L, base));

        LOG.warn("Transient error retrieving {} {} from namespace {} (attempt {}/{}): {}. Retrying in {} ms.",
                kind, id.getName(), id.getNamespace(), attempt, maxRetries, cause.getMessage(), backoff);

        try {
            sleeper.sleep(backoff);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // Preserve the underlying transient error in the cause chain for debugging.
            // Kafka's ConfigException lacks a (message, cause) constructor, so we use initCause().
            throw (ConfigException) new ConfigException("Interrupted while retrying to retrieve "
                    + kind + " " + id.getName() + " from namespace " + id.getNamespace()).initCause(cause);
        }
    }

    private ConfigException configException(KubernetesResourceIdentifier id, KubernetesClientException e) {
        LOG.error("Failed to retrieve {} {} from Kubernetes namespace {}", kind, id.getName(), id.getNamespace(), e);
        // Kafka's ConfigException lacks a (message, cause) constructor; chain via initCause().
        return (ConfigException) new ConfigException("Failed to retrieve " + kind + " " + id.getName()
                + " from Kubernetes namespace " + id.getNamespace()).initCause(e);
    }

    private static int parseIntConfig(Map<String, ?> config, String name, int defaultValue, int min, int max) {
        Object raw = config.get(name);
        if (raw == null) {
            return defaultValue;
        }
        try {
            int v = Integer.parseInt(raw.toString());
            if (v < min || v > max) {
                LOG.warn("Config '{}' value {} is outside [{},{}], using default {}", name, v, min, max, defaultValue);
                return defaultValue;
            }
            return v;
        } catch (NumberFormatException nfe) {
            LOG.warn("Config '{}' value '{}' is not a valid integer, using default {}", name, raw, defaultValue);
            return defaultValue;
        }
    }

    private static long parseLongConfig(Map<String, ?> config, String name, long defaultValue, long min, long max) {
        Object raw = config.get(name);
        if (raw == null) {
            return defaultValue;
        }
        try {
            long v = Long.parseLong(raw.toString());
            if (v < min || v > max) {
                LOG.warn("Config '{}' value {} is outside [{},{}], using default {}", name, v, min, max, defaultValue);
                return defaultValue;
            }
            return v;
        } catch (NumberFormatException nfe) {
            LOG.warn("Config '{}' value '{}' is not a valid long, using default {}", name, raw, defaultValue);
            return defaultValue;
        }
    }

    // ─── Test-only seams ─────────────────────────────────────────────────────────────

    /**
     * Functional interface so unit tests can substitute a no-op or fake clock for Thread.sleep.
     */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    // Visible-for-testing: replace the default Thread::sleep so tests don't actually wait.
    void setSleeper(Sleeper sleeper) {
        this.sleeper = sleeper;
    }

    // Visible-for-testing accessors for retry configuration.
    int getMaxRetries() {
        return maxRetries;
    }

    long getInitialBackoffMs() {
        return initialBackoffMs;
    }

    long getMaxBackoffMs() {
        return maxBackoffMs;
    }

    /**
     * Visible-for-testing helper that applies retry-related configuration without constructing a
     * real KubernetesClient. Allows unit tests to instantiate provider subclasses with a chosen
     * retry budget while bypassing the side effects of {@link #configure(Map)}.
     */
    void configureRetryForTest(Map<String, ?> config) {
        maxRetries = parseIntConfig(config, MAX_RETRIES_CONFIG_NAME, DEFAULT_MAX_RETRIES, 1, 50);
        initialBackoffMs = parseLongConfig(config, INITIAL_BACKOFF_MS_CONFIG_NAME, DEFAULT_INITIAL_BACKOFF_MS, 1L, 60_000L);
        maxBackoffMs = parseLongConfig(config, MAX_BACKOFF_MS_CONFIG_NAME, DEFAULT_MAX_BACKOFF_MS, initialBackoffMs, 600_000L);
    }
}
