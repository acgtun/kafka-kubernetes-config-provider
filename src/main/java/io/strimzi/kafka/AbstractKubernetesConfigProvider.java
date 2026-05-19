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
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
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
    //
    // Semantics: `max.retries = N` means N retries AFTER the initial attempt, for a total of
    // N+1 attempts. With the defaults (N=5, 200ms..5s backoff) the worst-case startup latency
    // is the sum of 5 sleeps capped at 5s ≈ 200+400+800+1600+3200 ≈ 6.2s, which fits under
    // Kafka Connect's default config-provider timeout.
    static final String MAX_RETRIES_CONFIG_NAME = "max.retries";
    static final String INITIAL_BACKOFF_MS_CONFIG_NAME = "retry.backoff.ms";
    static final String MAX_BACKOFF_MS_CONFIG_NAME = "retry.backoff.max.ms";

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

        parseRetryConfig(config);

        client = new KubernetesClientBuilder().build();
    }

    /**
     * Parses and validates the retry-related configuration keys. Single source of truth for
     * both production ({@link #configure(Map)}) and the test seam ({@link #configureRetryForTest(Map)}),
     * so future changes don't drift between the two.
     */
    private void parseRetryConfig(Map<String, ?> config) {
        maxRetries = parseIntConfig(config, MAX_RETRIES_CONFIG_NAME, DEFAULT_MAX_RETRIES, 1, 50);
        initialBackoffMs = parseLongConfig(config, INITIAL_BACKOFF_MS_CONFIG_NAME, DEFAULT_INITIAL_BACKOFF_MS, 1L, 60_000L);
        // Parse max independently of initial so we can give a clearer error message when the
        // user sets max < initial (parseLongConfig would otherwise log a generic "outside [min,max]").
        long parsedMax = parseLongConfig(config, MAX_BACKOFF_MS_CONFIG_NAME, DEFAULT_MAX_BACKOFF_MS, 1L, 600_000L);
        if (parsedMax < initialBackoffMs) {
            LOG.warn("Config '{}' value {} is less than '{}' value {}; using default {} for the max backoff.",
                    MAX_BACKOFF_MS_CONFIG_NAME, parsedMax,
                    INITIAL_BACKOFF_MS_CONFIG_NAME, initialBackoffMs,
                    DEFAULT_MAX_BACKOFF_MS);
            maxBackoffMs = Math.max(DEFAULT_MAX_BACKOFF_MS, initialBackoffMs);
        } else {
            maxBackoffMs = parsedMax;
        }
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
     * `maxRetries` is the number of retries AFTER the initial attempt, so we allow another
     * attempt whenever `attempt <= maxRetries` (i.e. up to `maxRetries + 1` total attempts).
     *
     * @param e         The exception thrown by the Kubernetes client
     * @param attempt   The just-completed attempt number (1-indexed)
     * @return          true if another attempt should be made
     */
    boolean shouldRetry(KubernetesClientException e, int attempt) {
        return attempt <= maxRetries && isTransient(e);
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
        if (isRetryableHttpCode(e.getCode())) {
            return true;
        }
        // Walk the cause chain for transport-level exception types or a GOAWAY message.
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (isTransientCause(cause)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Retryable HTTP status codes: request timeout, throttling, and the 5xx server-side errors
     * we know to be transient.
     */
    private static boolean isRetryableHttpCode(int code) {
        switch (code) {
            case 408: // Request Timeout
            case 429: // Too Many Requests
            case 500: // Internal Server Error
            case 502: // Bad Gateway
            case 503: // Service Unavailable
            case 504: // Gateway Timeout
                return true;
            default:
                return false;
        }
    }

    /**
     * Classifies a single Throwable in the cause chain. SocketException covers ConnectException
     * and friends; UnknownHostException covers DNS hiccups during apiserver / load-balancer
     * failover. GOAWAY is matched only on IOException to stay narrow — auth failures and other
     * permanent errors also surface as IOException in some fabric8 code paths.
     */
    private static boolean isTransientCause(Throwable cause) {
        if (cause instanceof SocketException
                || cause instanceof SocketTimeoutException
                || cause instanceof UnknownHostException
                || cause instanceof EOFException) {
            return true;
        }
        // Case-insensitive match in case the JDK ever rewords the message.
        return cause instanceof IOException
                && cause.getMessage() != null
                && cause.getMessage().toUpperCase(Locale.ROOT).contains("GOAWAY");
    }

    private void sleepBeforeRetry(int attempt, KubernetesResourceIdentifier id, KubernetesClientException cause) {
        // Exponential backoff with full jitter: random in [base/2, base) to avoid thundering herd.
        // Cap the left shift at 30 to keep the multiplier inside positive long range even if a
        // future change raises the maxRetries upper bound above 31.
        long base = Math.min(initialBackoffMs * (1L << Math.min(attempt - 1, 30)), maxBackoffMs);
        long backoff = ThreadLocalRandom.current().nextLong(Math.max(1L, base / 2), Math.max(2L, base));

        LOG.warn("Transient error retrieving {} {} from namespace {} (attempt {}/{}): {}. Retrying in {} ms.",
                kind, id.getName(), id.getNamespace(), attempt, maxRetries + 1, cause.getMessage(), backoff);

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
     * real KubernetesClient. Delegates to {@link #parseRetryConfig(Map)} so the parsing logic
     * stays in a single place.
     */
    void configureRetryForTest(Map<String, ?> config) {
        parseRetryConfig(config);
    }
}
