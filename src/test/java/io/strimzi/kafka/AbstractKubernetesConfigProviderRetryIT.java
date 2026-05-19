/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.ConfigMapList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end retry IT against a real Kubernetes cluster.
 *
 * Sits a TCP-forwarding proxy in front of the apiserver. The proxy kills the first N connections
 * with a TCP RST (which fabric8 surfaces as {@link java.net.SocketException}, classified as
 * transient by the production code), then forwards transparently. We then make a single provider
 * call through the production retry loop and verify it actually recovers — using a real
 * fabric8 client, real network, and real TLS to a real apiserver.
 *
 * This complements the unit tests by exercising the parts they can't: real fabric8 client
 * wiring, real exception types produced by real network failures, and real connection re-use
 * behavior across retries.
 */
public class AbstractKubernetesConfigProviderRetryIT {

    private static final String RESOURCE_NAME = "retry-it-cm";

    private static KubernetesClient realClient;
    private static String namespace;
    private static URI masterUri;

    // Per-test state: each test creates a fresh proxy and fresh fabric8 client so that
    // HTTP/1.1 keepalive from a previous test cannot mask the next test's failure injection.
    private FailingTcpProxy proxy;
    private KubernetesClient proxiedClient;

    @BeforeAll
    public static void beforeAll() {
        realClient = new KubernetesClientBuilder().build();
        namespace = realClient.getNamespace();
        masterUri = URI.create(realClient.getConfiguration().getMasterUrl());

        ConfigMap cm = new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withNamespace(namespace)
                .endMetadata()
                .addToData("hello", "world")
                .build();
        realClient.configMaps().resource(cm).create();
    }

    @AfterAll
    public static void afterAll() {
        if (realClient != null) {
            try {
                realClient.configMaps().inNamespace(namespace).withName(RESOURCE_NAME).delete();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
            realClient.close();
        }
    }

    @BeforeEach
    public void beforeEach() throws IOException {
        int targetPort = masterUri.getPort() > 0 ? masterUri.getPort() : 443;
        proxy = new FailingTcpProxy(masterUri.getHost(), targetPort);
        proxy.start();

        // autoConfigure() preserves the kubeconfig client cert/key so TLS client-auth still
        // works against the real apiserver; we just bypass hostname + CA validation since the
        // bytes are tunnelled through a localhost endpoint. http2Disable=true forces HTTP/1.1,
        // so each retry from the production loop opens (or attempts to open) a new TCP
        // connection — making each retry observable at the proxy's connection counter.
        Config base = Config.autoConfigure(null);
        Config cfg = new ConfigBuilder(base)
                .withMasterUrl("https://localhost:" + proxy.getPort())
                .withTrustCerts(true)
                .withDisableHostnameVerification(true)
                .withHttp2Disable(true)
                .withRequestTimeout(5_000)
                .build();
        proxiedClient = new KubernetesClientBuilder().withConfig(cfg).build();
    }

    @AfterEach
    public void afterEach() throws IOException {
        if (proxiedClient != null) {
            proxiedClient.close();
        }
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    public void retryLoopRecoversFromRealNetworkFailures() {
        proxy.resetCounter();
        proxy.setFailCount(3);

        TestConfigMapProvider provider = new TestConfigMapProvider(proxiedClient);
        provider.configureRetryForTest(Map.of(
                AbstractKubernetesConfigProvider.MAX_RETRIES_CONFIG_NAME, "15",
                AbstractKubernetesConfigProvider.INITIAL_BACKOFF_MS_CONFIG_NAME, "10",
                AbstractKubernetesConfigProvider.MAX_BACKOFF_MS_CONFIG_NAME, "50"
        ));
        provider.setSleeper(ms -> { /* no actual sleep — keep the test fast */ });

        KubernetesResourceIdentifier id = new KubernetesResourceIdentifier(namespace, RESOURCE_NAME);

        ConfigMap got = provider.getResourceWithRetry(id,
                () -> proxiedClient.configMaps().inNamespace(namespace).withName(RESOURCE_NAME).get());

        assertNotNull(got, "Provider should recover after transient failures");
        assertEquals("world", got.getData().get("hello"),
                "Recovered ConfigMap should carry the expected data");
        assertTrue(proxy.getConnectionCount() > 3,
                "Proxy should have seen more than 3 connections (failures + at least one success); got "
                        + proxy.getConnectionCount());
    }

    @Test
    public void exhaustingRetriesAgainstRealNetworkThrowsWithCause() {
        proxy.resetCounter();
        proxy.setFailCount(Integer.MAX_VALUE);

        TestConfigMapProvider provider = new TestConfigMapProvider(proxiedClient);
        provider.configureRetryForTest(Map.of(
                AbstractKubernetesConfigProvider.MAX_RETRIES_CONFIG_NAME, "2",
                AbstractKubernetesConfigProvider.INITIAL_BACKOFF_MS_CONFIG_NAME, "10",
                AbstractKubernetesConfigProvider.MAX_BACKOFF_MS_CONFIG_NAME, "50"
        ));
        provider.setSleeper(ms -> { });

        KubernetesResourceIdentifier id = new KubernetesResourceIdentifier(namespace, RESOURCE_NAME);

        ConfigException ce = assertThrows(ConfigException.class,
                () -> provider.getResourceWithRetry(id,
                        () -> proxiedClient.configMaps().inNamespace(namespace).withName(RESOURCE_NAME).get()));

        assertNotNull(ce.getCause(), "Real KubernetesClientException must be preserved as cause");
        assertTrue(ce.getCause() instanceof KubernetesClientException,
                "Cause should be the actual fabric8 exception, got: " + ce.getCause().getClass());
    }

    /** Concrete provider subclass that uses a caller-provided KubernetesClient. */
    static final class TestConfigMapProvider extends AbstractKubernetesConfigProvider<ConfigMap, ConfigMapList, Resource<ConfigMap>> {
        TestConfigMapProvider(KubernetesClient client) {
            super("ConfigMap");
            this.client = client;
        }

        @Override
        protected MixedOperation<ConfigMap, ConfigMapList, Resource<ConfigMap>> operator() {
            return client.configMaps();
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

    /**
     * TCP proxy that forwards bytes between a client and a target host:port, but kills the first
     * {@code failCount} accepted connections with a TCP RST. Counted at TCP accept time, so each
     * retry from the production loop (with HTTP/2 disabled) maps to one new connection here.
     */
    static final class FailingTcpProxy implements Closeable {
        private final String targetHost;
        private final int targetPort;
        private final ServerSocket serverSocket;
        private final Thread acceptThread;
        private final AtomicInteger connectionCount = new AtomicInteger();
        private volatile int failCount = 0;
        private volatile boolean running = true;

        FailingTcpProxy(String targetHost, int targetPort) throws IOException {
            this.targetHost = targetHost;
            this.targetPort = targetPort;
            this.serverSocket = new ServerSocket(0);
            this.acceptThread = new Thread(this::acceptLoop, "FailingTcpProxy-accept");
            this.acceptThread.setDaemon(true);
        }

        void start() {
            acceptThread.start();
        }

        int getPort() {
            return serverSocket.getLocalPort();
        }

        int getConnectionCount() {
            return connectionCount.get();
        }

        void setFailCount(int f) {
            this.failCount = f;
        }

        void resetCounter() {
            this.connectionCount.set(0);
        }

        private void acceptLoop() {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    int n = connectionCount.incrementAndGet();
                    if (n <= failCount) {
                        // SO_LINGER 0 → close sends TCP RST. Client side sees SocketException
                        // ("Connection reset"), which our classifier treats as transient.
                        try {
                            client.setSoLinger(true, 0);
                        } catch (Exception ignored) {
                            // some platforms reject SO_LINGER 0; fall through and just close
                        }
                        try {
                            client.close();
                        } catch (IOException ignored) {
                            // already torn down
                        }
                    } else {
                        Socket upstream = new Socket(targetHost, targetPort);
                        Thread t1 = new Thread(() -> pipe(client, upstream), "FailingTcpProxy-c2u");
                        Thread t2 = new Thread(() -> pipe(upstream, client), "FailingTcpProxy-u2c");
                        t1.setDaemon(true);
                        t2.setDaemon(true);
                        t1.start();
                        t2.start();
                    }
                } catch (IOException e) {
                    if (running) {
                        System.err.println("FailingTcpProxy accept error: " + e);
                    }
                }
            }
        }

        private static void pipe(Socket inSock, Socket outSock) {
            try {
                InputStream in = inSock.getInputStream();
                OutputStream out = outSock.getOutputStream();
                byte[] buf = new byte[8192];
                int n;
                while ((n = in.read(buf)) > 0) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            } catch (IOException ignored) {
                // expected on connection close
            } finally {
                try {
                    inSock.close();
                } catch (IOException ignored) {
                    // already closed
                }
                try {
                    outSock.close();
                } catch (IOException ignored) {
                    // already closed
                }
            }
        }

        @Override
        public void close() throws IOException {
            running = false;
            serverSocket.close();
        }
    }
}
