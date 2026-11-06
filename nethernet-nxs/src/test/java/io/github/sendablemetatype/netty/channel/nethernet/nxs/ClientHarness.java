package io.github.sendablemetatype.netty.channel.nethernet.nxs;

import io.github.sendablemetatype.netty.channel.nethernet.nxs.transport.ProviderTransport;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.handler.ssl.SslContext;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/** A provider stub and clients on one virtual clock, so schedules run without waiting. */
final class ClientHarness implements AutoCloseable {
    static final ProviderClient.Software SOFTWARE = new ProviderClient.Software("fixture", "1.0");

    final ManualScheduler scheduler = new ManualScheduler();
    final IndependentProviderStub stub;
    final List<ProviderDiagnostic> logs = new CopyOnWriteArrayList<>();
    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final List<ProviderClient> clients = new ArrayList<>();
    private final HttpClient http;
    private final SslContext tls;

    ClientHarness() throws Exception {
        this(null, ProviderHttp.defaultClient(), null);
    }

    ClientHarness(SslContext serverTls, HttpClient http, SslContext clientTls) throws Exception {
        stub = new IndependentProviderStub(scheduler::millis, serverTls);
        this.http = http;
        this.tls = clientTls;
    }

    ProviderClient.Configuration.Builder configuration() {
        return ProviderClient.Configuration.builder(URI.create(stub.origin)).label("Example");
    }

    ProviderClient.ConnectedPlayers connected(int count) {
        return new ProviderClient.ConnectedPlayers(count, scheduler.millis());
    }

    ProviderClient client(ProviderClient.Configuration configuration, Path state, ProviderTransport transport,
                          Supplier<ServerStatus> status, Supplier<ProviderClient.Health> health) throws Exception {
        var client = new ProviderClient(configuration, new ProviderStateStore(state), transport, status, health,
                logs::add, new ProviderClient.Environment(scheduler, scheduler, false, http, () -> group, tls));
        clients.add(client);
        return client;
    }

    /** A client with a healthy default and no listing. */
    ProviderClient client(Path state, ProviderTransport transport) throws Exception {
        return client(configuration().build(), state, transport, () -> null,
                () -> new ProviderClient.Health(true, 20, SOFTWARE, connected(0)));
    }

    <T> T await(CompletionStage<T> stage) throws Exception {
        return scheduler.await(stage);
    }

    Throwable failure(CompletionStage<?> stage) throws Exception {
        return scheduler.failure(stage);
    }

    void advance(Duration duration) {
        scheduler.advance(duration);
    }

    /** Lets the clients' ticks run for a while of virtual time, a second at a time, each to completion. */
    void run(Duration duration) throws InterruptedException {
        for (long second = 0; second < duration.toSeconds(); second++) {
            scheduler.advance(Duration.ofSeconds(1));
            settle();
        }
    }

    /** Runs what the clients started, loopback replies included, until none has an operation left. */
    void settle() throws InterruptedException {
        scheduler.settle(() -> clients.stream().allMatch(ProviderClient::idle));
    }

    void until(BooleanSupplier condition) throws InterruptedException {
        scheduler.until(condition);
    }

    void stop(ProviderClient client) throws Exception {
        await(client.stop());
    }

    List<ProviderDiagnostic> logged(ProviderDiagnostic.Level level, String fragment) {
        return logs.stream().filter(d -> d.level() == level && d.message().contains(fragment)).toList();
    }

    @Override
    public void close() throws Exception {
        for (ProviderClient client : clients) {
            if (!client.stop().toCompletableFuture().isDone()) {
                scheduler.await(client.stop());
            }
        }
        stub.close();
        group.shutdownGracefully(0, 1, java.util.concurrent.TimeUnit.SECONDS).syncUninterruptibly();
    }
}
