package io.github.sendablemetatype.netty.channel.nethernet;

import io.github.sendablemetatype.netty.channel.nethernet.engine.ChannelParameters;
import io.github.sendablemetatype.netty.channel.nethernet.engine.DataLink;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(10)
class DataChannelSlotsTest {
    private static final ChannelParameters RELIABLE = ChannelParameters.reliable(NetherNetConstants.RELIABLE_CHANNEL_LABEL);
    private static final ChannelParameters UNRELIABLE = ChannelParameters.unreliable(NetherNetConstants.UNRELIABLE_CHANNEL_LABEL);

    @Test
    void bothExpectedChannelsAreAdmitted() {
        DataChannelSlots slots = new DataChannelSlots();
        Link reliable = new Link(RELIABLE);
        Link unreliable = new Link(UNRELIABLE);

        assertTrue(slots.admit(unreliable));
        assertFalse(slots.complete());
        assertTrue(slots.admit(reliable));

        assertSame(reliable, slots.reliable());
        assertSame(unreliable, slots.unreliable());
        assertTrue(slots.complete());
    }

    @Test
    void theSubprotocolIsNotChecked() {
        DataChannelSlots slots = new DataChannelSlots();
        assertTrue(slots.admit(new Link(new ChannelParameters(RELIABLE.label(), true,
                OptionalInt.empty(), OptionalInt.empty(), false, "older-client"))));
    }

    @Test
    void anExplicitZeroLifetimeCountsAsUnreliable() {
        DataChannelSlots slots = new DataChannelSlots();
        assertTrue(slots.admit(new Link(new ChannelParameters(UNRELIABLE.label(), false,
                OptionalInt.empty(), OptionalInt.of(0), false, ""))));
    }

    @ParameterizedTest
    @MethodSource("invalidParameters")
    void aMalformedExtraDoesNotReplaceOrBlockThePendingHandshake(ChannelParameters parameters) {
        DataChannelSlots slots = new DataChannelSlots();
        Link reliable = new Link(RELIABLE);
        Link unreliable = new Link(UNRELIABLE);
        assertTrue(slots.admit(reliable));

        assertFalse(slots.admit(new Link(parameters)));

        assertSame(reliable, slots.reliable());
        assertNull(slots.unreliable());
        assertTrue(slots.admit(unreliable));
        assertSame(unreliable, slots.unreliable());
    }

    private static Stream<ChannelParameters> invalidParameters() {
        OptionalInt none = OptionalInt.empty();
        return Stream.of(
                new ChannelParameters("unexpected", true, none, none, false, ""),
                new ChannelParameters(RELIABLE.label(), false, none, none, false, ""),
                new ChannelParameters(RELIABLE.label(), true, OptionalInt.of(0), none, false, ""),
                new ChannelParameters(RELIABLE.label(), true, none, none, true, ""),
                new ChannelParameters(RELIABLE.label(), true, none, OptionalInt.of(1), false, ""),
                new ChannelParameters(RELIABLE.label(), true, OptionalInt.of(1), none, false, ""),
                new ChannelParameters(UNRELIABLE.label(), true, OptionalInt.of(0), none, false, ""),
                new ChannelParameters(UNRELIABLE.label(), false, none, none, false, ""),
                new ChannelParameters(UNRELIABLE.label(), false, OptionalInt.of(0), OptionalInt.of(1), false, ""),
                new ChannelParameters(UNRELIABLE.label(), false, OptionalInt.of(1), none, false, ""),
                new ChannelParameters(UNRELIABLE.label(), false, OptionalInt.of(0), none, true, ""));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void aDuplicateLabelCannotRedirectAnEstablishedStream(boolean replaceReliable) {
        DataChannelSlots slots = new DataChannelSlots();
        Link reliable = new Link(RELIABLE);
        Link unreliable = new Link(UNRELIABLE);
        assertTrue(slots.admit(reliable));
        assertTrue(slots.admit(unreliable));

        assertFalse(slots.admit(new Link(replaceReliable ? RELIABLE : UNRELIABLE)));

        assertSame(reliable, slots.reliable());
        assertSame(unreliable, slots.unreliable());
    }

    @Test
    void aRepeatedAnnouncementOfTheAdmittedLinkKeepsIt() {
        DataChannelSlots slots = new DataChannelSlots();
        Link link = new Link(RELIABLE);
        assertTrue(slots.admit(link));

        assertTrue(slots.admit(link));

        assertSame(link, slots.reliable());
    }

    @Test
    void channelsArrivingAfterClosureAreRefused() {
        DataChannelSlots slots = new DataChannelSlots();
        slots.stopAccepting();

        assertFalse(slots.admit(new Link(RELIABLE)));
        assertFalse(slots.admit(new Link(UNRELIABLE)));

        assertNull(slots.reliable());
        assertNull(slots.unreliable());
    }

    @Test
    void racingDuplicateChannelsHaveExactlyOneOwner() throws Exception {
        DataChannelSlots slots = new DataChannelSlots();
        Link first = new Link(RELIABLE);
        Link second = new Link(RELIABLE);
        CountDownLatch start = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2);
        try {
            var firstAdmission = executor.submit(() -> {
                start.await();
                return slots.admit(first);
            });
            var secondAdmission = executor.submit(() -> {
                start.await();
                return slots.admit(second);
            });
            start.countDown();

            boolean acceptedFirst = firstAdmission.get(2, TimeUnit.SECONDS);
            boolean acceptedSecond = secondAdmission.get(2, TimeUnit.SECONDS);

            assertTrue(acceptedFirst ^ acceptedSecond);
            assertSame(acceptedFirst ? first : second, slots.reliable());
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    private static final class Link implements DataLink {
        private final ChannelParameters parameters;

        private Link(ChannelParameters parameters) {
            this.parameters = parameters;
        }

        @Override
        public ChannelParameters parameters() {
            return parameters;
        }

        @Override
        public void send(ByteBuffer message, Consumer<Throwable> completion) {
            completion.accept(null);
        }

        @Override
        public long queuedBytes() {
            return 0;
        }

        @Override
        public void lowWatermark(long bytes) {
        }
    }
}
