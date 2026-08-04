package io.hypercell.fsm.listener;

import io.hypercell.fsm.OrderContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class EventBusTest {

    @Test
    void empty_publishIsNoOp() {
        EventBus<OrderContext> bus = EventBus.empty();
        assertThat(bus.hasListeners()).isFalse();
        assertThatCode(() -> bus.publish(new MachineEvent.StateEnteredEvent<>("exec-1", "order", null, "PENDING")))
                .doesNotThrowAnyException();
    }

    @Test
    void publish_dispatchesToAllListenersInOrder() {
        List<String> received = new ArrayList<>();

        MachineEventListener<OrderContext> l1 = new MachineEventListener<>() {
            @Override
            public void onEvent(MachineEvent<OrderContext> e) {
                received.add("listener-1");
            }
        };
        MachineEventListener<OrderContext> l2 = new MachineEventListener<>() {
            @Override
            public void onEvent(MachineEvent<OrderContext> e) {
                received.add("listener-2");
            }
        };

        EventBus<OrderContext> bus = new EventBus<>(List.of(l1, l2));
        bus.publish(new MachineEvent.StateEnteredEvent<>("exec-1", "order", null, "PENDING"));

        assertThat(received).containsExactly("listener-1", "listener-2");
    }

    @Test
    void publish_oneListenerThrowing_doesNotStopOthers() {
        List<String> received = new ArrayList<>();

        MachineEventListener<OrderContext> throwing = new MachineEventListener<>() {
            @Override
            public void onEvent(MachineEvent<OrderContext> e) {
                throw new RuntimeException("fail");
            }
        };
        MachineEventListener<OrderContext> good = new MachineEventListener<>() {
            @Override
            public void onEvent(MachineEvent<OrderContext> e) {
                received.add("good-listener");
            }
        };

        EventBus<OrderContext> bus = new EventBus<>(List.of(throwing, good));
        assertThatCode(() ->
                bus.publish(new MachineEvent.StateEnteredEvent<>("exec-1", "order", null, "PENDING"))
        ).doesNotThrowAnyException();

        assertThat(received).containsExactly("good-listener");
    }

    @Test
    void hasListeners_trueWhenListenersPresent() {
        MachineEventListener<OrderContext> l = new MachineEventListener<>() {
        };
        EventBus<OrderContext> bus = new EventBus<>(List.of(l));
        assertThat(bus.hasListeners()).isTrue();
    }
}
