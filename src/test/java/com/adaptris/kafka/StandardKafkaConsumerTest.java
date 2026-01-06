package com.adaptris.kafka;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.time.Duration;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.adaptris.core.AdaptrisMessage;

class StandardKafkaConsumerTest {

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void injectConsumer(StandardKafkaConsumer target, KafkaConsumer mock) throws Exception {
        Field f = StandardKafkaConsumer.class.getDeclaredField("consumer");
        f.setAccessible(true);
        f.set(target, mock);
    }

    private Runnable createRunnable(StandardKafkaConsumer target) throws Exception {
        Class<?> inner = Class.forName("com.adaptris.kafka.StandardKafkaConsumer$MessageConsumerRunnable");
        Constructor<?> ctor = inner.getDeclaredConstructor(StandardKafkaConsumer.class);
        ctor.setAccessible(true);
        return (Runnable) ctor.newInstance(target);
    }

    @Test
    void testWakeupExceptionIsHandledAndBreaksLoop() throws Exception {
        StandardKafkaConsumer target = new StandardKafkaConsumer();
        KafkaConsumer<String, AdaptrisMessage> mockConsumer = Mockito.mock(KafkaConsumer.class);
        Mockito.when(mockConsumer.poll(Mockito.any(Duration.class))).thenThrow(new WakeupException());

        injectConsumer(target, mockConsumer);

        Runnable r = createRunnable(target);
        // Should not throw and should call poll once
        r.run();

        Mockito.verify(mockConsumer).poll(Mockito.any(Duration.class));
    }

    @Test
    void testAuthorizationExceptionIsLoggedAndSwallowed() throws Exception {
        StandardKafkaConsumer target = new StandardKafkaConsumer();
        KafkaConsumer<String, AdaptrisMessage> mockConsumer = Mockito.mock(KafkaConsumer.class);
        Mockito.when(mockConsumer.poll(Mockito.any(Duration.class))).thenThrow(new AuthorizationException(""));

        injectConsumer(target, mockConsumer);

        Runnable r = createRunnable(target);
        // Should not propagate the exception
        r.run();

        Mockito.verify(mockConsumer).poll(Mockito.any(Duration.class));
    }

    @Test
    void testGenericExceptionIsSwallowed() throws Exception {
        StandardKafkaConsumer target = new StandardKafkaConsumer();
        KafkaConsumer<String, AdaptrisMessage> mockConsumer = Mockito.mock(KafkaConsumer.class);
        Mockito.when(mockConsumer.poll(Mockito.any(Duration.class))).thenThrow(new RuntimeException("boom"));

        injectConsumer(target, mockConsumer);

        Runnable r = createRunnable(target);
        // Should not propagate the exception
        r.run();

        Mockito.verify(mockConsumer).poll(Mockito.any(Duration.class));
    }
}
