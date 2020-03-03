package fi.hsl.common.pulsar;

import com.typesafe.config.*;
import fi.hsl.common.health.*;
import org.apache.pulsar.client.admin.*;
import org.apache.pulsar.client.api.*;
import redis.clients.jedis.*;

public class PulsarApplicationContext {

    private Config config;

    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;
    private PulsarClient client;
    private PulsarAdmin admin;
    private Jedis jedis;
    private HealthServer healthServer;

    public Config getConfig() {
        return config;
    }

    void setConfig(Config config) {
        this.config = config;
    }

    public Consumer<byte[]> getConsumer() {
        return consumer;
    }

    void setConsumer(Consumer<byte[]> consumer) {
        this.consumer = consumer;
    }

    public Producer<byte[]> getProducer() {
        return producer;
    }

    void setProducer(Producer<byte[]> producer) {
        this.producer = producer;
    }

    public PulsarClient getClient() {
        return client;
    }

    void setClient(PulsarClient client) {
        this.client = client;
    }

    public Jedis getJedis() {
        return jedis;
    }

    void setJedis(Jedis jedis) {
        this.jedis = jedis;
    }

    public PulsarAdmin getAdmin() {
        return admin;
    }

    void setAdmin(PulsarAdmin admin) {
        this.admin = admin;
    }

    public HealthServer getHealthServer() {
        return healthServer;
    }

    void setHealthServer(HealthServer healthServer) {
        this.healthServer = healthServer;
    }
}
