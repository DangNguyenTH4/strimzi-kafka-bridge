/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */

package io.strimzi.kafka.bridge;

import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.config.KafkaConfig;
import io.vertx.core.*;
import io.vertx.core.impl.ContextInternal;
import io.vertx.kafka.admin.ConfigEntry;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import io.vertx.kafka.admin.TopicDescription;
import io.vertx.kafka.admin.Config;
import io.vertx.kafka.admin.OffsetSpec;
import io.vertx.kafka.admin.ListOffsetsResultInfo;
import io.vertx.kafka.client.common.ConfigResource;
import io.vertx.kafka.client.common.TopicPartition;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.util.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * Base class for admin client endpoint
 */
public abstract class AdminClientEndpoint implements BridgeEndpoint {
    protected final Logger log = LoggerFactory.getLogger(AdminClientEndpoint.class);

    protected String name;
    protected final Vertx vertx;
    protected final BridgeConfig bridgeConfig;
    protected SinkBridgeEndpoint sinkBridgeEndpoint;

    private Handler<BridgeEndpoint> closeHandler;

    private KafkaAdminClient adminClient;

    /**
     * Constructor
     *
     * @param vertx Vert.x instance
     * @param bridgeConfig Bridge configuration
     */
    public AdminClientEndpoint(Vertx vertx, BridgeConfig bridgeConfig) {
        this.vertx = vertx;
        this.name = "kafka-bridge-admin";
        this.bridgeConfig = bridgeConfig;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public BridgeEndpoint closeHandler(Handler<BridgeEndpoint> endpointCloseHandler) {
        this.closeHandler = endpointCloseHandler;
        return this;
    }

    @Override
    public void open() {
        // create an admin client
        KafkaConfig kafkaConfig = this.bridgeConfig.getKafkaConfig();
        Properties props = new Properties();
        props.putAll(kafkaConfig.getConfig());
        props.putAll(kafkaConfig.getAdminConfig().getConfig());

        this.adminClient = KafkaAdminClient.create(this.vertx, props);
    }

    @Override
    public void close() {
        if (this.adminClient != null) {
            this.adminClient.close();
        }
        this.handleClose();
    }

    /**
     * Returns all the topics.
     */
    public void listTopics(Handler<AsyncResult<Set<String>>> handler) {
        log.info("List topics");
        this.adminClient.listTopics(handler);
    }

    protected void createTopic(Map<String, String> configs, String name, int partition, short re,
                               Handler<AsyncResult<Void>> handler) {
        log.info("Create topic, topic name: {}", name);
        NewTopic newTopic = new NewTopic(name, partition, re);
        if (configs != null && !configs.isEmpty()) {
            newTopic.setConfig(configs);
        }
        this.adminClient.createTopics(Collections.singletonList(newTopic), handler);
    }

    protected void deleteTopics(List<String> topics, Handler<AsyncResult<Void>> handler) {
        log.info("Delete topics");
        this.adminClient.deleteTopics(topics, handler);
    }
    protected void updateTopicConfig(String name, Map<String, String> configsToSet,
                                     Handler<AsyncResult<Void>> handler) {
        log.info("Update topic, topic name: {}", name);
        ConfigResource resource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, name);
        this.adminClient.describeConfigs(List.of(resource), result-> {
            if(result.succeeded()){
                Config config  = result.result().get(resource);
                List<ConfigEntry> configEntries = new ArrayList<>();
                Map<String, ConfigEntry> mapConfig = config.getEntries().stream().collect(Collectors.toMap(ConfigEntry::getName, Function.identity()));
                for (Map.Entry<String, String> entry : configsToSet.entrySet()) {
                    String configName = entry.getKey();
                    String targetConfigValue = entry.getValue();
                    if (mapConfig.get(configName) == null || !mapConfig.get(configName).getValue().equals(targetConfigValue)) {
                        configEntries.add(new ConfigEntry(configName, targetConfigValue));
                    }
                }
                config.setEntries(configEntries);
                Map<ConfigResource, Config> updateConfig = new HashMap<>();
                updateConfig.put(resource, config);
                adminClient.alterConfigs(updateConfig, handler);

            } else {
                ContextInternal ctx = (ContextInternal)this.vertx.getOrCreateContext();
                Promise<Void> promise = ctx.promise();
                promise.fail(result.cause());
                promise.future().onComplete(handler);
            }

        });

    }
    /**
     * Returns the description of the specified topics.
     */
    protected void describeTopics(List<String> topicNames, Handler<AsyncResult<Map<String, TopicDescription>>> handler) {
        log.info("Describe topics {}", topicNames);
        this.adminClient.describeTopics(topicNames, handler);
    }

    /**
     * Returns the configuration of the specified resources.
     */
    protected void describeConfigs(List<ConfigResource> configResources, Handler<AsyncResult<Map<ConfigResource, Config>>> handler) {
        log.info("Describe configs {}", configResources);
        this.adminClient.describeConfigs(configResources, handler);
    }

    /**
     * Returns the offset spec for the given partition.
     */
    protected void listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, Handler<AsyncResult<Map<TopicPartition, ListOffsetsResultInfo>>> handler) {
        log.info("Get the offset spec for partition {}", topicPartitionOffsets);
        this.adminClient.listOffsets(topicPartitionOffsets, handler);
    }

    protected void getMessage(Map<TopicPartition, OffsetSpec> topicPartitionOffsets, Handler<AsyncResult<Map<TopicPartition, ListOffsetsResultInfo>>> handler) {
        log.info("Get the offset spec for partition {}", topicPartitionOffsets);
        this.adminClient.listOffsets(topicPartitionOffsets, handler);
    }


    /**
     * Raise close event
     */
    protected void handleClose() {

        if (this.closeHandler != null) {
            this.closeHandler.handle(this);
        }
    }
}
