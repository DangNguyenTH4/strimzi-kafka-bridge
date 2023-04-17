package io.strimzi.kafka.bridge.http.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Map;
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaConfigBody {
    private String namespace;
    private String clusterName;
    private String topic;
    private int partitions=1;
    private int replicationFactor=1;
    private Map<String, String> configs;

    public String getNamespace() {
        return this.namespace;
    }

    public String getClusterName() {
        return this.clusterName;
    }

    public String getTopic() {
        return this.topic;
    }

    public int getPartitions() {
        return this.partitions;
    }

    public int getReplicationFactor() {
        return this.replicationFactor;
    }

    public Map<String, String> getConfigs() {
        return this.configs;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public void setConfigs(Map<String, String> configs) {
        this.configs = configs;
    }

    public KafkaConfigBody(String namespace, String clusterName, String topic, int partitions, int replicationFactor, Map<String, String> configs) {
        this.namespace = namespace;
        this.clusterName = clusterName;
        this.topic = topic;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.configs = configs;
    }

    public KafkaConfigBody() {
    }
}
