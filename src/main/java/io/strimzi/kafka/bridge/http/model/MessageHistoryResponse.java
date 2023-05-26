/*
 *  Copyright (C) 2021 Sunteco, Inc.
 *
 *  Sunteco License Notice
 *
 *  The contents of this file are subject to the Sunteco License
 *  Version 1.0 (the "License"). You may not use this file except in
 *  compliance with the License. The Initial Developer of the Original
 *  Code is Sunteco, JSC. Portions Copyright 2021 Sunteco JSC
 *
 *  All Rights Reserved.
 */

package io.strimzi.kafka.bridge.http.model;

/**
 * @author dang.nguyen1@sunteco.io
 */


public class MessageHistoryResponse<K,V> {
    private String topic;
    private Integer partition;
    private Integer offset;
    private Long timestamp;
    private Integer serializedKeySize;
    private Integer serializedValueSize;
    private K key;
    private V value;

    public MessageHistoryResponse() {
    }

    public MessageHistoryResponse(String topic, Integer partition, Integer offset, Long timestamp,
                                  Integer serializedKeySize, Integer serializedValueSize, K key, V value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.key = key;
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getSerializedKeySize() {
        return serializedKeySize;
    }

    public void setSerializedKeySize(Integer serializedKeySize) {
        this.serializedKeySize = serializedKeySize;
    }

    public Integer getSerializedValueSize() {
        return serializedValueSize;
    }

    public void setSerializedValueSize(Integer serializedValueSize) {
        this.serializedValueSize = serializedValueSize;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "MessageHistoryResponse{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                ", serializedKeySize=" + serializedKeySize +
                ", serializedValueSize=" + serializedValueSize +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
