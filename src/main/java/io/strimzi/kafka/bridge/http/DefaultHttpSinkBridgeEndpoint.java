package io.strimzi.kafka.bridge.http;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.strimzi.kafka.bridge.BridgeContentType;
import io.strimzi.kafka.bridge.ConsumerInstanceId;
import io.strimzi.kafka.bridge.EmbeddedFormat;
import io.strimzi.kafka.bridge.SinkBridgeEndpoint;
import io.strimzi.kafka.bridge.config.BridgeConfig;
import io.strimzi.kafka.bridge.http.model.HttpBridgeError;
import io.strimzi.kafka.bridge.http.model.MessageHistoryResponse;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class DefaultHttpSinkBridgeEndpoint<K, V> extends HttpSinkBridgeEndpoint<K, V> {
    private static final Logger log = LoggerFactory.getLogger(DefaultHttpSinkBridgeEndpoint.class);

    public DefaultHttpSinkBridgeEndpoint(String defaultGroup, String defaultName, Vertx vertx, BridgeConfig bridgeConfig, HttpBridgeContext<K, V> context,
                                         EmbeddedFormat format, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        super(vertx, bridgeConfig, context, format, keyDeserializer, valueDeserializer);
        this.groupId = defaultGroup;
        this.name = defaultName;
        this.consumerInstanceId = new ConsumerInstanceId(this.groupId, this.name);
        this.initDefaultConsumer();
    }

    public SinkBridgeEndpoint<K, V> initDefaultConsumer() {
        if (this.httpBridgeContext.getHttpSinkEndpoints().containsKey(this.consumerInstanceId)) {
            log.info("default consumer created");
            return this.httpBridgeContext.getHttpSinkEndpoints().get(this.consumerInstanceId);
        } else {
            return this.createDefaultConsumer();
        }
    }

    private SinkBridgeEndpoint<K, V> createDefaultConsumer() {
        JsonObject bodyAsJson = new JsonObject();
        // construct base URI for consumer
//        String requestUri = this.buildRequestUri(routingContext);
//        if (!routingContext.request().path().endsWith("/")) {
//            requestUri += "/";
//        }
//        String consumerBaseUri = requestUri + "instances/" + this.name;

        // get supported consumer configuration parameters
        Properties config = new Properties();
        addConfigParameter(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                bodyAsJson.getString(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, null), config);
        // OpenAPI validation handles boolean and integer, quoted or not as string, in the same way
        // instead of raising a validation error due to this: https://github.com/vert-x3/vertx-web/issues/1375
        Object enableAutoCommit = bodyAsJson.getValue(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        addConfigParameter(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                enableAutoCommit != null ? String.valueOf(enableAutoCommit) : null, config);
        Object fetchMinBytes = bodyAsJson.getValue(ConsumerConfig.FETCH_MIN_BYTES_CONFIG);
        addConfigParameter(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
                fetchMinBytes != null ? String.valueOf(fetchMinBytes) : null, config);
        Object requestTimeoutMs = bodyAsJson.getValue("consumer." + ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        addConfigParameter(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                requestTimeoutMs != null ? String.valueOf(requestTimeoutMs) : null, config);
        addConfigParameter(ConsumerConfig.CLIENT_ID_CONFIG, this.name, config);
        Object isolationLevel = bodyAsJson.getValue(ConsumerConfig.ISOLATION_LEVEL_CONFIG);
        addConfigParameter(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                isolationLevel != null ? String.valueOf(isolationLevel) : null, config);

        // create the consumer
        this.initConsumer(config);


        log.info("Created default consumer {} in group {}", this.name, this.groupId);
        // send consumer instance id(name) and base URI as response
//        JsonObject body = new JsonObject()
//                .put("instance_id", this.name)
//                .put("base_uri", consumerBaseUri);
//        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(),
//                BridgeContentType.KAFKA_JSON, body.toBuffer());
        return this;
    }

    public ConsumerInstanceId getInstanceId() {
        return this.consumerInstanceId;
    }

    private void doGetMessageHistory(RoutingContext routingContext, JsonObject bodyAsJson) {
//        String topic = bodyAsJson.getString("topic");
//        int limit = bodyAsJson.getInteger("limitMessage", 10);
        String topic = routingContext.queryParam("topic").get(0);
        List<String> limitList = routingContext.queryParam("limitMessage");
        int limit;
        if(limitList != null && limitList.size() != 0){
            limit = Integer.parseInt(limitList.get(0));
        } else {
            limit = 10;
        }
        HttpAdminClientEndpoint admin = (HttpAdminClientEndpoint)this.httpBridgeContext.getAdminClientEndpoint();
        admin.listTopics(listTopicResult ->{
            if(listTopicResult.succeeded()){
                if(listTopicResult.result().stream().noneMatch(existed->existed.equals(topic))){
                    HttpBridgeError error = new HttpBridgeError(
                            HttpResponseStatus.NOT_FOUND.code(),
                            "Topic does not exist");
                    HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                            BridgeContentType.JSON, error.toJson().toBuffer());
                }

                log.info("Start doGetMessageHistory gt message topic: {}", topic);
                getMessageHistory(topic, limit).onComplete(response -> {
                    if (response.succeeded()) {
                        List<MessageHistoryResponse<String, String>> stringRes =
                                response.result().stream().map(ms -> {
                                    MessageHistoryResponse<String, String> newM = new MessageHistoryResponse<>(ms.getTopic(),
                                            ms.getPartition(), ms.getOffset(), ms.getTimestamp(),
                                            ms.getSerializedKeySize(), ms.getSerializedValueSize(),
                                            new String((byte[]) ms.getKey()), new String((byte[]) ms.getValue()));
                                    return newM;
                                }).collect(Collectors.toList());
                        JsonObject root = new JsonObject();
                        root.put("message", stringRes);
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.OK.code(), BridgeContentType.JSON, root.toBuffer());
                    }else {
                        log.error("Get message failed :{}", response.cause().getMessage());
                        HttpBridgeError error = new HttpBridgeError(
                                HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                response.cause().getMessage());
                        HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                                BridgeContentType.JSON, error.toJson().toBuffer());
                    }
                });
            }else{
                log.error("Get message, check topic failed :{}", listTopicResult.cause().getMessage());
                HttpBridgeError error = new HttpBridgeError(
                        HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        listTopicResult.cause().getMessage());
                HttpUtils.sendResponse(routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        BridgeContentType.JSON, error.toJson().toBuffer());
            }
        });

    }

    @Override
    protected void othersHandler(RoutingContext routingContext, JsonObject bodyAsJson, Handler<?> handler) {
        switch (this.httpBridgeContext.getOpenApiOperation()) {
            case MESSAGE_HISTORY:
                doGetMessageHistory(routingContext, bodyAsJson);
                break;
            default:
                super.othersHandler(routingContext, bodyAsJson, handler);
        }
    }
}
