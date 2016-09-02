package com.dante.zg.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @ClassName: KafkaProducerUtil
 * @Description: kafka-client-consumer version:2.10_0.8.2.1
 * @Author dante.zg
 * @Date 2015-11-18 15:37:58
 */
public class ConsumerUtil {

    private String topic = null;

    private Integer partitions = 1;

    private Integer blockingQueueSize = 1000000;

    private ConsumerConnector consumerConnector = null;

    private volatile BlockingQueue<String> blockingQueue;

    private static ConsumerUtil consumerUtil = null;

    /**
     * singleton
     * @param zkCluster - hostname:port
     * @param groupId - group id of consumer
     * @param topic - topic
     * @param partitions - kafka partitions
     * @param size - the queue size which put receved message
     */
    private ConsumerUtil(final String zkCluster, final String groupId, final String topic, final Integer partitions, final Integer size) {
        if (StringUtils.isEmpty(zkCluster) || StringUtils.isEmpty(zkCluster) || StringUtils.isEmpty(topic)) {
            throw new IllegalArgumentException("Input parameters error: nullOrEmpty!");
        }
        if (partitions < 1 || size < 1) {
            throw new IllegalArgumentException("Input parameters error: numberNotCorrectly!");
        }

        this.topic = topic;
        this.partitions = partitions;
        this.blockingQueueSize = size < this.blockingQueueSize ? this.blockingQueueSize : size;

        Properties properties = new Properties();
        properties.put("zookeeper.connect", zkCluster);
        properties.put("zookeeper.connectiontimeout.ms", "1000000");
        properties.put("group.id", groupId);

        ConsumerConfig consumerConfig = new ConsumerConfig(properties);
        this.consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        this.blockingQueue = new ArrayBlockingQueue<>(blockingQueueSize);
    }

    /**
     * start the consumer thread
     */
    public void startConsumer() {
        Map<String, Integer> topicCountmap = new HashMap<>();
        topicCountmap.put(topic, partitions);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreamsMap = consumerConnector.createMessageStreams(topicCountmap);
        List<KafkaStream<byte[], byte[]>> kafkaSteamList = messageStreamsMap.get(topic);
        ExecutorService executor = Executors.newFixedThreadPool(partitions);
        for (final KafkaStream<byte[], byte[]> kafkaStream : kafkaSteamList) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : kafkaStream) {
                        String message = new String(messageAndMetadata.message());
                        try {
                            blockingQueue.put(message);// blocking put
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }

    /**
     * get message from local blockingQueue
     *
     * @return String message
     */
    public String getMessageFromBlockingQueue() {
        String message = null;
        try {
            if (blockingQueue.isEmpty()) {
                TimeUnit.SECONDS.sleep(5000);;// default sleep 5 seconds
                getMessageFromBlockingQueue();
            } else {
                // message = blockingQueue.take();// blocking and wait
                message = blockingQueue.poll(1, TimeUnit.SECONDS);// default wait 1 second timeout
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return message;
    }

    /**
     * get a consumer with default partitions and queue size
     * @param zkCluser
     * @param groupId
     * @param topic
     * @return ConsumerUtil
     */
    public synchronized static ConsumerUtil getInstance(final String zkCluser, final String groupId, final String topic) {
        if (null == consumerUtil) {
            consumerUtil = new ConsumerUtil(zkCluser, groupId, topic, 1, 0);
        }
        return consumerUtil;
    }

    /**
     * get a consumer with customed partitions and queue size
     * @param zkCluser
     * @param groupId
     * @param topic
     * @param partitions
     * @param size
     * @return ConsumerUtil
     */
    public synchronized static ConsumerUtil getInstance(final String zkCluser, final String groupId, final String topic, final Integer partitions, final Integer size) {
        if (null == consumerUtil) {
            consumerUtil = new ConsumerUtil(zkCluser, groupId, topic, partitions, size);
        }
        return consumerUtil;
    }
}
