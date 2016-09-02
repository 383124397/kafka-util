package com.dante.zg.kafka;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import kafka.producer.KeyedMessage;
import kafka.producer.Producer;
import kafka.producer.ProducerConfig;
import scala.collection.JavaConversions;

/**
 * @ClassName: KafkaProducerUtil
 * @Description: kafka-client-producer version:2.10_0.8.2.1
 * @Author dante.zg
 * @Date 2015-11-18 15:37:58
 */
public class ProducerUtil {

	private ProducerConfig producerConfig;

	private Producer<String, String> producer;

	private List<KeyedMessage<String, String>> messageList;

	private static ProducerUtil producerUtil = null;

	/**
	 * singleton
	 * @param zkCluster - ZK cluster hostname:port
	 * @param kafkaCluster - Kafka cluster hostname:port
	 * @param isAsync - sync or async
	 * @throws Exception
	 */
	private ProducerUtil(String zkCluster, String kafkaCluster, boolean isAsync, boolean ack) throws Exception {
		if (StringUtils.isEmpty(zkCluster) || StringUtils.isEmpty(kafkaCluster)) {
			throw new Exception("Null Input, please check the params!");
		}
		Properties properties = new Properties();
		properties.put("zk.connect", zkCluster);
		properties.put("metadata.broker.list", kafkaCluster);
		properties.put("zk.connectiontimeout", "6000");
		if (isAsync)
			properties.put("producer.type", "async");
		if (ack)
		    properties.put("acks", "0");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		producerConfig = new ProducerConfig(properties);
		producer = new Producer<>(producerConfig);
	}

	/**
     * send message method
     * @param topic: topic
     * @param message: message
     * @throws Exception
     */
    public void sendMessage(final String topic, String message) throws Exception {
        if (StringUtils.isEmpty(topic) || StringUtils.isEmpty(message)) {
            throw new Exception("Null Input, please check the params!");
        }
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<>(topic, UUID.randomUUID().toString(), message);
        messageList = new ArrayList<>();
        messageList.add(keyedMessage);
        producer.send(JavaConversions.asScalaBuffer(messageList));
    }

    /**
     * get a producer with this three parameters
     * @param zkCluster: zk cluster
     * @param kafkaCluster: kafka cluster
     * @param isAsync: is aysnc send model
     * @return ProducerUtil
     */
	public synchronized static ProducerUtil getInstance(final String zkCluster, final String kafkaCluster, final boolean isAsync) {
	    if (null == producerUtil) {
	        try {
                producerUtil= new ProducerUtil(zkCluster, kafkaCluster, isAsync, false);
            } catch (Exception e) {
                e.printStackTrace();
            }
	    }
	    return producerUtil;
	}

	/**
	 * get a producer with this four parameters
	 * @param zkCluster: zk cluster
	 * @param kafkaCluster: kafka cluster
	 * @param isAsync: is aysnc send model
	 * @param ack: reference ack mechanism
	 * @return ProducerUtil
	 */
	public synchronized static ProducerUtil getInstance(final String zkCluster, final String kafkaCluster, final boolean isAsync, final boolean ack) {
	    if (null == producerUtil) {
            try {
                producerUtil= new ProducerUtil(zkCluster, kafkaCluster, isAsync, ack);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return producerUtil;
	}
}