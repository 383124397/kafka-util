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
	private ProducerUtil(String zkCluster, String kafkaCluster, boolean isAsync) throws Exception {
		if (StringUtils.isEmpty(zkCluster) || StringUtils.isEmpty(kafkaCluster)) {
			throw new Exception("Null Input, please check the params!");
		}
		Properties properties = new Properties();
		properties.put("zk.connect", zkCluster);
		properties.put("metadata.broker.list", kafkaCluster);
		properties.put("zk.connectiontimeout", "6000");
		if (isAsync) {
			properties.put("producer.type", "async");
		}
//		properties.put("acks", "0");ack
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		producerConfig = new ProducerConfig(properties);
		producer = new Producer<>(producerConfig);
	}

	public static ProducerUtil getProducerUtil(final String zkCluster, final String kafkaCluster, final boolean isAsync) {
	    if (null == producerUtil) {
	        try {
                producerUtil= new ProducerUtil(zkCluster, kafkaCluster, isAsync);
            } catch (Exception e) {
                e.printStackTrace();
            }
	    }
	    return producerUtil;
	}

	/**
	 * 发送消息方法
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
}
